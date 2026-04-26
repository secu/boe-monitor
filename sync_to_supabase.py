"""
sync_to_supabase.py — Pipeline automático de subastas para InvestorMAP

Ejecuta el scraping BOE → clasifica tipo de bien → sube a Supabase → geocodifica.
Diseñado para GitHub Actions (se ejecuta periódicamente).

Requiere variables de entorno:
  SUPABASE_URL, SUPABASE_SERVICE_KEY, GEOAPIFY_KEY
"""

import json
import os
import re
import sys
import time
from collections import OrderedDict

import requests
from bs4 import BeautifulSoup

# ── Config ──────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://grjbvzrscxljxzjoqyky.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
GEOAPIFY_KEY = os.environ.get("GEOAPIFY_KEY", "2d45d6de85a14f309a64704120ff4835")

URL_BOE = "https://subastas.boe.es/subastas_ava.php"
DETAIL_URL = "https://subastas.boe.es/detalleSubasta.php"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/139.0.0.0 Safari/537.36"
    ),
    "Referer": "https://subastas.boe.es/subastas_ava.php",
}

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
}

MAX_AUCTIONS = 500  # Límite para no sobrecargar en una sola ejecución
GEOCODE_BATCH = 200  # Máximo de geocodificaciones por ejecución


# ── Clasificador de tipo de bien ────────────────────────────────────────────
def classify_tipo_bien(raw_tipo: str) -> str:
    """Clasifica el tipo de bien del BOE en categorías para los iconos del mapa."""
    t = (raw_tipo or "").lower().strip()
    if any(w in t for w in ["vivienda", "piso", "apartamento", "ático", "dúplex", "chalet", "adosado", "unifamiliar", "casa"]):
        return "vivienda"
    if any(w in t for w in ["garaje", "plaza de garaje", "parking", "aparcamiento", "cochera"]):
        return "garaje"
    if any(w in t for w in ["trastero", "almacén", "anejo"]):
        return "trastero"
    if any(w in t for w in ["finca", "rústica", "rústico", "agrícola", "agraria"]):
        return "finca"
    if any(w in t for w in ["nave", "industrial", "local", "comercial", "oficina"]):
        return "nave"
    if any(w in t for w in ["solar", "parcela", "terreno", "suelo"]):
        return "solar"
    return "otro"


# ── Parseo de valores euro ──────────────────────────────────────────────────
def parse_euro(euro_str: str):
    """Convierte '65.210,60 €' → 65210.60"""
    if not euro_str:
        return None
    m = re.search(r"\d[\d.,]*\d", euro_str)
    if not m:
        return None
    clean = euro_str.replace("€", "").strip().replace(".", "").replace(",", ".")
    try:
        val = float(clean)
        return int(val) if val == int(val) else val
    except ValueError:
        return None


# ── Scraping BOE ────────────────────────────────────────────────────────────
def get_value(soup, keyword: str) -> str:
    """Extrae valor de una celda th/td del BOE."""
    th = soup.find("th", string=re.compile(re.escape(keyword), re.IGNORECASE))
    if th:
        td = th.find_next_sibling("td")
        if td:
            return " ".join(td.get_text(separator=" ").split()).strip()
    return ""


def fetch_auction_list(estado_codigo: str) -> list[str]:
    """Obtiene la lista de URLs de subastas activas para un estado dado."""
    all_urls = []
    page = 0
    
    while True:
        form_data = {
            "accion": "Buscar",
            "page_hits": "500",
            "campo[0]": "SUBASTA.ORIGEN",
            "dato[0]": "",
            "campo[1]": "SUBASTA.AUTORIDAD",
            "dato[1]": "",
            "campo[2]": "SUBASTA.ESTADO.CODIGO",
            "dato[2]": estado_codigo,
            "campo[3]": "BIEN.TIPO",
            "dato[3]": "I",  # Inmuebles
            "campo[4]": "BIEN.SUBTIPO",
            "dato[4]": "",
            "sort_field[0]": "SUBASTA.FECHA_FIN",
            "sort_order[0]": "asc",
        }
        
        try:
            resp = requests.post(URL_BOE, data=form_data, headers=HEADERS, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"[ERROR] Request fallida: {e}")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Buscar enlaces a detalles de subasta
        links = soup.select("a[href*='detalleSubasta.php']")
        urls = list(set(
            "https://subastas.boe.es/" + a["href"] for a in links if a.get("href")
        ))
        
        if not urls:
            break
        
        all_urls.extend(urls)
        print(f"  Página {page + 1}: {len(urls)} subastas encontradas")
        
        # Buscar paginación
        next_link = soup.find("a", title="Página siguiente")
        if not next_link or len(all_urls) >= MAX_AUCTIONS:
            break
            
        page += 1
        time.sleep(1)
    
    return list(set(all_urls))[:MAX_AUCTIONS]


def fetch_auction_detail(url: str) -> dict | None:
    """Scrape completo de una subasta individual (info general + bienes)."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [ERROR] {url}: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Info general
    item = {
        "codigo_subasta": get_value(soup, "Identificador"),
        "tipo_subasta": get_value(soup, "Tipo de subasta"),
        "cuenta_expediente": get_value(soup, "Cuenta expediente"),
        "estado": get_value(soup, "Estado"),
        "anuncio_boe": get_value(soup, "Anuncio BOE"),
        "forma_adjudicacion": get_value(soup, "Forma adjudicación"),
        "valor_subasta": parse_euro(get_value(soup, "Valor subasta")),
        "valor_tasacion": parse_euro(get_value(soup, "Tasación")),
        "cantidad_reclamada": parse_euro(get_value(soup, "Cantidad reclamada")),
        "puja_minima": parse_euro(get_value(soup, "Puja mínima")),
        "tramos_entre_pujas": parse_euro(get_value(soup, "Tramos entre pujas")),
        "importe_deposito": parse_euro(get_value(soup, "Importe del depósito")),
        "fecha_inicio": get_value(soup, "Fecha de inicio").lower().split("cet")[0].strip(),
        "fecha_fin": get_value(soup, "Fecha de conclusión").lower().split("cet")[0].strip(),
        "link_boe": url,
    }

    if not item["codigo_subasta"]:
        return None

    # Navegar a pestaña de Bienes
    bienes_link = soup.find("a", string=re.compile("Bienes", re.IGNORECASE))
    if bienes_link and bienes_link.get("href"):
        bienes_url = "https://subastas.boe.es/" + bienes_link["href"]
        try:
            resp2 = requests.get(bienes_url, headers=HEADERS, timeout=20)
            resp2.raise_for_status()
            soup2 = BeautifulSoup(resp2.text, "html.parser")
            
            # Tipo de bien del header h4
            h4 = soup2.select_one("[id*='idBloqueLote'] h4")
            tipo_raw = h4.get_text(strip=True) if h4 else ""
            
            item["tipo_bien_raw"] = tipo_raw
            item["tipo_bien"] = classify_tipo_bien(tipo_raw)
            item["descripcion"] = get_value(soup2, "Descripción")
            item["referencia_catastral"] = get_value(soup2, "Referencia catastral")
            item["direccion"] = get_value(soup2, "Dirección")
            item["codigo_postal"] = get_value(soup2, "Código Postal")
            item["localidad"] = get_value(soup2, "Localidad")
            item["provincia"] = get_value(soup2, "Provincia")
            item["situacion_posesoria"] = get_value(soup2, "Situación posesoria")
            item["visitable"] = get_value(soup2, "Visitable")
            item["cargas"] = get_value(soup2, "Cargas")
            item["inscripcion_registral"] = get_value(soup2, "Inscripción registral")
            item["informacion_adicional"] = get_value(soup2, "Información adicional")
            item["vivienda_habitual"] = get_value(soup2, "Vivienda habitual")
            item["idufir"] = get_value(soup2, "IDUFIR")
            item["superficie"] = get_value(soup2, "Superficie")
            item["cuota"] = get_value(soup2, "Cuota")
            
            time.sleep(0.5)
        except requests.RequestException:
            pass

    return item


# ── Geocodificación con Geoapify + fallback Nominatim ─────────────────────
def build_geocode_queries(auction: dict) -> tuple[str | None, str | None]:
    """Construye query exacta y query aproximada para geocodificación."""
    parts_exact = []
    if auction.get("direccion"):
        parts_exact.append(auction["direccion"])
    if auction.get("codigo_postal"):
        parts_exact.append(auction["codigo_postal"])
    if auction.get("localidad"):
        parts_exact.append(auction["localidad"])
    if auction.get("provincia"):
        parts_exact.append(auction["provincia"])
    parts_exact.append("España")

    parts_aprox = []
    if auction.get("localidad"):
        parts_aprox.append(auction["localidad"])
    elif auction.get("provincia"):
        parts_aprox.append(auction["provincia"])
    if auction.get("provincia") and auction.get("localidad"):
        parts_aprox.append(auction["provincia"])
    parts_aprox.append("España")

    q_exact = ", ".join(p for p in parts_exact if p) if len(parts_exact) > 1 else None
    q_aprox = ", ".join(p for p in parts_aprox if p) if len(parts_aprox) > 1 else None
    return q_exact, q_aprox


def geocode_geoapify(query: str) -> tuple[float | None, float | None]:
    """Geocodifica con Geoapify. Retorna (lat, lng) o (None, None)."""
    if not query or not GEOAPIFY_KEY:
        return None, None
    try:
        resp = requests.get(
            "https://api.geoapify.com/v1/geocode/search",
            params={
                "text": query,
                "filter": "countrycode:es",
                "limit": 1,
                "apiKey": GEOAPIFY_KEY,
            },
            timeout=8,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("features"):
            coords = data["features"][0]["geometry"]["coordinates"]
            return coords[1], coords[0]  # [lng, lat] → (lat, lng)
    except Exception as e:
        print(f"  [GEOAPIFY ERROR] {query[:60]}: {e}")
    return None, None


def geocode_nominatim(query: str) -> tuple[float | None, float | None]:
    """Geocodifica con Nominatim (fallback gratuito). Retorna (lat, lng) o (None, None)."""
    if not query:
        return None, None
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"format": "json", "q": query, "limit": 1, "countrycodes": "es"},
            headers={"User-Agent": "InvestorMAP_Sync_Bot/2.0 (geocoding pipeline)"},
            timeout=8,
        )
        resp.raise_for_status()
        data = resp.json()
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        print(f"  [NOMINATIM ERROR] {query[:60]}: {e}")
    return None, None


def geocode_auction(auction: dict) -> tuple[float | None, float | None, str]:
    """Geocodifica una subasta con Geoapify primero, Nominatim como fallback.
    Retorna (lat, lng, source) donde source es 'geoapify', 'nominatim' o 'failed'."""
    q_exact, q_aprox = build_geocode_queries(auction)

    # 1. Intentar Geoapify con query exacta (incluye dirección)
    if q_exact and auction.get("direccion"):
        lat, lng = geocode_geoapify(q_exact)
        if lat and lng:
            return lat, lng, "geoapify"

    # 2. Intentar Geoapify con query aproximada (localidad + provincia)
    if q_aprox:
        lat, lng = geocode_geoapify(q_aprox)
        if lat and lng:
            return lat, lng, "geoapify_aprox"

    # 3. Fallback Nominatim con query exacta
    if q_exact and auction.get("direccion"):
        time.sleep(0.5)  # Respetar rate limit Nominatim entre llamadas
        lat, lng = geocode_nominatim(q_exact)
        if lat and lng:
            return lat, lng, "nominatim"

    # 4. Fallback Nominatim con query aproximada
    if q_aprox:
        time.sleep(0.5)
        lat, lng = geocode_nominatim(q_aprox)
        if lat and lng:
            return lat, lng, "nominatim_aprox"

    return None, None, "failed"


# ── Upsert a Supabase ──────────────────────────────────────────────────────
def upsert_auctions(auctions: list[dict]) -> int:
    """Sube las subastas a Supabase con upsert (insert or update on conflict)."""
    if not auctions:
        return 0
    
    ALL_KEYS = [
        "codigo_subasta", "tipo_subasta", "cuenta_expediente", "estado", 
        "anuncio_boe", "forma_adjudicacion", "valor_subasta", "valor_tasacion", 
        "cantidad_reclamada", "puja_minima", "tramos_entre_pujas", "importe_deposito", 
        "fecha_inicio", "fecha_fin", "link_boe", "tipo_bien_raw", "tipo_bien", 
        "descripcion", "referencia_catastral", "direccion", "codigo_postal", 
        "localidad", "provincia", "situacion_posesoria", "visitable", "cargas", 
        "inscripcion_registral", "informacion_adicional", "vivienda_habitual", 
        "idufir", "superficie", "cuota"
    ]
    
    # Asegurar que todas las subastas tienen exactamente las mismas keys
    clean = []
    for a in auctions:
        if not a.get("codigo_subasta"):
            continue
        row = {}
        for key in ALL_KEYS:
            val = a.get(key)
            row[key] = val if val != "" else None
        clean.append(row)
    
    if not clean:
        return 0
    
    # Upsert en batches de 50
    inserted = 0
    batch_size = 50
    
    for i in range(0, len(clean), batch_size):
        batch = clean[i:i + batch_size]
        try:
            resp = requests.post(
                f"{SUPABASE_URL}/rest/v1/map_auctions",
                headers={
                    **SUPABASE_HEADERS,
                    "Prefer": "resolution=merge-duplicates,return=minimal",
                },
                json=batch,
                timeout=30,
            )
            if resp.status_code in (200, 201):
                inserted += len(batch)
                print(f"  Batch {i // batch_size + 1}: {len(batch)} subastas upserted")
            else:
                print(f"  [ERROR] Supabase batch {i // batch_size + 1}: {resp.status_code} - {resp.text[:200]}")
        except requests.RequestException as e:
            print(f"  [ERROR] Supabase request: {e}")
    
    return inserted


def geocode_pending() -> int:
    """Geocodifica subastas que aún no tienen coordenadas (Geoapify + Nominatim fallback)."""
    # IMPORTANTE: El filtro lat=is.null debe ir en el query string de Supabase REST
    try:
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/map_auctions",
            headers=SUPABASE_HEADERS,
            params={
                "select": "id,direccion,codigo_postal,localidad,provincia,created_at",
                "lat": "is.null",
                "order": "created_at.desc",
                "limit": str(GEOCODE_BATCH),
            },
            timeout=20,
        )
        resp.raise_for_status()
        pending = resp.json()
    except Exception as e:
        print(f"[ERROR] Fetching pending geocodes: {e}")
        return 0

    if not pending:
        print("[OK] No hay subastas pendientes de geocodificar")
        return 0

    print(f"[GEOCODE] {len(pending)} subastas pendientes de geocodificar (Geoapify + Nominatim fallback)")
    geocoded = 0
    failed = 0
    sources = {}

    for i, auction in enumerate(pending):
        lat, lng, source = geocode_auction(auction)
        sources[source] = sources.get(source, 0) + 1

        if lat and lng:
            try:
                patch_resp = requests.patch(
                    f"{SUPABASE_URL}/rest/v1/map_auctions?id=eq.{auction['id']}",
                    headers={
                        **SUPABASE_HEADERS,
                        "Prefer": "return=minimal",
                    },
                    json={
                        "lat": lat,
                        "lng": lng,
                        "geocoded_at": "now()",
                        "geocode_source": source,
                    },
                    timeout=10,
                )
                if patch_resp.status_code in (200, 204):
                    geocoded += 1
                    if geocoded % 20 == 0:
                        print(f"  → {geocoded}/{len(pending)} geocodificadas... (fuentes: {sources})")
                else:
                    print(f"  [PATCH ERROR] {auction['id']}: {patch_resp.status_code}")
            except Exception as e:
                print(f"  [PATCH EXCEPTION] {auction['id']}: {e}")
        else:
            failed += 1
            if failed <= 5:  # Solo loguear los primeros 5 fallos
                loc = auction.get('localidad') or auction.get('provincia') or 'Sin ubicación'
                print(f"  [FAIL] Sin coordenadas: {loc}")

        # Rate limiting: Geoapify permite ~3 req/s en plan gratuito
        # Con Nominatim como fallback necesitamos ser más conservadores
        time.sleep(0.4)

    print(f"[GEOCODE] Completado: {geocoded} geocodificadas, {failed} fallidas | Fuentes: {sources}")
    return geocoded


# ── Main ────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  InvestorMAP — Sync subastas a Supabase")
    print("=" * 60)

    if not SUPABASE_KEY:
        print("[ERROR] SUPABASE_SERVICE_KEY no configurada")
        sys.exit(1)

    # 1. Recoger URLs de subastas activas (Próxima apertura + Celebrándose)
    all_urls = []
    for codigo, nombre in [("PU", "Próxima apertura"), ("EJ", "Celebrándose")]:
        print(f"\n[SCAN] {nombre} ({codigo})...")
        urls = fetch_auction_list(codigo)
        print(f"  → {len(urls)} subastas encontradas")
        all_urls.extend(urls)
        time.sleep(2)

    all_urls = list(set(all_urls))
    print(f"\n[TOTAL] {len(all_urls)} subastas únicas a procesar")

    # 2. Verificar cuáles ya están en Supabase
    try:
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/map_auctions",
            headers=SUPABASE_HEADERS,
            params={"select": "codigo_subasta"},
            timeout=15,
        )
        resp.raise_for_status()
        existing = {r["codigo_subasta"] for r in resp.json()}
    except Exception:
        existing = set()

    # Filtrar: solo procesar subastas nuevas o que necesitan actualización
    # Para la primera ejecución procesamos todas; luego solo las nuevas
    new_urls = []
    for url in all_urls:
        # Extraer idSub de la URL para comparar
        m = re.search(r"idSub=([^&]+)", url)
        if m:
            sub_id = m.group(1)
            if sub_id not in existing:
                new_urls.append(url)
        else:
            new_urls.append(url)

    if not new_urls and existing:
        print(f"[OK] Sin subastas nuevas. {len(existing)} ya en base de datos.")
        # Aun así geocodificamos pendientes
        geo = geocode_pending()
        print(f"[GEOCODE] {geo} subastas geocodificadas")
        return

    urls_to_process = new_urls if new_urls else all_urls
    print(f"[SCRAPE] Procesando {len(urls_to_process)} subastas...")

    # 3. Scrapear detalles de cada subasta
    auctions = []
    for i, url in enumerate(urls_to_process):
        print(f"  [{i+1}/{len(urls_to_process)}] {url[:70]}...")
        detail = fetch_auction_detail(url)
        if detail:
            auctions.append(detail)
        time.sleep(1)  # Ser amable con el BOE

    print(f"\n[SCRAPED] {len(auctions)} subastas con datos completos")

    # 4. Subir a Supabase
    if auctions:
        inserted = upsert_auctions(auctions)
        print(f"[UPSERT] {inserted} subastas sincronizadas en Supabase")

    # 5. Geocodificar pendientes
    geo = geocode_pending()
    print(f"[GEOCODE] {geo} subastas geocodificadas")

    print(f"\n{'=' * 60}")
    print(f"  ✅ Sync completado: {len(auctions)} procesadas, {geo} geocodificadas")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
