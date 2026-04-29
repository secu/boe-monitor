"""
BOE Monitor — bot.py
Comprueba el portal subastas.boe.es y avisa por Telegram si ha
cambiado el número de subastas de Inmuebles en estado
"Próxima apertura" (PU) o "Celebrándose" (EJ).
Diseñado para ejecutarse una vez por invocación (GitHub Actions cron).
"""

import json
import os
import random
import re
import time

import requests
from bs4 import BeautifulSoup

# En Render se usan variables de entorno; en local se usa config.py
import os as _os
try:
    import config as _cfg
    _FALLBACK_TOKEN   = getattr(_cfg, "TELEGRAM_TOKEN", "")
    _FALLBACK_CHAT_ID = getattr(_cfg, "TELEGRAM_CHAT_ID", "")
except ImportError:
    _FALLBACK_TOKEN   = ""
    _FALLBACK_CHAT_ID = ""

TELEGRAM_TOKEN   = _os.environ.get("TELEGRAM_TOKEN",   _FALLBACK_TOKEN)
TELEGRAM_CHAT_ID = _os.environ.get("TELEGRAM_CHAT_ID", _FALLBACK_CHAT_ID)

# ── Constantes ─────────────────────────────────────────────────────────────
URL_BOE = "https://subastas.boe.es/subastas_ava.php"
ESTADO_FILE = "estado.json"

ESTADOS = {
    "PU": "Próxima apertura",
    "EJ": "Celebrándose",
}

# URLs directas al portal BOE con el filtro de estado ya aplicado
URL_BASE_FILTRO = (
    "https://subastas.boe.es/subastas_ava.php"
    "?accion=Buscar"
    "&campo[2]=SUBASTA.ESTADO.CODIGO&dato[2]={codigo}"
    "&campo[3]=BIEN.TIPO&dato[3]=I"
)
URLS_ESTADO = {
    codigo: URL_BASE_FILTRO.format(codigo=codigo)
    for codigo in ["PU", "EJ"]
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://subastas.boe.es/subastas_ava.php",
}


# ── Scraping ────────────────────────────────────────────────────────────────
def get_numero_subastas(estado_codigo: str) -> int | None:
    """
    Consulta el portal BOE con los filtros deseados y devuelve
    únicamente el número total de subastas encontradas.
    Retorna None si hay error de red o de parseo.
    """
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
        resp = requests.post(
            URL_BOE,
            data=form_data,
            headers=HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[ERROR] Request fallida para estado {estado_codigo}: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    texto = soup.get_text(separator=" ")

    # El portal muestra: "Resultados 1 a 50 de 162" o "1.058"
    match = re.search(r"Resultados\s+[\d\.]+\s+a\s+[\d\.]+\s+de\s+([\d\.]+)", texto, re.IGNORECASE)
    if match:
        return int(match.group(1).replace(".", ""))

    # Fallback: cualquier patrón "de X" cerca de "Resultado"
    match2 = re.search(r"de\s+([\d\.]+)", texto, re.IGNORECASE)
    if match2:
        return int(match2.group(1).replace(".", ""))

    print(f"[AVISO] No se pudo extraer el número de subastas para estado {estado_codigo}")
    return None


# ── Persistencia de estado ──────────────────────────────────────────────────
def cargar_estado() -> dict:
    if os.path.exists(ESTADO_FILE):
        try:
            with open(ESTADO_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def guardar_estado(estado: dict) -> None:
    with open(ESTADO_FILE, "w", encoding="utf-8") as f:
        json.dump(estado, f, ensure_ascii=False, indent=2)


# ── Notificaciones Telegram ─────────────────────────────────────────────────
def enviar_telegram(mensaje: str) -> None:
    endpoint = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": mensaje,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    try:
        r = requests.post(endpoint, data=payload, timeout=15)
        r.raise_for_status()
        print("[OK] Notificación Telegram enviada.")
    except requests.RequestException as e:
        print(f"[ERROR] No se pudo enviar el Telegram: {e}")


# ── Lógica principal de chequeo ─────────────────────────────────────────────
def chequear() -> None:
    print(f"\n[BOE Monitor] Chequeo iniciado...")
    estado_anterior = cargar_estado()
    estado_nuevo = {}
    cambios = []

    for codigo, nombre in ESTADOS.items():
        num = get_numero_subastas(codigo)

        if num is None:
            # Error de red — no actualizamos estado para no perder referencia
            estado_nuevo[codigo] = estado_anterior.get(codigo)
            continue

        estado_nuevo[codigo] = num
        anterior = estado_anterior.get(codigo)

        print(f"  {nombre}: {anterior} → {num}")

        if anterior is not None and anterior != num:
            diferencia = num - anterior
            signo = "+" if diferencia > 0 else ""
            cambios.append(
                f"🏠 <b>{nombre}</b>: {anterior} → {num} ({signo}{diferencia})"
            )

        # Pausa aleatoria entre peticiones para parecer un humano
        time.sleep(random.uniform(2, 5))

    guardar_estado(estado_nuevo)

    es_primera_vez = not estado_anterior  # True si no había estado previo

    nuevas = []     # Subastas que han subido (nuevas publicaciones)
    cierres = []    # Subastas que han bajado (cierres normales)

    for codigo, nombre in ESTADOS.items():
        num = estado_nuevo.get(codigo)
        anterior = estado_anterior.get(codigo)
        if num is None or anterior is None:
            continue
        url = URLS_ESTADO[codigo]
        if num > anterior:
            diff = num - anterior
            nuevas.append(f"🆕 <a href='{url}'><b>{nombre}</b>: +{diff} nuevas ({anterior} → {num})</a>")
        elif num < anterior:
            diff = anterior - num
            cierres.append(f"📋 <a href='{url}'><b>{nombre}</b>: -{diff} finalizadas ({anterior} → {num})</a>")

    if es_primera_vez:
        resumen = "\n".join(
            f"🏠 <a href='{URLS_ESTADO[codigo]}'><b>{nombre}</b>: {estado_nuevo.get(codigo, '?')} subastas</a>"
            for codigo, nombre in ESTADOS.items()
        )
        msg = (
            "✅ <b>BOE Monitor activado</b>\n\n"
            "Estado actual de subastas de Inmuebles:\n"
            + resumen
            + "\n\nℹ️ <i>Este bot monitoriza únicamente subastas de <b>Inmuebles</b>. No incluye otro tipo de bienes.</i>\n\n"
            "🔔 Te avisaré solo cuando se publiquen nuevas subastas.\n"
            "🔗 <a href='https://subastas.boe.es'>Ir al Portal BOE</a>  |  "
            "🏠 <a href='https://app.investormap.es'>Ir a Suite InvestorMAP</a>"
        )
    elif nuevas:
        cuerpo = "\n".join(nuevas)
        if cierres:
            cuerpo += "\n\n" + "\n".join(cierres)
        msg = (
            "⚠️ <b>BOE — Nuevas subastas publicadas</b>\n\n"
            + cuerpo
            + "\n\nℹ️ <i>Solo se monitorizan subastas de <b>Inmuebles</b>.</i>\n\n"
            "🔗 <a href='https://subastas.boe.es'>Ir al Portal BOE</a>  |  "
            "🏠 <a href='https://app.investormap.es'>Ir a Suite InvestorMAP</a>"
        )
    else:
        # Sin nuevas subastas — mensaje informativo tranquilizador
        resumen = "\n".join(
            f"🏠 <a href='{URLS_ESTADO[codigo]}'><b>{nombre}</b>: {estado_nuevo.get(codigo, '?')} subastas</a>"
            for codigo, nombre in ESTADOS.items()
        )
        extra = ("\n📉 " + " / ".join(cierres)) if cierres else ""
        msg = (
            "✅ <b>BOE — Sin novedades</b>\n\n"
            + resumen
            + extra
            + "\n\nℹ️ <i>Solo se monitorizan subastas de <b>Inmuebles</b>.</i>\n\n"
            "🔕 No se han publicado subastas nuevas.\n"
            "🔗 <a href='https://subastas.boe.es'>Ir al Portal BOE</a>  |  "
            "🏠 <a href='https://app.investormap.es'>Ir a Suite InvestorMAP</a>"
        )

    enviar_telegram(msg)



# ── Arranque ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("  BOE Monitor — Subastas Inmuebles")
    print("=" * 50)
    chequear()
