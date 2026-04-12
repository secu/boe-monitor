"""
debug_boe.py — Script temporal para ver la respuesta real del BOE
Ejecutar: py debug_boe.py
"""
import requests
from bs4 import BeautifulSoup

URL = "https://subastas.boe.es/subastas_ava.php"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://subastas.boe.es/subastas_ava.php",
}

form_data = {
    "accion": "Buscar",
    "page_hits": "500",
    "campo[0]": "SUBASTA.TIPOSUBASTA",
    "dato[0]": "",
    "campo[2]": "SUBASTA.ESTADO.CODIGO",
    "dato[2]": "PU",
    "campo[3]": "BIEN.TIPOBIEN",
    "dato[3]": "I",
}

print("Enviando POST al BOE...")
r = requests.post(URL, data=form_data, headers=HEADERS, timeout=30)
print(f"Status HTTP: {r.status_code}")
print(f"URL final: {r.url}")
print()

soup = BeautifulSoup(r.text, "html.parser")
texto = soup.get_text(separator="\n")

# Mostrar líneas que contienen números o palabras clave de conteo
print("=== LÍNEAS RELEVANTES DEL TEXTO ===")
for line in texto.splitlines():
    line = line.strip()
    if line and any(kw in line.lower() for kw in ["subasta", "resultado", "encontr", "total", "registro"]):
        print(repr(line))

print()
print("=== PRIMEROS 3000 CARACTERES DEL HTML ===")
print(r.text[:3000])
