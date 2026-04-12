# BOE Monitor — Subastas Inmuebles

Bot que monitoriza el portal [subastas.boe.es](https://subastas.boe.es) y envía una notificación por **Telegram** cuando cambia el número de subastas de **Inmuebles** en estado **Próxima apertura** o **Celebrándose**.

## Funcionamiento

- Se conecta **3 veces al día** (08:00, 14:00 y 22:00 hora España)
- Solo consulta el **número total de subastas** — nada de descargar listados
- Si el número cambia → notificación instantánea por Telegram
- Si no hay cambios → silencio total

## Instalación local

```bash
pip install -r requirements.txt
```

Edita `config.py` con tu TOKEN y CHAT_ID de Telegram y ejecuta:

```bash
python bot.py
```

## Despliegue en Render (recomendado)

1. Sube este repo a GitHub (sin `config.py` — usa variables de entorno)
2. En [Render](https://render.com) crea un **Background Worker**
3. Añade las variables de entorno:
   - `TELEGRAM_TOKEN`
   - `TELEGRAM_CHAT_ID`
4. Build Command: `pip install -r requirements.txt`
5. Start Command: `python bot.py`

> En Render, el script lee las variables de entorno automáticamente.
> Las horas están configuradas en **UTC** (06:00 / 12:00 / 20:00 = 08:00 / 14:00 / 22:00 CEST).

## Estructura

```
boe-monitor/
├── bot.py           # Script principal
├── config.py        # Credenciales (NO subir a GitHub)
├── requirements.txt # Dependencias Python
└── estado.json      # Estado auto-generado en primera ejecución
```
