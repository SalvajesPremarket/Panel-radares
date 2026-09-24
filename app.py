from datetime import datetime, timedelta, timezone
import yfinance as yf
import os
import time
import requests
import pandas as pd
import pandas_ta_classic as ta
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockSnapshotRequest
from openai import OpenAI
from threading import Thread
import webbrowser
import tkinter as tk

print("⚙️ Iniciando el Sistema de Radar Definitivo...")

# ==========================================
# 📊 CONFIGURACIÓN GENERAL Y FILTROS
# ==========================================
PRECIO_MIN = 0.5
PRECIO_MAX = 20.0
GAP_MINIMO_PORCENTAJE = 0.1
GAP_MAXIMO_PORCENTAJE = 500.0
VOLUMEN_MINIMO_5S = 15000  # Volumen mínimo negociado dentro de la ventana de escaneo (5s)
INTERVALO_ESCANEO_SEGUNDOS = 5
TAMANO_LOTE_SNAPSHOT = 300
MAX_CANDIDATOS_A_ANALIZAR = 15

# ==========================================
# 🗂 NOMBRE DE ARCHIVO ÚNICO PARA ESTE BOT
# ==========================================
NOMBRE_ARCHIVO_HTML = "radar_salvajes.html"

# Credenciales Globales
ALPACA_API_KEY = "PKS25MKAHSFGTQTSXUSWQC46ZE"
ALPACA_SECRET_KEY = "cnhS7BpuWw5dQopXm98giNc1B8dXwKGzb93rioeuhd3"
DEEPSEEK_API_KEY = "sk-8f7b21227e494547931f908d5f9068ed"

# Telegram
TELEGRAM_BOT_TOKEN = "8776037533:AAFsIze3nF14gWANVbHtLS9AL-AiPTLDndE"
TELEGRAM_CHAT_ID = "-1004440734539"

trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
data_client = StockHistoricalDataClient(api_key=ALPACA_API_KEY, secret_key=ALPACA_SECRET_KEY)
deepseek_client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com")

UNIVERSO_MERCADO = []
CACHE_FLOAT = {}
CACHE_VOLUMEN_ANTERIOR = {}  # Guarda el volumen diario acumulado del escaneo anterior, por ticker
BOT_ENCENDIDO = False  # Controlado ahora por el botón de la ventana flotante


def cargar_universo_mercado():
    print("🌐 Descargando universo completo del mercado desde Alpaca (puede tardar unos segundos)...")
    solicitud = GetAssetsRequest(asset_class=AssetClass.US_EQUITY, status=AssetStatus.ACTIVE)
    activos = trading_client.get_all_assets(solicitud)

    tickers = [
        a.symbol for a in activos
        if a.tradable
        and a.exchange in ("NASDAQ", "NYSE", "AMEX", "ARCA")
        and "." not in a.symbol
        and "-" not in a.symbol
    ]
    print(f"🌐 Universo cargado: {len(tickers)} tickers activos y operables.")
    return tickers


UNIVERSO_MERCADO = cargar_universo_mercado()
print(f"📊 ¡Éxito! Bot cargado con {len(UNIVERSO_MERCADO)} activos del mercado completo.")

id_mensaje_activo = None


def enviar_radar_a_telegram(texto_tabla):
    global id_mensaje_activo
    try:
        mensaje_html = f"⚡️ <b>SCANNER SALVAJES PRE MARKET</b>\n<pre>{texto_tabla}</pre>"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": mensaje_html,
            "parse_mode": "HTML"
        }
        cabeceras = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

        if id_mensaje_activo is None:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            respuesta = requests.post(url, json=payload, headers=cabeceras, timeout=15)
            if respuesta.status_code == 200:
                id_mensaje_activo = respuesta.json()["result"]["message_id"]
                print("   ✅ ¡Mensaje inicial enviado a Telegram!")
            else:
                print(f"   ❌ Telegram rechazó el mensaje. Estado HTTP: {respuesta.status_code} - {respuesta.text}")
        else:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
            payload["message_id"] = id_mensaje_activo
            respuesta = requests.post(url, json=payload, headers=cabeceras, timeout=15)
            if respuesta.status_code != 200 and "message is not modified" not in respuesta.text:
                print(f"   ❌ Error al editar. Estado HTTP: {respuesta.status_code} - {respuesta.text}")
    except Exception as e:
        print(f"   ⚠️ Error de red con Telegram: {e}")
def calcular_tecnicos_rapido(ticker):
    global CACHE_FLOAT
    if ticker in CACHE_FLOAT:
        return CACHE_FLOAT[ticker]
    try:
        float_shares = None
        try:
            info = yf.Ticker(ticker).info
            float_shares = info.get('floatShares')
            CACHE_FLOAT[ticker] = float_shares
        except:
            float_shares = None
        return float_shares
    except:
        return None


def formatear_numero_grande(numero):
    try:
        numero = float(numero)
    except (TypeError, ValueError):
        return "N/A"
    if numero >= 1_000_000:
        return f"{numero / 1_000_000:.1f}M"
    elif numero >= 1_000:
        return f"{numero / 1_000:.0f}K"
    else:
        return f"{numero:.0f}"


def actualizar_cuadro_flotante_html(texto_tabla):
    ruta_archivo = os.path.join(os.getcwd(), NOMBRE_ARCHIVO_HTML)
    contenido_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>📊 Radar Premium Flotante</title>
        <meta http-equiv="refresh" content="30">
        <style>
            body {{ background-color: #121212; color: #00ffcc; font-family: 'Courier New', Courier, monospace; padding: 20px; }}
            pre {{ background-color: #1e1e1e; padding: 25px; border-radius: 8px; border: 1px solid #333; font-size: 14px; color: #ffffff; line-height: 1.5; }}
            h2 {{ color: #00ffcc; font-family: Arial, sans-serif; text-align: center; margin-bottom: 2px; }}
            .info {{ color: #888; font-size: 11px; text-align: center; margin-bottom: 20px; }}
        </style>
    </head>
    <body>
        <h2>⚡️ Scanner Salvajes Pre Market</h2>
        <div class="info">Auto-refresco cada 30 seg. Arrastra esta pestaña fuera para hacerla un cuadro flotante.</div>
        <pre>{texto_tabla}</pre>
    </body>
    </html>
    """
    try:
        with open(ruta_archivo, "w", encoding="utf-8") as f:
            f.write(contenido_html)
        return ruta_archivo
    except:
        return None


def ejecutar_ciclo_escaneo():
    print(f"\n🔄 [{datetime.now().strftime('%H:%M:%S')}] Buscando en el mercado completo...")

    snapshots = {}
    for i in range(0, len(UNIVERSO_MERCADO), TAMANO_LOTE_SNAPSHOT):
        lote = UNIVERSO_MERCADO[i:i + TAMANO_LOTE_SNAPSHOT]
        try:
            filtro = StockSnapshotRequest(symbol_or_symbols=lote)
            resultado_lote = data_client.get_stock_snapshot(filtro)
            if resultado_lote:
                snapshots.update(resultado_lote)
        except:
            continue

    candidatos_filtrados = []
    for ticker, snap in snapshots.items():
        if not snap or not snap.latest_trade or not snap.daily_bar or not snap.previous_daily_bar:
            continue

        precio_actual = snap.latest_trade.price
        precio_cierre_anterior = snap.previous_daily_bar.close
        volumen_dia = snap.daily_bar.volume

        # Volumen negociado desde el escaneo anterior (~ventana de 5 segundos)
        volumen_anterior = CACHE_VOLUMEN_ANTERIOR.get(ticker)
        CACHE_VOLUMEN_ANTERIOR[ticker] = volumen_dia
        if volumen_anterior is None:
            continue  # primera lectura de este ticker, todavía no hay ventana que medir
        volumen_5s = max(volumen_dia - volumen_anterior, 0)

        if precio_cierre_anterior <= 0 or volumen_5s < VOLUMEN_MINIMO_5S:
            continue

        if PRECIO_MIN <= precio_actual <= PRECIO_MAX:
            cambio_porcentaje = ((precio_actual - precio_cierre_anterior) / precio_cierre_anterior) * 100

            if GAP_MINIMO_PORCENTAJE <= cambio_porcentaje <= GAP_MAXIMO_PORCENTAJE:
                candidatos_filtrados.append({
                    "ticker": ticker,
                    "precio": precio_actual,
                    "cambio_pct": cambio_porcentaje,
                    "volumen_5s": volumen_5s,
                    "actualizado": snap.latest_trade.timestamp
                })

    candidatos_filtrados = sorted(candidatos_filtrados, key=lambda x: x['actualizado'], reverse=True)
    top_candidatos = candidatos_filtrados[:MAX_CANDIDATOS_A_ANALIZAR]

    if not top_candidatos:
        print("   ⏳ Buscando... Sin candidatos salvajes detectados todavía.")
        return

    tabla_texto = f"{'TICK':<5}|{'PX':>5}|{'CHG%':>4}|{'FLT':>5}|{'V5S':>5}\n"
    tabla_texto += "-" * 28 + "\n"
    for c in top_candidatos:
        ticker = c['ticker']
        float_shares = calcular_tecnicos_rapido(ticker)
        float_formateado = formatear_numero_grande(float_shares) if float_shares else "N/A"
        vol_formateado = formatear_numero_grande(c['volumen_5s'])
        chg_texto = f"{c['cambio_pct']:.0f}%"
        tabla_texto += f"{ticker:<5}|{c['precio']:>5.2f}|{chg_texto:>4}|{float_formateado:>5}|{vol_formateado:>5}\n"

    print("   📊 ¡Candidatos encontrados! Actualizando canales...")
    enviar_radar_a_telegram(tabla_texto)
    actualizar_cuadro_flotante_html(tabla_texto)


# ==========================================
# 🔄 PROCESAMIENTO ASÍNCRONO DEL SCANNER
# ==========================================
def bucle_control_scanner():
    while True:
        if BOT_ENCENDIDO:
            try:
                ejecutar_ciclo_escaneo()
            except Exception as e:
                print(f"⚠️ Error en escaneo: {e}")
        time.sleep(INTERVALO_ESCANEO_SEGUNDOS)


# Lanzar el hilo del scanner en segundo plano (arranca en pausa hasta que le des al botón)
hilo_servicio = Thread(target=bucle_control_scanner, daemon=True)
hilo_servicio.start()


# ==========================================
# 🎛 VENTANA FLOTANTE CON BOTÓN DE ENCENDIDO/APAGADO
# ==========================================
def abrir_web():
    ruta_html = os.path.join(os.getcwd(), NOMBRE_ARCHIVO_HTML)
    if os.path.exists(ruta_html):
        webbrowser.open(f"file:///{ruta_html}")
        print(f"🌐 Abriendo {NOMBRE_ARCHIVO_HTML} en tu navegador...")
    else:
        print("⏳ El archivo HTML se creará en cuanto el scanner haga su primer envío.")


def alternar_bot():
    global BOT_ENCENDIDO
    BOT_ENCENDIDO = not BOT_ENCENDIDO
    if BOT_ENCENDIDO:
        boton.config(text="🔴  DETENER SCANNER", bg="#c0392b")
        etiqueta_estado.config(text="Estado: 🟢 OPERANDO", fg="#2ecc71")
        print("\n   ⚙️ El Bot ahora está: ¡ENCENDIDO!")
        Thread(target=ejecutar_ciclo_escaneo, daemon=True).start()  # primer escaneo inmediato
    else:
        boton.config(text="🟢  INICIAR SCANNER", bg="#27ae60")
        etiqueta_estado.config(text="Estado: 🔴 EN PAUSA", fg="#e74c3c")
        print("\n   ⚙️ El Bot ahora está: ¡PAUSADO!")


ventana = tk.Tk()
ventana.title("Control Scanner Salvajes")
ventana.geometry("280x180")
ventana.resizable(False, False)
ventana.attributes("-topmost", True)  # Siempre encima de las demás ventanas
ventana.configure(bg="#121212")

titulo = tk.Label(
    ventana, text="⚡ SCANNER SALVAJES PRE MARKET",
    fg="#00ffcc", bg="#121212", font=("Arial", 10, "bold"), wraplength=260
)
titulo.pack(pady=(12, 4))

etiqueta_estado = tk.Label(
    ventana, text="Estado: 🔴 EN PAUSA",
    fg="#e74c3c", bg="#121212", font=("Arial", 10)
)
etiqueta_estado.pack(pady=4)

boton = tk.Button(
    ventana, text="🟢  INICIAR SCANNER", command=alternar_bot,
    bg="#27ae60", fg="white", font=("Arial", 11, "bold"),
    width=22, height=2, relief="flat", cursor="hand2", activebackground="#1e8449"
)
boton.pack(pady=8)

boton_web = tk.Button(
    ventana, text="🌐 Abrir cuadro en navegador", command=abrir_web,
    bg="#2c3e50", fg="white", font=("Arial", 8),
    relief="flat", cursor="hand2"
)
boton_web.pack(pady=2)

ventana.mainloop()
