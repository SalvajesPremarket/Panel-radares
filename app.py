from datetime import datetime, timedelta, timezone
import streamlit as st
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

print("⚙️ Iniciando el Sistema de Radar Definitivo...")

# ==========================================
# 📊 CONFIGURACIÓN GENERAL Y FILTROS
# ==========================================
PRECIO_MIN = 2.0
PRECIO_MAX = 20.0
GAP_MINIMO_PORCENTAJE = 7.0
GAP_MAXIMO_PORCENTAJE = 500.0
FLOTACION_MAXIMA_ACCIONES = 10_000_000
VOLUMEN_RELATIVO_MINIMO = 1.3   # bajado de 2.0 a 1.3 para dejar pasar más candidatos; subilo si querés exigir más fuerza
MINUTOS_NOTICIA_RECIENTE = 60   # ventana para considerar una noticia "de última hora"
TICKERS_POR_MINUTO = 15000      # ritmo de escaneo objetivo contra la API de Alpaca
TAMANO_LOTE_SNAPSHOT = 300
MAX_CANDIDATOS_A_ANALIZAR = 15
INTERVALO_ESCANEO_SEGUNDOS = 2  # pausa entre un ciclo completo y el siguiente
VENTANA_CRUCE_EMA_MINUTOS = 15  # busca el cruce en cualquiera de las últimas N velas de 1min, no solo en la última
MARGEN_PROXIMIDAD_EMA = 0.05    # qué tan lejos de la EMA20 puede estar el precio actual (5% en vez de 2%)

# ==========================================
# 🗂 NOMBRE DE ARCHIVO ÚNICO PARA ESTE BOT
# ==========================================
NOMBRE_ARCHIVO_HTML = "radar_premarket.html"

# Credenciales Globales
ALPACA_API_KEY = st.secrets["ALPACA_API_KEY"]
ALPACA_SECRET_KEY = st.secrets["ALPACA_SECRET_KEY"]
DEEPSEEK_API_KEY = st.secrets["DEEPSEEK_API_KEY"]

# Telegram
TELEGRAM_BOT_TOKEN = st.secrets["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = "-1004440734539"

trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
data_client = StockHistoricalDataClient(api_key=ALPACA_API_KEY, secret_key=ALPACA_SECRET_KEY)
deepseek_client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com")

UNIVERSO_MERCADO = []
CACHE_FLOAT = {}
CACHE_VOL_PROMEDIO = {}
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
        mensaje_html = f"⚡️ <b>SCANNER PRE MARKET 1.1.1</b>\n<pre>{texto_tabla}</pre>"
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


def calcular_datos_fundamentales(ticker):
    """Devuelve (float_shares, volumen_promedio) usando yfinance, con caché por ticker."""
    global CACHE_FLOAT, CACHE_VOL_PROMEDIO
    if ticker in CACHE_FLOAT and ticker in CACHE_VOL_PROMEDIO:
        return CACHE_FLOAT[ticker], CACHE_VOL_PROMEDIO[ticker]
    try:
        info = yf.Ticker(ticker).info
        float_shares = info.get('floatShares')
        vol_promedio = info.get('averageVolume') or info.get('averageDailyVolume10Day')
        CACHE_FLOAT[ticker] = float_shares
        CACHE_VOL_PROMEDIO[ticker] = vol_promedio
        return float_shares, vol_promedio
    except Exception as e:
        print(f"      ⚠️ [DEBUG] Error yfinance .info en {ticker}: {e}")
        return None, None


def calcular_ema_macd(ticker):
    """Devuelve (cruzo_recientemente_ema20, macd_positivo) usando velas de 1 minuto.
    En vez de exigir que el cruce pase justo en la última vela, busca si el precio
    cruzó de abajo hacia arriba de la EMA20 en cualquiera de las últimas
    VENTANA_CRUCE_EMA_MINUTOS velas, y que el precio actual siga relativamente
    cerca de la EMA20 (dentro de MARGEN_PROXIMIDAD_EMA)."""
    try:
        df = yf.Ticker(ticker).history(period="5d", interval="1m")
        if df is None or len(df) < 40:
            return False, False

        cierres = df['Close']
        ema20 = ta.ema(cierres, length=20)
        macd_df = ta.macd(cierres)

        if ema20 is None or macd_df is None or len(ema20) < VENTANA_CRUCE_EMA_MINUTOS + 1:
            return False, False

        precio_act = cierres.iloc[-1]
        ema_act = ema20.iloc[-1]

        if pd.isna(ema_act) or ema_act <= 0:
            return False, False

        # El precio actual debe estar por encima de la EMA20 y no demasiado lejos de ella
        cerca_de_ema = precio_act > ema_act and (precio_act - ema_act) / ema_act <= MARGEN_PROXIMIDAD_EMA

        # Buscar el cruce (de abajo hacia arriba) en cualquiera de las últimas N velas
        cruzo_recientemente = False
        for i in range(-VENTANA_CRUCE_EMA_MINUTOS, -1):
            precio_prev_i, precio_act_i = cierres.iloc[i - 1], cierres.iloc[i]
            ema_prev_i, ema_act_i = ema20.iloc[i - 1], ema20.iloc[i]
            if pd.isna(ema_prev_i) or pd.isna(ema_act_i):
                continue
            if precio_prev_i <= ema_prev_i and precio_act_i > ema_act_i:
                cruzo_recientemente = True
                break

        cruzando_ema20 = cerca_de_ema and cruzo_recientemente

        columnas_macd = [c for c in macd_df.columns if c.startswith('MACD_')]
        macd_line = macd_df[columnas_macd[0]].iloc[-1] if columnas_macd else None
        macd_positivo = macd_line is not None and not pd.isna(macd_line) and macd_line > 0

        return cruzando_ema20, macd_positivo
    except Exception as e:
        print(f"      ⚠️ [DEBUG] Error yfinance .history en {ticker}: {e}")
        return False, False


def tiene_noticia_reciente(ticker):
    """Consulta la API de noticias de Alpaca por artículos en la última hora."""
    try:
        desde = (datetime.now(timezone.utc) - timedelta(minutes=MINUTOS_NOTICIA_RECIENTE)).strftime("%Y-%m-%dT%H:%M:%SZ")
        url = "https://data.alpaca.markets/v1beta1/news"
        cabeceras = {
            "APCA-API-KEY-ID": ALPACA_API_KEY,
            "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY
        }
        parametros = {"symbols": ticker, "start": desde, "limit": 1}
        respuesta = requests.get(url, headers=cabeceras, params=parametros, timeout=8)
        if respuesta.status_code == 200:
            noticias = respuesta.json().get("news", [])
            return len(noticias) > 0
        return False
    except:
        return False


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
        <title>📊 SCANNER PRE MARKET 1.1.1</title>
        <meta http-equiv="refresh" content="30">
        <style>
            body {{ background-color: #121212; color: #00ffcc; font-family: 'Courier New', Courier, monospace; padding: 20px; }}
            pre {{ background-color: #1e1e1e; padding: 25px; border-radius: 8px; border: 1px solid #333; font-size: 14px; color: #ffffff; line-height: 1.5; }}
            h2 {{ color: #00ffcc; font-family: Arial, sans-serif; text-align: center; margin-bottom: 2px; }}
            .info {{ color: #888; font-size: 11px; text-align: center; margin-bottom: 20px; }}
        </style>
    </head>
    <body>
        <h2>⚡️ SCANNER PRE MARKET 1.1.1</h2>
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

    # Pausa entre lotes de snapshot para no superar TICKERS_POR_MINUTO
    pausa_entre_lotes = 60.0 / (TICKERS_POR_MINUTO / TAMANO_LOTE_SNAPSHOT)

    snapshots = {}
    for i in range(0, len(UNIVERSO_MERCADO), TAMANO_LOTE_SNAPSHOT):
        if not BOT_ENCENDIDO:
            return  # se apagó a mitad del escaneo
        lote = UNIVERSO_MERCADO[i:i + TAMANO_LOTE_SNAPSHOT]
        try:
            filtro = StockSnapshotRequest(symbol_or_symbols=lote)
            resultado_lote = data_client.get_stock_snapshot(filtro)
            if resultado_lote:
                snapshots.update(resultado_lote)
        except:
            pass
        time.sleep(pausa_entre_lotes)

    # --- Filtros "baratos" que solo usan el snapshot: precio y gap% ---
    preseleccion = []
    for ticker, snap in snapshots.items():
        if not snap or not snap.latest_trade or not snap.daily_bar or not snap.previous_daily_bar:
            continue

        precio_actual = snap.latest_trade.price
        precio_cierre_anterior = snap.previous_daily_bar.close
        volumen_dia = snap.daily_bar.volume

        if precio_cierre_anterior <= 0:
            continue
        if not (PRECIO_MIN <= precio_actual <= PRECIO_MAX):
            continue

        cambio_porcentaje = ((precio_actual - precio_cierre_anterior) / precio_cierre_anterior) * 100
        if not (GAP_MINIMO_PORCENTAJE <= cambio_porcentaje <= GAP_MAXIMO_PORCENTAJE):
            continue

        preseleccion.append({
            "ticker": ticker,
            "precio": precio_actual,
            "cambio_pct": cambio_porcentaje,
            "volumen_dia": volumen_dia,
            "actualizado": snap.latest_trade.timestamp
        })

    if not preseleccion:
        print("   ⏳ Sin candidatos que cumplan precio/gap% todavía.")
        return

    # --- Filtros "caros": flotación, volumen relativo, EMA20, MACD, noticias ---
    print(f"   [DEBUG] Preselección por precio/gap%: {len(preseleccion)} candidatos: {[c['ticker'] for c in preseleccion]}")
    candidatos_finales = []
    fallo_float_sin_dato = 0
    fallo_float_muy_alto = 0
    fallo_vol_promedio_sin_dato = 0
    fallo_vol_relativo = 0
    fallo_ema_macd = 0
    valores_vol_relativo_fallidos = []
    for c in preseleccion:
        ticker = c['ticker']

        float_shares, vol_promedio = calcular_datos_fundamentales(ticker)
        if float_shares is None:
            fallo_float_sin_dato += 1
            continue
        if float_shares >= FLOTACION_MAXIMA_ACCIONES:
            fallo_float_muy_alto += 1
            continue
        if not vol_promedio or vol_promedio <= 0:
            fallo_vol_promedio_sin_dato += 1
            continue

        volumen_relativo = c['volumen_dia'] / vol_promedio
        if volumen_relativo < VOLUMEN_RELATIVO_MINIMO:
            fallo_vol_relativo += 1
            valores_vol_relativo_fallidos.append((ticker, round(volumen_relativo, 2)))
            continue

        cruzando_ema20, macd_positivo = calcular_ema_macd(ticker)
        if not (cruzando_ema20 and macd_positivo):
            fallo_ema_macd += 1
            continue

        c['float_shares'] = float_shares
        c['volumen_relativo'] = volumen_relativo
        c['tiene_noticia'] = tiene_noticia_reciente(ticker)
        candidatos_finales.append(c)

    if not candidatos_finales:
        top10_vol_relativo = sorted(valores_vol_relativo_fallidos, key=lambda x: x[1], reverse=True)[:10]
        print(f"   ⏳ Ningún candidato cumple todos los filtros técnicos todavía. "
              f"[DEBUG] sin dato de float: {fallo_float_sin_dato}, "
              f"float demasiado alto: {fallo_float_muy_alto}, "
              f"sin dato de vol. promedio: {fallo_vol_promedio_sin_dato}, "
              f"por volumen relativo: {fallo_vol_relativo}, por EMA20/MACD: {fallo_ema_macd}")
        print(f"   [DEBUG] Top 10 volumen relativo entre los que fallaron ese filtro: {top10_vol_relativo}")
        return

    candidatos_finales = sorted(candidatos_finales, key=lambda x: x['actualizado'], reverse=True)
    top_candidatos = candidatos_finales[:MAX_CANDIDATOS_A_ANALIZAR]

    tabla_texto = f"{'TICK':<5}|{'PRE':>5}|{'CHG%':>4}|{'VOL':>5}|{'FLT':>5}\n"
    tabla_texto += "-" * 28 + "\n"
    for c in top_candidatos:
        ticker_mostrado = f"🔥{c['ticker']}" if c['tiene_noticia'] else c['ticker']
        vol_formateado = formatear_numero_grande(c['volumen_dia'])
        flt_formateado = formatear_numero_grande(c['float_shares'])
        chg_texto = f"{c['cambio_pct']:.0f}%"
        tabla_texto += f"{ticker_mostrado:<5}|{c['precio']:>5.2f}|{chg_texto:>4}|{vol_formateado:>5}|{flt_formateado:>5}\n"

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
ventana.title("Control Scanner Pre Market")
ventana.geometry("280x180")
ventana.resizable(False, False)
ventana.attributes("-topmost", True)  # Siempre encima de las demás ventanas
ventana.configure(bg="#121212")

titulo = tk.Label(
    ventana, text="⚡ SCANNER PRE MARKET 1.1.1",
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
