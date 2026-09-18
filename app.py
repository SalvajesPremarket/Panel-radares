from datetime import datetime, timedelta, timezone
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import yfinance as yf
import os
import json
import time
import requests
import pandas as pd
import pandas_ta_classic as ta
import plotly.express as px
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockSnapshotRequest
from openai import OpenAI
from threading import Thread

st.set_page_config(page_title="Scanner Pre Market", layout="wide")

print("⚙️ Iniciando el Sistema de Radar Definitivo...")

# ==========================================
# 💾 PERSISTENCIA DE FILTROS
# ==========================================
RUTA_CONFIG = os.path.join(os.getcwd(), "config_filtros.json")

VALORES_POR_DEFECTO = {
    "precio_min": 2.0,
    "precio_max": 20.0,
    "gap_min": 7.0,
    "gap_max": 500.0,
    "flotacion_max": 10_000_000,
    "vol_rel_min": 1.3,
    "intervalo_refresco": 15,
    "direccion_cruce": "Hacia arriba",
    "macd_signo": "Positivo",
    "top_n": 10,
}

def cargar_config():
    config = VALORES_POR_DEFECTO.copy()
    try:
        with open(RUTA_CONFIG, "r") as f:
            guardado = json.load(f)
            config.update(guardado)
    except Exception:
        pass
    return config

def guardar_config(config):
    try:
        with open(RUTA_CONFIG, "w") as f:
            json.dump(config, f)
    except Exception as e:
        print(f"⚠️ No se pudo guardar la configuración: {e}")

if "config_filtros" not in st.session_state:
    st.session_state.config_filtros = cargar_config()

cfg = st.session_state.config_filtros

# ==========================================
# 🎨 ESTILO OSCURO TIPO FINVIZ
# ==========================================
st.markdown("""
<style>
    .stApp {
        background-color: #0e1117;
        color: #e6e6e6;
    }
    [data-testid="stHeader"] { background-color: #0e1117; }
    [data-testid="stSidebar"] { background-color: #0e1117; }
    .block-container { padding-top: 1rem; }

    .finviz-topbar {
        background-color: #12151c;
        padding: 12px 20px;
        border-radius: 6px;
        margin-bottom: 14px;
        border: 1px solid #2a2e39;
        text-align: center;
    }
    .finviz-topbar h1 {
        color: #ffffff;
        font-size: 24px;
        margin: 0;
        font-family: Arial, sans-serif;
        text-align: center;
    }
    .finviz-badge {
        background-color: #2ecc71;
        color: white;
        font-size: 11px;
        padding: 3px 8px;
        border-radius: 3px;
        margin-left: 10px;
        vertical-align: middle;
    }
    .finviz-filterbar {
        background-color: #12151c;
        border: 1px solid #2a2e39;
        border-radius: 6px;
        padding: 12px 16px 2px 16px;
        margin-bottom: 14px;
    }
    label, .stNumberInput label, .stMarkdown, p, span {
        color: #cfd3da !important;
    }
    div[data-testid="stNumberInput"] input {
        background-color: #1a1e27;
        color: #ffffff;
    }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="finviz-topbar">
    <h1>SCANNER PRE MARKET <span class="finviz-badge">LIVE</span></h1>
</div>
""", unsafe_allow_html=True)

# ==========================================
# 📊 FILTROS (barra horizontal tipo Finviz, guardados automáticamente)
# ==========================================
st.markdown('<div class="finviz-filterbar">', unsafe_allow_html=True)
c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
with c1:
    PRECIO_MIN = st.number_input("Precio mín. ($)", value=float(cfg["precio_min"]), step=0.5)
with c2:
    PRECIO_MAX = st.number_input("Precio máx. ($)", value=float(cfg["precio_max"]), step=0.5)
with c3:
    GAP_MINIMO_PORCENTAJE = st.number_input("Gap mín. (%)", value=float(cfg["gap_min"]), step=1.0)
with c4:
    GAP_MAXIMO_PORCENTAJE = st.number_input("Gap máx. (%)", value=float(cfg["gap_max"]), step=10.0)
with c5:
    FLOTACION_MAXIMA_ACCIONES = st.number_input("Flotación máx.", value=int(cfg["flotacion_max"]), step=1_000_000)
with c6:
    VOLUMEN_RELATIVO_MINIMO = st.number_input("Vol. relativo mín.", value=float(cfg["vol_rel_min"]), step=0.1)
with c7:
    INTERVALO_REFRESCO_SEGUNDOS = st.selectbox(
        "Refresco (seg)",
        options=[1, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        index=[1, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15].index(cfg["intervalo_refresco"])
    )
st.markdown('</div>', unsafe_allow_html=True)

# Guardar cualquier cambio en los filtros automáticamente
nuevo_cfg = {
    "precio_min": PRECIO_MIN,
    "precio_max": PRECIO_MAX,
    "gap_min": GAP_MINIMO_PORCENTAJE,
    "gap_max": GAP_MAXIMO_PORCENTAJE,
    "flotacion_max": FLOTACION_MAXIMA_ACCIONES,
    "vol_rel_min": VOLUMEN_RELATIVO_MINIMO,
    "intervalo_refresco": INTERVALO_REFRESCO_SEGUNDOS,
    "direccion_cruce": DIRECCION_CRUCE,
    "macd_signo": MACD_SIGNO,
    "top_n": TOP_N,
}
if nuevo_cfg != st.session_state.config_filtros:
    st.session_state.config_filtros = nuevo_cfg
    guardar_config(nuevo_cfg)

MINUTOS_NOTICIA_RECIENTE = 60
TICKERS_POR_MINUTO = 15000
TAMANO_LOTE_SNAPSHOT = 300
MAX_CANDIDATOS_A_ANALIZAR = TOP_N
INTERVALO_ESCANEO_SEGUNDOS = INTERVALO_REFRESCO_SEGUNDOS
VENTANA_CRUCE_EMA_MINUTOS = 15
MARGEN_PROXIMIDAD_EMA = 0.05

NOMBRE_ARCHIVO_HTML = "radar_premarket.html"

ALPACA_API_KEY = st.secrets["ALPACA_API_KEY"]
ALPACA_SECRET_KEY = st.secrets["ALPACA_SECRET_KEY"]
DEEPSEEK_API_KEY = st.secrets["DEEPSEEK_API_KEY"]

TELEGRAM_BOT_TOKEN = st.secrets["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = "-1004440734539"

trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
data_client = StockHistoricalDataClient(api_key=ALPACA_API_KEY, secret_key=ALPACA_SECRET_KEY)
deepseek_client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com")

CACHE_FLOAT = {}
CACHE_VOL_PROMEDIO = {}

if "ULTIMOS_RESULTADOS" not in globals():
    ULTIMOS_RESULTADOS = []
    ULTIMA_ACTUALIZACION = None

if "bot_on" not in st.session_state:
    st.session_state.bot_on = True

BOT_ENCENDIDO = st.session_state.bot_on

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

if "universo_mercado" not in st.session_state:
    st.session_state.universo_mercado = cargar_universo_mercado()
    print(f"📊 ¡Éxito! Bot cargado con {len(st.session_state.universo_mercado)} activos del mercado completo.")

UNIVERSO_MERCADO = st.session_state.universo_mercado

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

def calcular_ema_macd(ticker, direccion_cruce="Hacia arriba", macd_signo="Positivo"):
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

        cruza_arriba = direccion_cruce == "Hacia arriba"

        if cruza_arriba:
            cerca_de_ema = precio_act > ema_act and (precio_act - ema_act) / ema_act <= MARGEN_PROXIMIDAD_EMA
        else:
            cerca_de_ema = precio_act < ema_act and (ema_act - precio_act) / ema_act <= MARGEN_PROXIMIDAD_EMA

        cruzo_recientemente = False
        for i in range(-VENTANA_CRUCE_EMA_MINUTOS, -1):
            precio_prev_i, precio_act_i = cierres.iloc[i - 1], cierres.iloc[i]
            ema_prev_i, ema_act_i = ema20.iloc[i - 1], ema20.iloc[i]
            if pd.isna(ema_prev_i) or pd.isna(ema_act_i):
                continue
            if cruza_arriba:
                if precio_prev_i <= ema_prev_i and precio_act_i > ema_act_i:
                    cruzo_recientemente = True
                    break
            else:
                if precio_prev_i >= ema_prev_i and precio_act_i < ema_act_i:
                    cruzo_recientemente = True
                    break

        cruzando_ema20 = cerca_de_ema and cruzo_recientemente

        columnas_macd = [c for c in macd_df.columns if c.startswith('MACD_')]
        macd_line = macd_df[columnas_macd[0]].iloc[-1] if columnas_macd else None
        if macd_signo == "Positivo":
            macd_cumple = macd_line is not None and not pd.isna(macd_line) and macd_line > 0
        else:
            macd_cumple = macd_line is not None and not pd.isna(macd_line) and macd_line < 0

        return cruzando_ema20, macd_cumple
    except Exception as e:
        print(f"      ⚠️ [DEBUG] Error yfinance .history en {ticker}: {e}")
        return False, False

def tiene_noticia_reciente(ticker):
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
        <div class="info">Auto-refresco cada 30 seg.</div>
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
    global ULTIMOS_RESULTADOS, ULTIMA_ACTUALIZACION

    print(f"\n🔄 [{datetime.now().strftime('%H:%M:%S')}] Buscando en el mercado completo...")

    pausa_entre_lotes = 60.0 / (TICKERS_POR_MINUTO / TAMANO_LOTE_SNAPSHOT)

    snapshots = {}
    for i in range(0, len(UNIVERSO_MERCADO), TAMANO_LOTE_SNAPSHOT):
        if not BOT_ENCENDIDO:
            return
        lote = UNIVERSO_MERCADO[i:i + TAMANO_LOTE_SNAPSHOT]
        try:
            filtro = StockSnapshotRequest(symbol_or_symbols=lote)
            resultado_lote = data
