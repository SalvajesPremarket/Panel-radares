from datetime import datetime, timedelta, timezone
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import yfinance as yf
import os
import json
import time
import requests
from urllib.parse import quote
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

# Ocultar barra superior (Share, gatito de GitHub, editar, menú), pie y badges de Streamlit
st.markdown("""
<style>
    [data-testid="stToolbar"],
    [data-testid="stAppDeployButton"],
    [data-testid="stDecoration"],
    [data-testid="stStatusWidget"],
    [data-testid="stMainMenu"],
    .stAppDeployButton,
    #MainMenu,
    footer,
    [class*="viewerBadge"],
    [class*="_profileContainer"],
    [class*="_terminalButton"],
    a[href*="github.com"][target="_blank"][class*="header"] {
        display: none !important;
        visibility: hidden !important;
    }
</style>
""", unsafe_allow_html=True)

print("⚙️ Iniciando el Sistema de Radar Definitivo...")

# ==========================================
# 🔐 CONTROL DE ACCESO: PRUEBA GRATIS 30 DÍAS + PAGO ÚNICO PAYPAL
# ==========================================
DIAS_DE_PRUEBA = 30
PRECIO_ACCESO = "65.00"
MONEDA_ACCESO = "USD"
CORREO_PAYPAL_RECEPTOR = "minorgt45@gmail.com"
GOOGLE_SCRIPT_URL = st.secrets.get("GOOGLE_SCRIPT_URL", "")

def obtener_email_usuario():
    try:
        correo = st.experimental_user.email
        if correo:
            return correo
    except Exception:
        pass
    try:
        correo = st.user.email
        if correo:
            return correo
    except Exception:
        pass
    return None

def verificar_acceso_usuario(email):
    try:
        respuesta = requests.get(
            GOOGLE_SCRIPT_URL,
            params={"action": "check_or_create", "email": email},
            timeout=10
        )
        if respuesta.status_code == 200:
            return respuesta.json()
    except Exception as e:
        print(f"⚠️ Error verificando acceso: {e}")
    return None

def mostrar_pantalla_de_pago(email, dias_restantes):
    link_pago = (
        "https://www.paypal.com/cgi-bin/webscr"
        "?cmd=_xclick"
        f"&business={CORREO_PAYPAL_RECEPTOR}"
        "&item_name=Acceso%20de%20por%20vida%20-%20Scanner%20Pre%20Market"
        f"&amount={PRECIO_ACCESO}"
        f"&currency_code={MONEDA_ACCESO}"
        f"&custom={quote(email)}"
        f"&notify_url={GOOGLE_SCRIPT_URL}"
    )
    st.markdown("""
    <style>
        .stApp { background-color: #0a0e1a; }
    </style>
    """, unsafe_allow_html=True)
    st.markdown(f"""
    <div style="max-width:520px; margin:80px auto; background:#11151f; border:1px solid #2a3348;
                border-radius:14px; padding:40px 36px; text-align:center; box-shadow:0 10px 30px rgba(0,0,0,0.5);">
        <div style="font-size:42px; margin-bottom:10px;">⏳</div>
        <h2 style="color:#FFD700; text-transform:uppercase; letter-spacing:1px; margin-bottom:6px;">
            Tu prueba gratis terminó
        </h2>
        <p style="color:#cfd3da; font-size:14px; margin-bottom:24px;">
            Tuviste {DIAS_DE_PRUEBA} días de acceso completo al Scanner Pre Market.
            Para seguir usándolo, el acceso es de <b style="color:#FFD700;">por vida</b>
            con un pago único de <b style="color:#FFD700;">${PRECIO_ACCESO} {MONEDA_ACCESO}</b>.
        </p>
        <a href="{link_pago}" target="_blank" style="display:inline-block; background:#ffc439; color:#111;
           font-weight:800; padding:14px 32px; border-radius:8px; text-decoration:none; font-size:15px;
           box-shadow:0 4px 14px rgba(255,196,57,0.4);">
            Pagar con PayPal — ${PRECIO_ACCESO}
        </a>
        <p style="color:#5c6577; font-size:11px; margin-top:22px;">
            Correo verificado: {email}<br>
            Tu acceso se activa automáticamente en cuanto PayPal confirme el pago.
        </p>
    </div>
    """, unsafe_allow_html=True)

def pedir_correo():
    st.markdown("""
    <style>
        .stApp { background-color: #0a0e1a; }
    </style>
    """, unsafe_allow_html=True)
    st.markdown("### Ingresa tu correo para acceder a tu prueba gratis de 30 días")
    with st.form("form_correo"):
        correo = st.text_input("Correo electrónico")
        enviado = st.form_submit_button("Entrar")
    if enviado:
        correo = correo.strip().lower()
        if "@" in correo and "." in correo.split("@")[-1]:
            st.session_state["email_ingresado"] = correo
            st.rerun()
        else:
            st.error("Escribe un correo válido.")
    st.stop()

_email_usuario = obtener_email_usuario() or st.session_state.get("email_ingresado")

if not _email_usuario:
    pedir_correo()

_dias_restantes_prueba = None
_info_acceso = verificar_acceso_usuario(_email_usuario) if GOOGLE_SCRIPT_URL else None

if _info_acceso is None:
    st.error("No se pudo verificar tu acceso. Intenta de nuevo en unos segundos.")
    st.stop()

if not _info_acceso.get("paid") and not _info_acceso.get("trial_activo"):
    mostrar_pantalla_de_pago(_email_usuario, 0)
    st.stop()
elif not _info_acceso.get("paid"):
    _dias_restantes_prueba = _info_acceso.get("dias_restantes")

# ==========================================
# 💾 PERSISTENCIA DE FILTROS
# ==========================================
RUTA_CONFIG = os.path.join(os.getcwd(), "config_filtros.json")

VALORES_POR_DEFECTO = {
    "precio_min": 2.0,
    "precio_max": 20.0,
    "gap_min": 4.0,
    "gap_max": 500.0,
    "flotacion_max": 20_000_000,
    "vol_rel_min": 1.8,
    "volumen_momento_min": 15000,
    "intervalo_refresco": 15,
    "direccion_cruce": "Hacia arriba",
    "macd_signo": "Neutro",
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

# Inicialización de las variables (se completan más abajo desde los controles de la interfaz)

# ==========================================
# 🎨 ESTILO OSCURO TIPO FINVIZ
# ==========================================
st.markdown("""
<style>
    .stApp {
        background-color: #0a0e1a;
        color: #e6e6e6;
    }
    [data-testid="stHeader"] { background-color: #0a0e1a; }
    [data-testid="stSidebar"] { background-color: #0a0e1a; }
    .block-container { padding-top: 1rem; }

    .finviz-topbar {
        position: relative;
        background: linear-gradient(135deg, #0d1420 0%, #131b2c 55%, #0d1420 100%);
        padding: 26px 90px;
        border-radius: 10px;
        margin-bottom: 18px;
        border: 1px solid #2a3348;
        border-bottom: 3px solid #ffd700;
        text-align: center;
        box-shadow: 0 6px 20px rgba(0,0,0,0.45);
    }
    .finviz-topbar h1 {
        color: #c9a227;
        font-size: 26px;
        margin: 0;
        font-family: 'Segoe UI', Arial, sans-serif;
        letter-spacing: 2px;
        font-weight: 700;
        text-transform: uppercase;
    }
    .finviz-topbar .subtitle {
        color: #8b93a7;
        font-size: 11px;
        letter-spacing: 3px;
        margin-top: 6px;
        text-transform: uppercase;
        font-family: Arial, sans-serif;
    }
    .finviz-badge {
        background: linear-gradient(90deg, #16c784, #0e9e68);
        color: #0a0e1a;
        font-size: 11px;
        font-weight: 700;
        padding: 3px 10px;
        border-radius: 20px;
        margin-left: 10px;
        vertical-align: middle;
        letter-spacing: 1px;
    }
    .market-figure {
        position: absolute;
        top: 50%;
        transform: translateY(-50%);
        width: 64px;
        height: 64px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 28px;
        background: #0a0e1a;
        box-shadow: 0 0 0 2px #c9a227, 0 4px 12px rgba(0,0,0,0.5);
    }
    .market-figure.bull { left: 24px; }
    .market-figure.bear { right: 24px; }
    .finviz-filterbar {
        background-color: #11151f;
        border: 1px solid #232838;
        border-radius: 10px;
        padding: 14px 18px 4px 18px;
        margin-bottom: 16px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.3);
    }
    .stMarkdown, p, span {
        color: #cfd3da !important;
    }
    label, .stNumberInput label, .stSelectbox label,
    label p, .stNumberInput label p, .stSelectbox label p,
    [data-testid="stWidgetLabel"] p, [data-testid="stWidgetLabel"] {
        color: #FFD700 !important;
        font-weight: 700 !important;
        text-transform: uppercase;
        font-size: 11px !important;
        letter-spacing: 0.5px;
    }
    div[data-testid="stNumberInput"] input {
        background-color: #1a1e27;
        color: #ffffff;
        border: 1px solid #2a3348 !important;
    }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="finviz-topbar">
    <div class="market-figure bull">
        <svg viewBox="0 0 100 100" width="52" height="52">
            <defs>
                <linearGradient id="goldGradBull" x1="10%" y1="0%" x2="95%" y2="100%">
                    <stop offset="0%" stop-color="#fff6d0"/>
                    <stop offset="22%" stop-color="#ffe680"/>
                    <stop offset="48%" stop-color="#ffcc33"/>
                    <stop offset="72%" stop-color="#d99a12"/>
                    <stop offset="100%" stop-color="#8a5f08"/>
                </linearGradient>
                <linearGradient id="goldHighlightBull" x1="0%" y1="0%" x2="100%" y2="100%">
                    <stop offset="0%" stop-color="#fffdf0"/>
                    <stop offset="100%" stop-color="#ffd84d"/>
                </linearGradient>
            </defs>
            <!-- cola -->
            <path d="M85,55 Q98,50 97,62 Q95,72 84,68" fill="none" stroke="url(#goldGradBull)" stroke-width="4" stroke-linecap="round"/>
            <!-- cuernos -->
            <path d="M40,30 Q30,10 18,12 Q26,26 34,36 Z" fill="url(#goldHighlightBull)"/>
            <path d="M50,26 Q52,6 64,4 Q60,20 54,32 Z" fill="url(#goldHighlightBull)"/>
            <!-- orejas -->
            <ellipse cx="34" cy="34" rx="4.5" ry="6" fill="url(#goldGradBull)"/>
            <!-- cuerpo -->
            <ellipse cx="58" cy="62" rx="34" ry="20" fill="url(#goldGradBull)"/>
            <ellipse cx="58" cy="55" rx="28" ry="10" fill="url(#goldHighlightBull)" opacity="0.55"/>
            <!-- patas -->
            <rect x="30" y="72" width="6" height="18" rx="2" fill="url(#goldGradBull)"/>
            <rect x="46" y="76" width="6" height="18" rx="2" fill="url(#goldGradBull)"/>
            <rect x="70" y="76" width="6" height="18" rx="2" fill="url(#goldGradBull)"/>
            <rect x="84" y="72" width="6" height="18" rx="2" fill="url(#goldGradBull)"/>
            <!-- cabeza baja embistiendo -->
            <ellipse cx="30" cy="48" rx="16" ry="13" fill="url(#goldGradBull)"/>
            <ellipse cx="24" cy="46" rx="7" ry="6" fill="url(#goldHighlightBull)" opacity="0.6"/>
            <!-- hocico -->
            <ellipse cx="18" cy="52" rx="7" ry="5.5" fill="url(#goldHighlightBull)"/>
            <circle cx="15" cy="52" r="1.4" fill="#4a2e00"/>
            <circle cx="21" cy="52" r="1.4" fill="#4a2e00"/>
            <!-- ojo -->
            <circle cx="29" cy="42" r="2.6" fill="#2a1a00"/>
            <circle cx="30" cy="41" r="0.8" fill="#fff"/>
        </svg>
    </div>
    <h1>SCANNER PRE MARKET <span class="finviz-badge">● LIVE</span></h1>
    <div class="subtitle">Radar de oportunidades en tiempo real</div>
    <div class="market-figure bear">
        <svg viewBox="0 0 100 100" width="52" height="52">
            <defs>
                <radialGradient id="goldGradBear" cx="40%" cy="30%" r="75%">
                    <stop offset="0%" stop-color="#fff6d0"/>
                    <stop offset="25%" stop-color="#ffe680"/>
                    <stop offset="55%" stop-color="#ffcc33"/>
                    <stop offset="80%" stop-color="#d99a12"/>
                    <stop offset="100%" stop-color="#8a5f08"/>
                </radialGradient>
                <linearGradient id="goldHighlightBear" x1="0%" y1="0%" x2="100%" y2="100%">
                    <stop offset="0%" stop-color="#fffdf0"/>
                    <stop offset="100%" stop-color="#ffd84d"/>
                </linearGradient>
            </defs>
            <!-- orejas -->
            <circle cx="22" cy="20" r="12" fill="url(#goldGradBear)"/>
            <circle cx="70" cy="16" r="13" fill="url(#goldGradBear)"/>
            <circle cx="22" cy="20" r="5.5" fill="#6b4a05"/>
            <circle cx="70" cy="16" r="6" fill="#6b4a05"/>
            <!-- cuerpo -->
            <ellipse cx="48" cy="62" rx="34" ry="24" fill="url(#goldGradBear)"/>
            <ellipse cx="42" cy="52" rx="24" ry="12" fill="url(#goldHighlightBear)" opacity="0.55"/>
            <!-- patas -->
            <rect x="22" y="76" width="7" height="16" rx="2" fill="url(#goldGradBear)"/>
            <rect x="66" y="80" width="7" height="16" rx="2" fill="url(#goldGradBear)"/>
            <!-- cabeza -->
            <ellipse cx="66" cy="42" rx="20" ry="17" fill="url(#goldGradBear)"/>
            <ellipse cx="60" cy="36" rx="9" ry="7" fill="url(#goldHighlightBear)" opacity="0.6"/>
            <!-- hocico alargado rugiendo -->
            <path d="M78,40 Q92,42 90,52 Q86,60 76,56 Z" fill="url(#goldGradBear)"/>
            <!-- boca abierta rugiendo -->
            <path d="M80,48 Q88,50 86,58 Q80,60 76,54 Z" fill="#3a1f00"/>
            <path d="M80,49 Q86,51 84,55" fill="none" stroke="#fff6d0" stroke-width="1.6" stroke-linecap="round"/>
            <path d="M78,55 Q83,57 85,57" fill="none" stroke="#fff6d0" stroke-width="1.6" stroke-linecap="round"/>
            <circle cx="88" cy="45" r="1.6" fill="#3a2400"/>
            <!-- ojos -->
            <circle cx="58" cy="38" r="3" fill="#2a1a00"/>
            <circle cx="72" cy="36" r="3" fill="#2a1a00"/>
            <circle cx="59" cy="37" r="0.9" fill="#fff"/>
            <circle cx="73" cy="35" r="0.9" fill="#fff"/>
        </svg>
    </div>
</div>
""", unsafe_allow_html=True)

if _dias_restantes_prueba is not None:
    st.markdown(f"""
    <div style="text-align:center; color:#FFD700; font-size:12px; font-weight:700;
                letter-spacing:0.5px; text-transform:uppercase; margin-bottom:10px;">
        🎁 Prueba gratis: {_dias_restantes_prueba} día{"s" if _dias_restantes_prueba != 1 else ""} restante{"s" if _dias_restantes_prueba != 1 else ""}
    </div>
    """, unsafe_allow_html=True)



# ==========================================
# 📊 FILTROS (barra horizontal tipo Finviz, guardados automáticamente)
# ==========================================
filtro_box = st.container(border=True)
with filtro_box:
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

    d1, d2, d3, d4 = st.columns(4)
    with d1:
        opciones_cruce = ["Hacia arriba", "Hacia abajo", "Neutro"]
        DIRECCION_CRUCE = st.selectbox(
            "Cruce EMA20",
            options=opciones_cruce,
            index=opciones_cruce.index(cfg.get("direccion_cruce", "Hacia arriba"))
        )
    with d2:
        opciones_macd = ["Positivo", "Negativo", "Neutro"]
        MACD_SIGNO = st.selectbox(
            "MACD",
            options=opciones_macd,
            index=opciones_macd.index(cfg.get("macd_signo", "Positivo"))
        )
    with d3:
        VOLUMEN_MOMENTO_MINIMO = st.number_input(
            "Vol. mínimo del momento",
            value=int(cfg.get("volumen_momento_min", 0)),
            step=1000,
            min_value=0
        )
    with d4:
        TOP_N = st.number_input(
            "Top N candidatos",
            value=int(cfg.get("top_n", 10)),
            step=1,
            min_value=1,
            max_value=50
        )

# Guardar cualquier cambio en los filtros automáticamente
nuevo_cfg = {
    "precio_min": PRECIO_MIN,
    "precio_max": PRECIO_MAX,
    "gap_min": GAP_MINIMO_PORCENTAJE,
    "gap_max": GAP_MAXIMO_PORCENTAJE,
    "flotacion_max": FLOTACION_MAXIMA_ACCIONES,
    "vol_rel_min": VOLUMEN_RELATIVO_MINIMO,
    "volumen_momento_min": VOLUMEN_MOMENTO_MINIMO,
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
        # 🔧 FIX: prepost=True para que traiga velas de premarket/afterhours en tiempo real.
        # Sin esto, fuera de horario regular (9:30-16:00 ET) yfinance devuelve
        # las últimas velas del cierre de ayer, así que "cruzó la EMA hace poco"
        # casi nunca se cumple durante premarket.
        df = yf.Ticker(ticker).history(period="5d", interval="1m", prepost=True)
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
            body {{ background-color: #121212; color: #c9a227; font-family: 'Courier New', Courier, monospace; padding: 20px; }}
            pre {{ background-color: #1e1e1e; padding: 25px; border-radius: 8px; border: 1px solid #333; font-size: 14px; color: #ffffff; line-height: 1.5; }}
            h2 {{ color: #c9a227; font-family: Arial, sans-serif; text-align: center; margin-bottom: 2px; }}
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
            resultado_lote = data_client.get_stock_snapshot(filtro)
            if resultado_lote:
                snapshots.update(resultado_lote)
        except:
            pass
        time.sleep(pausa_entre_lotes)

    # 🔧 DEBUG: cuántos snapshots llegaron en total
    print(f"   [DEBUG] Snapshots recibidos de Alpaca: {len(snapshots)}")

    preseleccion = []
    for ticker, snap in snapshots.items():
        if not snap or not snap.latest_trade or not snap.daily_bar or not snap.previous_daily_bar:
            continue

        precio_actual = snap.latest_trade.price
        precio_cierre_anterior = snap.previous_daily_bar.close
        volumen_dia = snap.daily_bar.volume
        # Volumen del momento: volumen de la última vela de 1 minuto (no el acumulado del día)
        volumen_momento = snap.minute_bar.volume if snap.minute_bar else 0

        if precio_cierre_anterior <= 0:
            continue
        if not (PRECIO_MIN <= precio_actual <= PRECIO_MAX):
            continue
        if volumen_momento < VOLUMEN_MOMENTO_MINIMO:
            continue
        cambio_porcentaje = ((precio_actual - precio_cierre_anterior) / precio_cierre_anterior) * 100
        if not (GAP_MINIMO_PORCENTAJE <= cambio_porcentaje <= GAP_MAXIMO_PORCENTAJE):
            continue

        preseleccion.append({
            "ticker": ticker,
            "precio": precio_actual,
            "cambio_pct": cambio_porcentaje,
            "volumen_dia": volumen_dia,
            "volumen_momento": volumen_momento,
            "actualizado": snap.latest_trade.timestamp
        })

    if not preseleccion:
        print("   ⏳ Sin candidatos que cumplan precio/gap% todavía.")
        return

    print(f"   [DEBUG] Etapa 1 (precio/gap%): {len(preseleccion)} candidatos: {[c['ticker'] for c in preseleccion]}")

    candidatos_finales = []
    descartados_float_vol = 0
    descartados_tecnico = 0
    for c in preseleccion:
        ticker = c['ticker']

        float_shares, vol_promedio = calcular_datos_fundamentales(ticker)
        if float_shares is None:
            descartados_float_vol += 1
            continue
        if float_shares >= FLOTACION_MAXIMA_ACCIONES:
            descartados_float_vol += 1
            continue
        if not vol_promedio or vol_promedio <= 0:
            descartados_float_vol += 1
            continue

        volumen_relativo = c['volumen_dia'] / vol_promedio
        if volumen_relativo < VOLUMEN_RELATIVO_MINIMO:
            descartados_float_vol += 1
            continue

        cruzando_ema20, macd_cumple = calcular_ema_macd(ticker, DIRECCION_CRUCE, MACD_SIGNO)
        requiere_ema = DIRECCION_CRUCE != "Neutro"
        requiere_macd = MACD_SIGNO != "Neutro"
        cumple_ema = (not requiere_ema) or cruzando_ema20
        cumple_macd = (not requiere_macd) or macd_cumple
        if not (cumple_ema and cumple_macd):
            descartados_tecnico += 1
            continue

        c['float_shares'] = float_shares
        c['volumen_relativo'] = volumen_relativo
        c['tiene_noticia'] = tiene_noticia_reciente(ticker)
        candidatos_finales.append(c)

    # 🔧 DEBUG: cuántos sobrevivieron cada etapa y cuántos se cayeron en cada una
    print(f"   [DEBUG] Etapa 2 (float/volumen relativo): descartados {descartados_float_vol}")
    print(f"   [DEBUG] Etapa 3 (EMA20): descartados {descartados_tecnico}")
    print(f"   [DEBUG] Etapa final: {len(candidatos_finales)} candidatos: {[c['ticker'] for c in candidatos_finales]}")

    if not candidatos_finales:
        print("   ⏳ Ningún candidato cumple todos los filtros técnicos todavía.")
        return

    candidatos_finales = sorted(candidatos_finales, key=lambda x: x['actualizado'], reverse=True)
    top_candidatos = candidatos_finales[:MAX_CANDIDATOS_A_ANALIZAR]

    ULTIMOS_RESULTADOS = top_candidatos
    ULTIMA_ACTUALIZACION = datetime.now()

    tabla_texto = f"{'TICK':<5}|{'PRE':>5}|{'CHG%':>4}|{'VOL':>5}|{'FLT':>5}\n"
    tabla_texto += "-" * 28 + "\n"
    for c in top_candidatos:
        ticker_mostrado = f"🔥{c['ticker']}" if c['tiene_noticia'] else c['ticker']
        vol_formateado = formatear_numero_grande(c['volumen_momento'])
        flt_formateado = formatear_numero_grande(c['float_shares'])
        chg_texto = f"{c['cambio_pct']:.0f}%"
        tabla_texto += f"{ticker_mostrado:<5}|{c['precio']:>5.2f}|{chg_texto:>4}|{vol_formateado:>5}|{flt_formateado:>5}\n"

    print("   📊 ¡Candidatos encontrados! Actualizando canales...")
    enviar_radar_a_telegram(tabla_texto)
    actualizar_cuadro_flotante_html(tabla_texto)

def bucle_control_scanner():
    while True:
        if BOT_ENCENDIDO:
            try:
                ejecutar_ciclo_escaneo()
            except Exception as e:
                print(f"⚠️ Error en escaneo: {e}")
        time.sleep(INTERVALO_ESCANEO_SEGUNDOS)

if "hilo_iniciado" not in st.session_state:
    st.session_state.hilo_iniciado = True
    hilo_servicio = Thread(target=bucle_control_scanner, daemon=True)
    hilo_servicio.start()

# ==========================================
# 🟢🔴 BOTÓN DE ENCENDIDO/APAGADO
# ==========================================
col_estado, col_bot = st.columns([3, 1])
with col_estado:
    if st.session_state.bot_on:
        st.markdown("### 🟢 Scanner ENCENDIDO")
    else:
        st.markdown("### 🔴 Scanner APAGADO")
with col_bot:
    st.session_state.bot_on = st.toggle("Encender / Apagar", value=st.session_state.bot_on, key="toggle_bot_encendido")

BOT_ENCENDIDO = st.session_state.bot_on

# ==========================================
# 🖥 TABLA DE RESULTADOS (estilo Finviz oscuro)
# ==========================================
col_toggle, col_manual, col_info = st.columns([1.3, 1.3, 3])
with col_toggle:
    auto_on = st.toggle("Auto-refresh", value=True, key="auto_refresh_toggle")
with col_manual:
    if st.button("🔄 Refrescar ahora"):
        st.rerun()
with col_info:
    st.markdown(f"#1 / {len(ULTIMOS_RESULTADOS)} Total · Refresco cada {INTERVALO_REFRESCO_SEGUNDOS}s")
if auto_on:
    st_autorefresh(interval=INTERVALO_REFRESCO_SEGUNDOS * 1000, key="auto_refresh_radar")

if ULTIMA_ACTUALIZACION:
    st.caption(f"Última actualización: {ULTIMA_ACTUALIZACION.strftime('%H:%M:%S')}")
else:
    st.caption("Esperando el primer escaneo con resultados...")

if ULTIMOS_RESULTADOS:
    df = pd.DataFrame([
        {
            "No.": i + 1,
            "Ticker": c["ticker"],
            "Precio": round(c["precio"], 2),
            "Cambio %": round(c["cambio_pct"], 1),
            "Volumen": formatear_numero_grande(c["volumen_momento"]),
            "Flotación": formatear_numero_grande(c.get("float_shares")),
            "Vol. Relativo": round(c.get("volumen_relativo", 0), 2),
            "Noticia": "🔥" if c.get("tiene_noticia") else "",
            "Actualizado": c["actualizado"].strftime("%H:%M:%S") if hasattr(c["actualizado"], "strftime") else c["actualizado"],
        }
        for i, c in enumerate(ULTIMOS_RESULTADOS)
    ])

    fig = px.bar(
        df.sort_values("Cambio %", ascending=False),
        x="Ticker",
        y="Cambio %",
        color="Cambio %",
        color_continuous_scale=["#e74c3c", "#2ecc71"],
        title=f"Top {TOP_N} · Cambio % pre-market",
        text="Cambio %",
    )
    fig.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
    fig.update_layout(
        paper_bgcolor="#0a0e1a",
        plot_bgcolor="#0a0e1a",
        font_color="#e6e6e6",
        title_font_color="#c9a227",
        coloraxis_showscale=False,
        margin=dict(t=50, b=10, l=10, r=10),
    )
    st.plotly_chart(fig, use_container_width=True)

    filas_html_principal = ""
    for i, c in enumerate(ULTIMOS_RESULTADOS):
        color_cambio_val = "#2ecc71" if c["cambio_pct"] >= 0 else "#e74c3c"
        hora_act = c["actualizado"].strftime("%H:%M:%S") if hasattr(c["actualizado"], "strftime") else c["actualizado"]
        filas_html_principal += f"""
        <tr>
            <td>{i + 1}</td>
            <td style="font-weight:700;">{"🔥" if c.get('tiene_noticia') else ""}{c['ticker']}</td>
            <td>${c['precio']:.2f}</td>
            <td style="color:{color_cambio_val}; font-weight:700;">{c['cambio_pct']:.1f}%</td>
            <td>{formatear_numero_grande(c['volumen_momento'])}</td>
            <td>{formatear_numero_grande(c.get('float_shares'))}</td>
            <td>{round(c.get('volumen_relativo', 0), 2)}</td>
            <td>{hora_act}</td>
        </tr>
        """
    tabla_html_principal = f"""
    <div class="scanner-grid-wrap">
    <table class="scanner-grid">
        <thead>
            <tr>
                <th>No.</th>
                <th>TICK</th>
                <th>PRE</th>
                <th>CHG%</th>
                <th>VOL</th>
                <th>FLT</th>
                <th>VOL. REL.</th>
                <th>HORA</th>
            </tr>
        </thead>
        <tbody>
            {filas_html_principal}
        </tbody>
    </table>
    </div>
    """
    st.markdown(tabla_html_principal, unsafe_allow_html=True)

# ==========================================
# 🕒 CUADRO DE CANDIDATOS (rejilla completa, sin encabezado de texto)
# ==========================================
st.markdown("""
<style>
    .scanner-grid-wrap {
        border: 1px solid #2a3348;
        border-radius: 10px;
        overflow: hidden;
        box-shadow: 0 2px 12px rgba(0,0,0,0.35);
        margin-top: 8px;
    }
    .scanner-grid {
        width: 100%;
        border-collapse: collapse;
        background-color: #11151f;
    }
    .scanner-grid th {
        background-color: #0a0e1a;
        color: #FFD700 !important;
        font-weight: 700;
        text-transform: uppercase;
        font-size: 12px;
        letter-spacing: 1px;
        padding: 10px 12px;
        border-right: 1px solid #2a3348;
        border-bottom: 2px solid #c9a227;
        text-align: center;
    }
    .scanner-grid th:last-child { border-right: none; }
    .scanner-grid td {
        padding: 9px 12px;
        border-right: 1px solid #232838;
        border-bottom: 1px solid #232838;
        text-align: center;
        color: #e6e6e6;
        font-family: 'Courier New', monospace;
        font-size: 13px;
    }
    .scanner-grid td:last-child { border-right: none; }
    .scanner-grid tr:nth-child(even) td { background-color: #151a26; }
    .scanner-grid tr:last-child td { border-bottom: none; }
    .cuadrito-caratula {
        background: linear-gradient(135deg, #11151f 0%, #171d2c 100%);
        border: 1px solid #2a3348;
        border-bottom: none;
        border-radius: 10px 10px 0 0;
        padding: 10px 16px;
        margin-top: 8px;
        color: #FFD700;
        font-weight: 700;
        font-size: 13px;
        letter-spacing: 0.5px;
        font-family: 'Courier New', monospace;
    }
    .cuadrito-caratula .cuadrito-resumen {
        color: #e6e6e6;
        font-weight: 400;
        margin-left: 6px;
    }
    .cuadrito-titulo {
        color: #FFD700;
        font-weight: 800;
        font-size: 15px;
        letter-spacing: 1px;
        text-transform: uppercase;
        margin: 18px 0 0 0;
        font-family: Arial, sans-serif;
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="cuadrito-titulo">Activos Encontrados</div>', unsafe_allow_html=True)

if ULTIMOS_RESULTADOS:
    recientes = sorted(ULTIMOS_RESULTADOS, key=lambda x: x['actualizado'], reverse=True)

    resumen_partes = [
        f"{'🔥' if c.get('tiene_noticia') else ''}{c['ticker']} {c['cambio_pct']:+.1f}%"
        for c in recientes
    ]
    resumen_texto = "  ·  ".join(resumen_partes)
    plural = "S" if len(recientes) != 1 else ""
    caratula_html = f"""
    <div class="cuadrito-caratula">
        ⚡️ {len(recientes)} CANDIDATO{plural} DETECTADO{plural}
        <span class="cuadrito-resumen">{resumen_texto}</span>
    </div>
    """
    st.markdown(caratula_html, unsafe_allow_html=True)

    filas_html = ""
    for c in recientes:
        color_cambio_val = "#2ecc71" if c["cambio_pct"] >= 0 else "#e74c3c"
        filas_html += f"""
        <tr>
            <td style="font-weight:700;">{"🔥" if c.get('tiene_noticia') else ""}{c['ticker']}</td>
            <td>${c['precio']:.2f}</td>
            <td style="color:{color_cambio_val}; font-weight:700;">{c['cambio_pct']:.1f}%</td>
            <td>{formatear_numero_grande(c['volumen_momento'])}</td>
            <td>{formatear_numero_grande(c.get('float_shares'))}</td>
        </tr>
        """
    radio_superior = "0 0 10px 10px"
else:
    filas_html = ""
    radio_superior = "10px"

tabla_html = f"""
<div class="scanner-grid-wrap" style="margin-top:{'0' if ULTIMOS_RESULTADOS else '8px'}; border-radius:{radio_superior};">
<table class="scanner-grid">
    <thead>
        <tr>
            <th>TICK</th>
            <th>PRE</th>
            <th>CHG%</th>
            <th>VOL</th>
            <th>FLT</th>
        </tr>
    </thead>
    <tbody>
        {filas_html}
    </tbody>
</table>
</div>
"""
st.markdown(tabla_html, unsafe_allow_html=True)
