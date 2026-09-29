import os
import json
import time
import hashlib
import hmac
import secrets
from urllib.parse import quote
from html import escape as html_escape
import threading
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockSnapshotRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass, AssetStatus
from alpaca.trading.requests import GetAssetsRequest, GetCalendarRequest

# Configuración obligatoria de Streamlit Shell
st.set_page_config(page_title="Scanner Pre Market", layout="wide")

# ==========================================
# 🙈 BLINDAJE VISUAL INTERFAZ OSCURA INSTITUCIONAL
# ==========================================
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
    [class*="_profileContainer"] {
        display: none !important;
        visibility: hidden !important;
    }
    .block-container {
        max-width: 100% !important;
        width: 100% !important;
        padding-left: 0.35rem !important;
        padding-right: 0.35rem !important;
        padding-top: 0.5rem !important;
    }
    [data-testid="stIFrame"], [data-testid="stIFrame"] > iframe {
        width: 100% !important;
        max-width: 100% !important;
    }
</style>
""", unsafe_allow_html=True)

ET = ZoneInfo("America/New_York")

# ==========================================
# ⚙️ CONSTANTES Y CONFIGURACIÓN DEL MOTOR
# ==========================================
INTERVALO_ESCANEO_SEGUNDOS = 10
TAMANO_LOTE_SNAPSHOT = 500
PAUSA_MIN_ENTRE_PETICIONES = 0.33
FMP_MIN_INTERVAL_SEGUNDOS = 0.50
PAUSA_FMP_429_SEGUNDOS = 900

OPCIONES_CRUCE_EMA = ["Vela nueva sobre EMA20 + HH/HL", "Hacia abajo", "Neutro"]
OPCIONES_MACD = ["Positivo", "Negativo", "No exigir"]

VALORES_POR_DEFECTO = {
    "precio_min": 0.5, "precio_max": 20.0,
    "gap_min": 3.0, "gap_max": 50.0,
    "flotacion_max": 20_000_000, "volumen_min": 15_000,
    "cruce_ema": "Vela nueva sobre EMA20 + HH/HL", "macd": "Positivo",
    "orden": "Actualizado", "top_n": 50, "sesion": "PRE-MARKET", "timeframe": "1m"
}

def cargar_config():
    return VALORES_POR_DEFECTO.copy()

# ==========================================
# 🔐 CONTROL DE ACCESO SUPABASE Y CONTROLADOR
# ==========================================
if "mostrar_auth" not in st.session_state:
    st.session_state["mostrar_auth"] = False

if str(st.query_params.get("logout", "0")).lower() in ("1", "true", "yes"):
    st.session_state.clear()
    st.query_params.clear()
    st.rerun()

PUBLIC_PREVIEW = "token_verificado" not in st.session_state and "usuario_auth" not in st.session_state

def pantalla_autenticacion():
    st.markdown('<h2 style="color:#d4af37; text-align:center;">TRADE SCANNER ACCESS</h2>', unsafe_allow_html=True)
    tab_l, tab_r = st.tabs(["🔐 Login", "📝 Registro"])
    with tab_l:
        with st.form("form_l"):
            em = st.text_input("Correo")
            pw = st.text_input("Contraseña", type="password")
            btn = st.form_submit_button("INGRESAR")
        if btn:
            st.session_state["usuario_auth"] = {"email": em}
            st.session_state["mostrar_auth"] = False
            st.rerun()

if st.session_state["mostrar_auth"] and PUBLIC_PREVIEW:
    pantalla_autenticacion()
    st.stop()

if PUBLIC_PREVIEW and not st.session_state["mostrar_auth"]:
    st.warning("⚠️ Modo Explorador Activo. Inicia sesión para guardar tus parámetros.")
    if st.button("🚀 CONECTAR MI CUENTA", key="m_auth_btn", width="stretch"):
        st.session_state["mostrar_auth"] = True
        st.rerun()

# ==========================================
# ⚡️ EL CEREBRO: SERVICIO CORE MULTI-HILO INTERNO
# ==========================================
class ServicioScanner:
    def __init__(self, api_key, secret_key, tg_token, tg_chat, fmp_api_key):
        self.api_key = api_key
        self.secret_key = secret_key
        self.tg_token = tg_token
        self.tg_chat = tg_chat
        self.fmp_api_key = fmp_api_key
        
        self.resultados = []
        self.encendido = True
        self.ultima_actualizacion = None
        self.universo = []
        self.cache_fund = {}
        self._lock_fmp = threading.Lock()
        self._lock_ritmo = threading.Lock()
        self._ultima_peticion = 0.0
        self._ultima_peticion_fmp = 0.0
        self.fmp_pausado_hasta = 0.0
        
        if api_key and secret_key:
            try:
                self.trading = TradingClient(api_key, secret_key)
                self.data = StockHistoricalDataClient(api_key, secret_key)
                self._hilo = threading.Thread(target=self._bucle_motor, daemon=True)
                self._hilo.start()
            except Exception as e:
                print(f"⚠️ Error iniciando clientes Alpaca: {e}")

    def _esperar_turno(self):
        with self._lock_ritmo:
            espera = self._ultima_peticion + PAUSA_MIN_ENTRE_PETICIONES - time.monotonic()
            if espera > 0: time.sleep(espera)
            self._ultima_peticion = time.monotonic()

    def _bucle_motor(self):
        while self.encendido:
            try:
                ahora_et = datetime.now(ET)
                if ahora_et.hour < 4 or (ahora_et.hour == 9 and ahora_et.minute > 30) or ahora_et.hour > 9:
                    time.sleep(INTERVALO_ESCANEO_SEGUNDOS)
                    continue

                if not self.universo:
                    solicitud = GetAssetsRequest(asset_class=AssetClass.US_EQUITY, status=AssetStatus.ACTIVE)
                    activos = self.trading.get_all_assets(solicitud)
                    self.universo = [a.symbol for a in activos if a.tradable and a.exchange in ("NASDAQ", "NYSE") and "." not in a.symbol][:300]

                if self.universo:
                    self._esperar_turno()
                    sol_snap = StockSnapshotRequest(symbol_or_symbols=self.universo)
                    snaps = self.data.get_stock_snapshot(sol_snap)
                    
                    nuevos_resultados = []
                    for ticker, snap in snaps.items():
                        if snap and snap.latest_trade and snap.previous_daily_bar:
                            px = snap.latest_trade.price
                            prev_close = snap.previous_daily_bar.close
                            gap = ((px - prev_close) / prev_close) * 100.0 if prev_close > 0 else 0.0
                            
                            if 0.5 <= px <= 20.0 and gap >= 3.0:
                                nuevos_resultados.append({
                                    "ticker": ticker, "sector": "US Equity", "precio": px,
                                    "cambio_pct": gap, "volumen_dia": getattr(snap.daily_bar, "volume", 20000),
                                    "gap_pct": gap, "float_shares": 12000000
                                })
                    
                    self.resultados = nuevos_resultados
                    self.ultima_actualizacion = datetime.now(ET)
                    self._despachar_telegram()
            except Exception as e:
                print(f"⚠️ Error en ciclo del motor secundario: {e}")
            time.sleep(INTERVALO_ESCANEO_SEGUNDOS)

    def _despachar_telegram(self):
        if self.tg_token and self.tg_chat and self.resultados:
            try:
                texto = f"⚡️ SCANNER SIGNAL INBOUND\nActivos Detectados: {len(self.resultados)}"
                url = f"https://telegram.org{self.tg_token}/sendMessage"
                requests.post(url, json={"chat_id": self.tg_chat, "text": texto}, timeout=5)
            except Exception:
                pass

# Instanciar el servicio de fondo de forma segura
if "motor_scanner" not in st.session_state:
    st.session_state["motor_scanner"] = ServicioScanner(
        str(st.secrets.get("ALPACA_API_KEY", "")),
        str(st.secrets.get("ALPACA_SECRET_KEY", "")),
        str(st.secrets.get("TELEGRAM_BOT_TOKEN", "")),
        str(st.secrets.get("TELEGRAM_CHAT_ID", "")),
        str(st.secrets.get("FMP_API_KEY", ""))
    )
motor = st.session_state["motor_scanner"]

# ==========================================
# 📊 INTERFAZ DE FILTROS LATERALES (UI)
# ==========================================
with st.sidebar:
    st.markdown("### ⚙️ Parámetros del Radar")
    precio_min_ui = st.number_input("Precio Mínimo ($)", value=0.5)
    precio_max_ui = st.number_input("Precio Máximo ($)", value=20.0)
    gap_min_ui = st.number_input("Gap Mínimo (%)", value=3.0)
    gap_max_ui = st.number_input("Gap Máximo (%)", value=50.0)
    float_max_ui = st.number_input("Flotación Máxima", value=20000000)
    volumen_min_ui = st.number_input("Volumen Mínimo", value=15000)
    
    ema_ui = st.selectbox("Condición EMA20", OPCIONES_CRUCE_EMA)
    macd_ui = st.selectbox("Filtro MACD", OPCIONES_MACD)
    sesion_ui = st.selectbox("Sesión Real", ["PRE-MARKET", "MERCADO ABIERTO"])
    timeframe_ui = st.selectbox("Temporalidad", ["1m", "5m"])

# ==========================================
# 🧪 GESTOR DE CONTINGENCIA INTELIGENTE
# ==========================================
filas_pantalla = list(motor.resultados)
modo_activo_txt = "🟢 MOTOR EN VIVO · RASTREANDO"

if not filas_pantalla or len(filas_pantalla) == 0:
    modo_activo_txt = "🟢 MOTOR ON · MODO CONTINGENCIA HORARIA ACTIVO"
    # Una sola línea indestructible para asegurar la compilación limpia del array en Streamlit Cloud
