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

import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components

# Configuración obligatoria de Streamlit Shell
st.set_page_config(page_title="Scanner Pre Market", layout="wide")

# ==========================================
# 🙈 BLINDAJE VISUAL INTERFAZ OSCURA
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
        padding-top: 1rem !important;
    }
    [data-testid="stIFrame"], [data-testid="stIFrame"] > iframe {
        width: 100% !important;
        max-width: 100% !important;
    }
    div.stButton > button {
        background-color: #d4af37 !important;
        color: #000000 !important;
        font-weight: 900 !important;
        border-radius: 8px !important;
        height: 50px !important;
        font-size: 16px !important;
    }
</style>
""", unsafe_allow_html=True)

ET = ZoneInfo("America/New_York")
INTERVALO_ESCANEO_SEGUNDOS = 10
OPCIONES_CRUCE_EMA = ["Vela nueva sobre EMA20 + HH/HL", "Hacia abajo", "Neutro"]
OPCIONES_MACD = ["Positivo", "Negativo", "No exigir"]

VALORES_POR_DEFECTO = {
    "precio_min": 0.5, "precio_max": 20.0,
    "gap_min": 3.0, "gap_max": 50.0,
    "flotacion_max": 20_000_000, "volumen_min": 15_000,
    "cruce_ema": "Vela nueva sobre EMA20 + HH/HL", "macd": "Positivo",
    "orden": "Actualizado", "top_n": 50, "sesion": "PRE-MARKET", "timeframe": "1m"
}

# ==========================================
# 🔐 GESTIÓN DE ACCESOS Y REDIRECCIONES
# ==========================================
if "mostrar_auth" not in st.session_state:
    st.session_state["mostrar_auth"] = False

if str(st.query_params.get("logout", "0")).lower() in ("1", "true", "yes"):
    st.session_state.clear()
    st.query_params.clear()
    st.rerun()

if str(st.query_params.get("auth", "0")).lower() in ("1", "true", "yes"):
    st.session_state["mostrar_auth"] = True
    st.query_params.pop("auth", None)
    st.rerun()

PUBLIC_PREVIEW = "token_verificado" not in st.session_state and "usuario_auth" not in st.session_state

# ==========================================
# 🎨 INTERFAZ NATIVA DE AUTENTICACIÓN
# ==========================================
def pantalla_autenticacion():
    st.markdown('<h2 style="color:#d4af37; text-align:center;">TRADE SCANNER</h2>', unsafe_allow_html=True)
    tab_l, tab_r = st.tabs(["🔐 Login", "📝 Registro"])
    
    with tab_l:
        with st.form("form_l"):
            em = st.text_input("Correo")
            pw = st.text_input("Contraseña", type="password")
            btn = st.form_submit_button("INGRESAR AL SCANNER")
        if btn:
            # Validación simulada rápida para desarrollo local/móvil
            st.session_state["usuario_auth"] = {"email": em}
            st.session_state["mostrar_auth"] = False
            st.rerun()

    with tab_r:
        with st.form("form_r"):
            st.text_input("Nuevo Correo")
            st.text_input("Contraseña (Min 8)", type="password")
            st.form_submit_button("CREAR CUENTA")

# CONTROL DE DETENCIÓN DE FLUJO SI SE SOLICITÓ AUTENTICACIÓN
if st.session_state["mostrar_auth"] and PUBLIC_PREVIEW:
    pantalla_autenticacion()
    st.stop()

# ==========================================
# 🚨 BOTÓN DE ACCESO SUPERIOR (VISIBILIDAD MÓVIL CRÍTICA)
# ==========================================
if PUBLIC_PREVIEW and not st.session_state["mostrar_auth"]:
    st.warning("⚠️ Vista de Explorador Activa. Para ver las señales en vivo debes iniciar sesión.")
    if st.button("🚀 INICIAR SESIÓN / REGISTRARSE", key="m_auth_btn", width="stretch"):
        st.session_state["mostrar_auth"] = True
        st.rerun()
    st.markdown("---")

# ==========================================
# CONTROLES Y RENDERIZADO DE TABLA FINVIZ
# ==========================================
with st.sidebar:
    st.markdown("### ⚙️ Parámetros")
    precio_min_ui = st.number_input("Precio Mínimo ($)", value=0.5)
    precio_max_ui = st.number_input("Precio Máximo ($)", value=20.0)
    gap_min_ui = st.number_input("Gap Mínimo (%)", value=3.0)
    gap_max_ui = st.number_input("Gap Máximo (%)", value=50.0)
    float_max_ui = st.number_input("Flotación Máxima", value=20000000)
    volumen_min_ui = st.number_input("Volumen Mínimo", value=15000)

filas_reales = [
    {"ticker": "AAPL", "sector": "Technology", "precio": 174.85, "cambio_pct": 3.42, "volumen_dia": 45000000, "gap_pct": 3.12, "float_shares": 15000000},
    {"ticker": "TSLA", "sector": "Consumer Cyclical", "precio": 218.30, "cambio_pct": 5.15, "volumen_dia": 68000000, "gap_pct": 4.85, "float_shares": 9000000},
    {"ticker": "NVDA", "sector": "Technology", "precio": 462.10, "cambio_pct": 7.89, "volumen_dia": 38000000, "gap_pct": 6.20, "float_shares": 12000000}
]

def _big(v):
    if v >= 1_000_000: return f"{v/1_000_000:.1f}M"
    return f"{v:.0f}"

rows_html = ""
for row in filas_reales:
    tk, sc, px, ch, vl = row["ticker"], row["sector"], row["precio"], row["cambio_pct"], row["volumen_dia"]
    rows_html += f"<tr><td>⚙️ Layout</td><td><b>{tk}</b></td><td>{sc}</td><td>${px:.2f}</td><td style='color:#37c77a;'>{ch:+.2f}%</td><td>{_big(vl)}</td><td>{row['gap_pct']}%</td><td>15M</td><td>Por Encima</td><td>Neutro</td><td>Neutro</td><td>Positivo</td></tr>"

# MAQUETACIÓN HTML COMPACTA
h = f"""
<!DOCTYPE html><html><head><meta charset='UTF-8'><meta name='viewport' content='width=device-width, initial-scale=1.0'>
<style>
    body {{ background:#15181d; font-family:sans-serif; color:#fff; padding:10px; margin:0; }}
    .topbar {{ background:#20242a; padding:10px; display:flex; justify-content:space-between; align-items:center; margin-bottom:8px; border:1px solid #444; }}
    .table-wrapper {{ width:100%; overflow-x:auto; background:#171a1f; border:1px solid #444; }}
    table {{ width:100%; min-width:800px; border-collapse:collapse; }}
    th {{ background:#2d333b; padding:8px; border:1px solid #555; text-align:left; font-size:11px; }}
    td {{ padding:8px; border:1px solid #333; font-size:12px; }}
</style></head><body>
<div class='topbar'><div style='font-weight:900;'>TRADESCANNER</div><div style='color:#37c77a;'>🟢 MODO SIMULACIÓN</div></div>
<div class='table-wrapper'><table><thead><tr><th>Layout</th><th>Ticker</th><th>Sector</th><th>Precio</th><th>Cambio</th><th>Volumen</th><th>Gap</th><th>Float</th><th>EMA20</th><th>EMA50</th><th>EMA200</th><th>MACD</th></tr></thead><tbody>{rows_html}</tbody></table></div>
</body></html>
"""

components.html(h, height=800, scrolling=True)
