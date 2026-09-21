"""
pagina_prueba_motor.py
========================
Pantalla de PRUEBA VISUAL del motor de velas. No compra ni vende nada —
solo conecta al websocket de Alpaca (feed IEX, tiempo real) para un ticker
que tú elijas, y muestra en vivo cómo se va armando la vela, en qué tramo
de 20 segundos va, y si detecta libélula/lápida.

Cómo usarla:
1. Súbela a tu repo junto con motor_velas.py (deben quedar en la misma carpeta)
2. En Streamlit Cloud, al desplegar, apunta el "Main file path" a este archivo
   (o crea una segunda app de prueba separada de tu scanner principal)
3. Escribe un ticker (ej: AAPL, TSLA) y dale "Conectar"
4. En horario de mercado (o pre-market), deberías ver los números moverse
   solos cada pocos segundos

IMPORTANTE: el plan gratuito de Alpaca permite UNA sola conexión websocket
por cuenta. Esta página usa un único motor compartido por todas las pestañas
y sesiones, pero si otra app (por ejemplo tu scanner principal) usa las mismas
llaves al mismo tiempo, una de las dos será rechazada.
"""

import streamlit as st
from threading import Thread
from datetime import datetime, timezone
import time
from motor_velas import MotorVelas

st.set_page_config(page_title="Prueba - Motor de Velas", layout="wide")

st.title("🔬 Prueba del Motor de Velas (sin compra/venta)")
st.caption(
    "Esta pantalla solo OBSERVA el mercado en tiempo real. No ejecuta "
    "ninguna orden. Sirve para confirmar que el motor arma las velas y "
    "detecta los tramos de 20 segundos correctamente antes de conectarle "
    "la lógica de compra/venta."
)

ALPACA_API_KEY = st.secrets["ALPACA_API_KEY"]
ALPACA_SECRET_KEY = st.secrets["ALPACA_SECRET_KEY"]


# ==========================================
# 🔒 MOTOR ÚNICO Y COMPARTIDO (una sola conexión a Alpaca)
# ==========================================
# st.cache_resource crea el objeto UNA vez por proceso de la app y lo reutiliza
# en todas las re-ejecuciones, pestañas y sesiones. Así nunca se abren
# conexiones duplicadas al websocket.
@st.cache_resource
def obtener_motor():
    return MotorVelas(ALPACA_API_KEY, ALPACA_SECRET_KEY)


@st.cache_resource
def obtener_estado_conexion():
    return {"hilo": None}


motor = obtener_motor()
estado = obtener_estado_conexion()

if "ticker_conectado" not in st.session_state:
    st.session_state.ticker_conectado = None


def hilo_vivo() -> bool:
    hilo = estado["hilo"]
    return hilo is not None and hilo.is_alive()


# ==========================================
# 🎛️ CONTROLES
# ==========================================
col_input, col_boton = st.columns([3, 1])
with col_input:
    ticker = st.text_input("Ticker a observar", value="AAPL").strip().upper()
with col_boton:
    st.write("")
    st.write("")
    conectar = st.button("🔌 Conectar")

if conectar and ticker:
    if not hilo_vivo():
        # No hay conexión activa (primera vez, o el hilo anterior murió por
        # un error): arrancar una nueva.
        def _arrancar(simbolo=ticker):
            motor.iniciar([simbolo])

        hilo = Thread(target=_arrancar, daemon=True)
        hilo.start()
        estado["hilo"] = hilo
        st.success(f"Conectando a {ticker}... espera unos segundos.")
    else:
        # Ya hay una conexión viva: solo agregar el símbolo.
        motor.agregar_simbolo_en_caliente(ticker)
    st.session_state.ticker_conectado = ticker

st.divider()

# ==========================================
# 📡 DIAGNÓSTICO DE LA CONEXIÓN
# ==========================================
if st.session_state.ticker_conectado:
    vivo = hilo_vivo()

    d1, d2, d3 = st.columns(3)
    d1.metric("Conexión", "🟢 Activa" if vivo else "🔴 Caída")
    d2.metric("Trades recibidos", motor.total_trades)
    if motor.ultimo_trade is not None:
        segundos = max(0, int((datetime.now(timezone.utc) - motor.ultimo_trade).total_seconds()))
        d3.metric("Último trade", f"hace {segundos} s")
    else:
        d3.metric("Último trade", "—")

    if not vivo:
        st.warning(
            "La conexión con Alpaca no está activa. Pulsa 'Conectar' para reintentar. "
            "Si vuelve a caerse, revisa los logs (Manage app): puede ser un límite de "
            "conexiones de Alpaca (otra app usando las mismas llaves) o llaves inválidas."
        )

    # ==========================================
    # 🕯️ ESTADO DE LA VELA
    # ==========================================
    snap = motor.snapshot_simbolo(st.session_state.ticker_conectado)

    if snap.get("sin_datos"):
        st.info(
            f"Esperando la primera operación de {st.session_state.ticker_conectado}... "
            "si el mercado está cerrado, esto puede tardar."
        )
    else:
        va = snap["vela_actual"]
        vp = snap["vela_anterior"]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Tramo actual", f"{snap['tramo_actual']} de 3")
        c2.metric("Apertura", va["apertura"])
        c3.metric("Máximo / Mínimo", f"{va['maximo']} / {va['minimo']}")
        c4.metric("Cierre (en vivo)", va["cierre"])

        c5, c6, c7 = st.columns(3)
        c5.metric("¿Libélula en curso?", "SÍ" if va["es_libelula_en_curso"] else "No")
        c6.metric("¿Lápida en curso?", "SÍ" if va["es_lapida_en_curso"] else "No")
        c7.metric("¿Positiva?", "SÍ" if va["es_positiva"] else "No")

        st.subheader("Comparación con la vela anterior")
        if vp:
            cc1, cc2 = st.columns(2)
            with cc1:
                st.write("**Vela anterior (cerrada):**")
                st.json(vp)
            with cc2:
                st.write(f"¿Mínimo actual supera al anterior? **{snap['minimo_supera_anterior']}**")
                st.write(f"¿Máximo actual supera al anterior? **{snap['maximo_supera_anterior']}**")
        else:
            st.caption("Todavía no hay una vela anterior cerrada (es la primera del historial).")

        st.subheader("Indicadores")
        i1, i2, i3, i4 = st.columns(4)
        i1.metric("EMA9", round(snap["ema9"], 4) if snap["ema9"] else "—")
        i2.metric("EMA20", round(snap["ema20"], 4) if snap["ema20"] else "—")
        i3.metric("EMA50", round(snap["ema50"], 4) if snap["ema50"] else "—")
        i4.metric("EMA200", round(snap["ema200"], 4) if snap["ema200"] else "—")

        i5, i6, i7 = st.columns(3)
        i5.metric("MACD", round(snap["macd"], 5) if snap["macd"] else "—")
        i6.metric("Banda Bollinger sup.", round(snap["banda_bollinger_superior"], 4) if snap["banda_bollinger_superior"] else "—")
        i7.metric("Banda Bollinger inf.", round(snap["banda_bollinger_inferior"], 4) if snap["banda_bollinger_inferior"] else "—")

        st.caption(f"Velas cerradas en historial: {snap['num_velas_historial']}")

    time.sleep(2)
    st.rerun()
else:
    st.info("Escribe un ticker y dale 'Conectar' para empezar a ver datos en vivo.")
