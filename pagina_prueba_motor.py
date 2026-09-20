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
"""

import streamlit as st
from threading import Thread
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

if "motor_prueba" not in st.session_state:
    st.session_state.motor_prueba = MotorVelas(ALPACA_API_KEY, ALPACA_SECRET_KEY)
    st.session_state.hilo_motor_prueba = None
    st.session_state.ticker_conectado = None

motor = st.session_state.motor_prueba

col_input, col_boton = st.columns([3, 1])
with col_input:
    ticker = st.text_input("Ticker a observar", value="AAPL").strip().upper()
with col_boton:
    st.write("")
    st.write("")
    conectar = st.button("🔌 Conectar", use_container_width=True)

if conectar and ticker:
    if st.session_state.hilo_motor_prueba is None:
        def _arrancar():
            motor.iniciar([ticker])

        hilo = Thread(target=_arrancar, daemon=True)
        hilo.start()
        st.session_state.hilo_motor_prueba = hilo
        st.session_state.ticker_conectado = ticker
        st.success(f"Conectando a {ticker}... espera unos segundos y refresca.")
    else:
        motor.agregar_simbolo_en_caliente(ticker)
        st.session_state.ticker_conectado = ticker

st.divider()

if st.session_state.ticker_conectado:
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
