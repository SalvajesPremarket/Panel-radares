import os
import json
import time
import hashlib
from urllib.parse import quote
from html import escape as html_escape
import threading
from datetime import date, datetime, timedelta, timezone, time as dt_time
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
import streamlit as st
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockSnapshotRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass, AssetStatus
from alpaca.trading.requests import GetAssetsRequest, GetCalendarRequest

st.set_page_config(page_title="Scanner Pre Market", layout="wide")

# ==========================================
# 🙈 OCULTAR BARRA SUPERIOR DE STREAMLIT (Share, GitHub, editar, menú, badges)
# 📱 + AJUSTES RESPONSIVOS para que se vea bien en celular
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

    /* --- Responsivo: pantallas de celular (ancho <= 640px) --- */
    @media (max-width: 640px) {
        .block-container {
            padding-left: 0.6rem !important;
            padding-right: 0.6rem !important;
            padding-top: 1rem !important;
        }
        h1 { font-size: 1.3rem !important; }
        h2 { font-size: 1.1rem !important; }
        h3 { font-size: 1rem !important; }
        [data-testid="stMetricValue"] { font-size: 1.1rem !important; }
        /* la tabla de resultados no se recorta: permite scroll horizontal */
        [data-testid="stDataFrame"] { overflow-x: auto !important; }
    }
</style>
""", unsafe_allow_html=True)


ET = ZoneInfo("America/New_York")

# ==========================================
# ⚙️ PARÁMETROS DEL MOTOR
# ==========================================
INTERVALO_ESCANEO_SEGUNDOS = 10        # cada cuánto el motor recorre el mercado (una sola vez para todos los usuarios)
TAMANO_LOTE_SNAPSHOT = 500             # tickers por petición de snapshot
WORKERS_SNAPSHOT = 4                   # peticiones de snapshot en paralelo
PAUSA_MIN_ENTRE_PETICIONES = 0.33      # ~180 peticiones/min a Alpaca (límite: 200/min)

# Radar base: rango AMPLIO que el motor enriquece. Cada usuario filtra su vista dentro de este rango.
BASE_PRECIO_MIN = 0.5
BASE_PRECIO_MAX = 20.0
BASE_GAP_MIN = 3.0
BASE_GAP_MAX = 50.0
BASE_FLOTACION_MAX = 20_000_000

# 🧪 ETAPA DE DEPURACIÓN DE FILTROS
# 1 = solo precio + EMA20 + MACD. Telegram queda APAGADO.
# Luego podremos pasar a 2, 3, 4... agregando un filtro por vez.
ETAPA_PRUEBA_FILTROS = 4

# PRUEBA 7: medir alcanzabilidad de objetivos sobre la misma señal.
PRUEBA7_OBJETIVOS_PCT = (0.25, 0.50, 1.00)

MAX_ENRIQUECER = 500                   # PRUEBA 3: ampliar temporalmente la muestra técnica; no es un filtro de trading

# Float: FMP es la fuente principal; volumen y velas técnicas se obtienen con Alpaca.
FMP_API_URL = "https://financialmodelingprep.com/stable/shares-float"
MAX_FUNDAMENTALES_POR_CICLO = 1        # FMP: una consulta de float por ciclo para evitar HTTP 429
WORKERS_FUNDAMENTALES = 1               # FMP no se consulta en paralelo
VIGENCIA_FUNDAMENTALES = 7 * 86400
REINTENTO_FUNDAMENTALES = 300
PAUSA_FMP_429_SEGUNDOS = 900            # tras HTTP 429, pausa FMP durante 15 min
FMP_MIN_INTERVAL_SEGUNDOS = 30          # máximo 2 consultas/minuto para no golpear el límite de FMP

# Horario automático: 04:00–16:00 ET, solo días de mercado según Alpaca.
HORA_AUTO_INICIO_ET = 4
HORA_AUTO_FIN_ET = 16
TTL_CALENDARIO_MERCADO = 12 * 3600

TTL_TECNICO_SEGUNDOS = 30              # no recalcular EMA/MACD de un ticker más seguido que esto
VENTANA_CRUCE_EMA_MINUTOS = 1
MARGEN_PROXIMIDAD_EMA = 0.05
# PRUEBA 6: ventana fija de observación posterior a la detección.
# Es diagnóstico únicamente; no modifica ningún filtro ni resultado.
VENTANA_PRUEBA6_MINUTOS = 10
MINUTOS_NOTICIA_RECIENTE = 60

# --- Cuadro "Eventos en vivo" (parte de abajo de la interfaz) ---
MAX_EVENTOS = 500                      # eventos que guarda el motor en memoria
MAX_HISTORIAL_CICLOS = 10               # ciclos recientes conservados para depuración
EVENTOS_MOSTRAR = 40                   # filas visibles en el cuadro
EVENTOS_ALTO_PX = 430                  # alto del cuadro (con scroll)

# --- Botón encender/apagar del scanner ---
MOSTRAR_BOTON_ENCENDIDO_A_TODOS = True  # True: lo ve cualquier usuario con licencia. False: solo el administrador

# --- Opciones de los filtros técnicos (la primera es la que viene por defecto) ---
OPCIONES_CRUCE_EMA = ["Vela nueva sobre EMA20 + HH/HL", "Hacia abajo", "Neutro"]
OPCIONES_MACD = ["Positivo", "Negativo", "No exigir"]

NOMBRE_ARCHIVO_HTML = "radar.html"
RUTA_CACHE_FUNDAMENTALES = os.path.join(os.getcwd(), "cache_fundamentales.json")
RUTA_CONFIG = os.path.join(os.getcwd(), "config_filtros.json")

VALORES_POR_DEFECTO = {
    "precio_min": 0.5,
    "precio_max": 20.0,
    "gap_min": 3.0,
    "gap_max": 50.0,
    "flotacion_max": 20_000_000,
    "volumen_min": 20_000,
    "intervalo_refresco": 5,
    # Valores técnicos usados por el motor compartido/diagnóstico.
    # Antes faltaban aquí y filtrar_resultados() podía lanzar KeyError
    # con self.filtros_dueno, abortando el ciclo antes de publicar el diagnóstico.
    "cruce_ema": "Hacia arriba",
    "macd": "Positivo",
    "orden": "Actualizado",
    "top_n": 50,
}


def cargar_config():
    """Filtros por defecto (los del dueño). Solo lectura: cada usuario ajusta su propia vista."""
    return VALORES_POR_DEFECTO.copy()


def cargar_horario_guardado():
    """Horario automático guardado en disco (sobrevive a reinicios de la app).
    Si no hay nada guardado, usa los valores por defecto del código."""
    try:
        with open(RUTA_CONFIG, "r", encoding="utf-8") as f:
            d = json.load(f)
        return int(d["hora_inicio_auto_min"]), int(d["hora_fin_auto_min"])
    except Exception:
        return HORA_AUTO_INICIO_ET * 60, HORA_AUTO_FIN_ET * 60


def guardar_horario_en_disco(inicio_min, fin_min):
    """Guarda el horario automático en disco para que sobreviva a reinicios de la app."""
    try:
        with open(RUTA_CONFIG, "w", encoding="utf-8") as f:
            json.dump({"hora_inicio_auto_min": int(inicio_min), "hora_fin_auto_min": int(fin_min)}, f)
    except Exception:
        pass


# ==========================================
# 🔐 AUTENTICACIÓN — ADMIN + USUARIOS
# ==========================================
# ADMIN:
#   - Mantiene el acceso actual mediante ADMIN_TOKEN.
# USUARIOS:
#   - Se registran con email + contraseña.
#   - Inician/cerran sesión desde la propia aplicación.
#   - Sus credenciales son gestionadas por Supabase Auth.
#
# Secrets necesarios para el registro/login de usuarios:
#   SUPABASE_URL
#   SUPABASE_ANON_KEY
#
# El scanner, las API keys y los secrets del servidor NO se entregan
# al usuario desde este módulo.

def obtener_tokens():
    """Tokens/licencias antiguas del sistema. Se conservan para compatibilidad."""
    for clave in ("tokens_autorizados", "TOKENS_AUTORIZADOS"):
        try:
            if clave in st.secrets:
                return dict(st.secrets[clave])
        except Exception:
            pass
    return {}


def verificar_token(token_usuario):
    """Valida el acceso administrativo/legacy por token."""
    admin_token = str(st.secrets.get("ADMIN_TOKEN", "")).strip()
    if admin_token and token_usuario == admin_token:
        return True, "2099-01-01"

    tokens = obtener_tokens()
    if token_usuario in tokens:
        try:
            fecha_exp = datetime.strptime(str(tokens[token_usuario]), "%Y-%m-%d").date()
        except ValueError:
            return False, "FORMATO"
        if datetime.now().date() <= fecha_exp:
            return True, str(tokens[token_usuario])
        return False, "EXPIRADO"
    return False, "INVALIDO"


def _supabase_config():
    """Obtiene la URL y la anon key de Supabase desde Streamlit Secrets."""
    url = str(st.secrets.get("SUPABASE_URL", "")).strip().rstrip("/")
    key = str(st.secrets.get("SUPABASE_ANON_KEY", "")).strip()
    return url, key


def supabase_auth_request(endpoint, payload):
    """
    Llama directamente a Supabase Auth REST API usando requests.
    No requiere instalar el paquete supabase.
    """
    url, key = _supabase_config()
    if not url or not key:
        return None, "Faltan SUPABASE_URL y/o SUPABASE_ANON_KEY en Streamlit Secrets."

    try:
        respuesta = requests.post(
            f"{url}/auth/v1/{endpoint}",
            headers={
                "apikey": key,
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=20,
        )

        try:
            data = respuesta.json()
        except Exception:
            data = {}

        if respuesta.ok:
            return data, None

        mensaje = (
            data.get("msg")
            or data.get("message")
            or data.get("error_description")
            or data.get("error")
            or "No se pudo completar la operación."
        )
        return None, str(mensaje)

    except Exception as e:
        return None, f"Error de conexión con el servicio de autenticación: {e}"


def registrar_usuario(email, password):
    """Crea una cuenta de usuario mediante Supabase Auth."""
    email = str(email).strip().lower()

    if not email or "@" not in email:
        return None, "Introduce un correo electrónico válido."

    if len(password) < 8:
        return None, "La contraseña debe tener al menos 8 caracteres."

    # Supabase debe enviar el enlace de confirmación de vuelta a la
    # aplicación pública del scanner, no a share.streamlit.io.
    redirect_url = "https://jd6gih.streamlit.app"

    data, error = supabase_auth_request(
        f"signup?redirect_to={quote(redirect_url, safe='')}",
        {
            "email": email,
            "password": password,
        },
    )

    if error:
        return None, error

    return data, None


def iniciar_sesion_usuario(email, password):
    """Inicia sesión con email y contraseña mediante Supabase Auth."""
    email = str(email).strip().lower()

    if not email or not password:
        return None, "Introduce tu correo y contraseña."

    data, error = supabase_auth_request(
        "token?grant_type=password",
        {
            "email": email,
            "password": password,
        },
    )

    if error:
        return None, error

    return data, None


def solicitar_recuperacion(email):
    """Solicita un código OTP de recuperación por correo.

    No usamos el enlace de un solo uso de Supabase porque algunos clientes
    de correo/seguridad pueden abrirlo automáticamente y consumirlo antes
    de que el usuario lo pulse.
    """
    email = str(email).strip().lower()

    if not email or "@" not in email:
        return None, "Introduce un correo electrónico válido."

    data, error = supabase_auth_request(
        "recover",
        {"email": email},
    )

    if error:
        return None, error

    return data, None


def verificar_codigo_recuperacion(email, codigo):
    """Verifica el OTP de recuperación y obtiene una sesión temporal."""
    email = str(email).strip().lower()
    codigo = "".join(str(codigo).split())

    if not email or "@" not in email:
        return None, "Introduce un correo electrónico válido."

    # Supabase usa OTP numérico para este flujo. Aceptamos 6-8 dígitos para
    # mantener compatibilidad con las variantes de plantilla documentadas.
    if not codigo.isdigit() or len(codigo) not in (6, 8):
        return None, "El código debe tener 6 u 8 dígitos."

    data, error = supabase_auth_request(
        "verify",
        {
            "email": email,
            "token": codigo,
            "type": "recovery",
        },
    )

    if error:
        return None, error

    if not data or not data.get("access_token"):
        return None, "Supabase no devolvió una sesión válida de recuperación."

    return data, None


def actualizar_password_recuperacion(access_token, nueva_password):
    """Cambia la contraseña usando la sesión temporal obtenida con el OTP."""
    if not access_token:
        return None, "La sesión de recuperación no es válida. Solicita un nuevo código."

    if len(str(nueva_password)) < 8:
        return None, "La contraseña debe tener al menos 8 caracteres."

    url, key = _supabase_config()
    if not url or not key:
        return None, "Faltan SUPABASE_URL y/o SUPABASE_ANON_KEY en Streamlit Secrets."

    try:
        respuesta = requests.put(
            f"{url}/auth/v1/user",
            headers={
                "apikey": key,
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json={"password": str(nueva_password)},
            timeout=20,
        )

        try:
            data = respuesta.json()
        except Exception:
            data = {}

        if respuesta.ok:
            return data, None

        mensaje = (
            data.get("msg")
            or data.get("message")
            or data.get("error_description")
            or data.get("error")
            or "No se pudo cambiar la contraseña."
        )
        return None, str(mensaje)

    except Exception as e:
        return None, f"Error de conexión con el servicio de autenticación: {e}"


def limpiar_recuperacion():
    """Elimina cualquier sesión temporal de recuperación."""
    for clave in (
        "recovery_email",
        "recovery_access_token",
        "recovery_codigo_verificado",
    ):
        st.session_state.pop(clave, None)

def cerrar_sesion():
    """Limpia la sesión local de Streamlit."""
    for clave in (
        "usuario_auth",
        "token_verificado",
        "fecha_vencimiento",
        "tipo_acceso",
        # No dejar credenciales/configuración del broker en una sesión
        # que pueda ser reutilizada por otro usuario.
        "bk_api_key",
        "bk_api_secret",
        "bk_nombre",
        "bk_puente",
        "bk_cargado",
        "_bk_guardado",
        "bk_colores",
        "usar_api_broker_dashboard",
    ):
        st.session_state.pop(clave, None)
    for _i in range(len(COLORES_LAYOUT_DEFECTO)):
        st.session_state.pop(f"bk_wh_{_i}", None)


def _guardar_usuario_auth(data, tipo="usuario"):
    """Guarda únicamente los datos necesarios para la sesión actual."""
    usuario = data.get("user") or {}

    # En algunos flujos de Supabase el user puede no venir completo,
    # pero sí viene el access_token.
    st.session_state["usuario_auth"] = {
        "user_id": usuario.get("id", ""),
        "email": usuario.get("email", ""),
        "access_token": data.get("access_token", ""),
        "refresh_token": data.get("refresh_token", ""),
    }
    st.session_state["tipo_acceso"] = tipo


def pantalla_autenticacion():
    """
    Pantalla inicial:
      1) Iniciar sesión
      2) Registrarse
      3) Acceso administrador
    """
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at 50% 0%, rgba(212,175,55,.10), transparent 35%),
                #030303 !important;
        }
        .auth-card {
            max-width: 520px;
            margin: 45px auto 20px auto;
            background: #0d1118;
            border: 1px solid #2a3348;
            border-radius: 16px;
            padding: 30px;
            box-shadow: 0 18px 50px rgba(0,0,0,.35);
        }
        .auth-title {
            color: #d4af37;
            font-family: sans-serif;
            font-weight: 800;
            text-align: center;
            margin-bottom: 4px;
        }
        .auth-subtitle {
            color: #8e96a3;
            text-align: center;
            font-size: 11px;
            letter-spacing: 2px;
            margin-bottom: 20px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="auth-card">
            <div class="auth-title">TRADE SCANNER INSTITUTIONAL</div>
            <div class="auth-subtitle">SCANNER</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Una sola ventana para todos.
    # El acceso de administrador está dentro de la misma pantalla y
    # requiere el token secreto configurado en Streamlit Secrets.
    # No se utiliza una segunda URL ni un parámetro especial de administrador.
    tab_login, tab_registro, tab_admin = st.tabs(
        ["🔐 Iniciar sesión", "📝 Registrarse", "👑 Administrador"]
    )

    with tab_login:
        st.markdown("### Acceso de usuario")
        with st.form("form_login_usuario"):
            email = st.text_input(
                "Correo electrónico",
                placeholder="tu@email.com",
                key="login_email",
            )
            password = st.text_input(
                "Contraseña",
                type="password",
                key="login_password",
            )
            entrar = st.form_submit_button(
                "🚀 INICIAR SESIÓN",
                width="stretch",
            )

        if entrar:
            data, error = iniciar_sesion_usuario(email, password)
            if error:
                st.error(f"❌ {error}")
            else:
                _guardar_usuario_auth(data, tipo="usuario")
                st.rerun()

        with st.expander("🔑 ¿Olvidaste tu contraseña?", expanded=bool(st.session_state.get("recovery_email"))):
            st.caption("Te enviaremos un código de recuperación por correo. No necesitas abrir ningún enlace.")

            email_recuperacion = st.text_input(
                "Correo de tu cuenta",
                value=st.session_state.get("recovery_email", st.session_state.get("login_email", "")),
                placeholder="tu@email.com",
                key="recovery_email_ui",
            )
            st.session_state["recovery_email"] = str(email_recuperacion).strip().lower()

            if not st.session_state.get("recovery_codigo_verificado"):
                with st.form("form_recuperar_password"):
                    enviar_recuperacion = st.form_submit_button(
                        "📩 ENVIAR CÓDIGO DE RECUPERACIÓN",
                        width="stretch",
                    )

                if enviar_recuperacion:
                    _, error_recuperacion = solicitar_recuperacion(email_recuperacion)
                    if error_recuperacion:
                        st.error(f"❌ {error_recuperacion}")
                    else:
                        st.success(
                            "✅ Si el correo está registrado, recibirás un código. "
                            "Revisa también la carpeta de spam."
                        )
                        st.session_state["recovery_codigo_enviado"] = True

                if st.session_state.get("recovery_codigo_enviado"):
                    with st.form("form_verificar_codigo_recuperacion"):
                        codigo_recuperacion = st.text_input(
                            "Código recibido por correo",
                            placeholder="Ej.: 123456",
                            max_chars=8,
                            key="recovery_otp",
                        )
                        verificar_codigo = st.form_submit_button(
                            "🔐 VERIFICAR CÓDIGO",
                            width="stretch",
                        )

                    if verificar_codigo:
                        data_recuperacion, error_verificacion = verificar_codigo_recuperacion(
                            email_recuperacion,
                            codigo_recuperacion,
                        )
                        if error_verificacion:
                            st.error(f"❌ {error_verificacion}")
                        else:
                            st.session_state["recovery_access_token"] = data_recuperacion.get("access_token", "")
                            st.session_state["recovery_codigo_verificado"] = True
                            st.success("✅ Código verificado. Ahora puedes crear una contraseña nueva.")
                            st.rerun()

            if st.session_state.get("recovery_codigo_verificado"):
                st.info("🔓 Identidad verificada. Crea tu nueva contraseña.")
                with st.form("form_nueva_password_recuperacion"):
                    nueva_password_recuperacion = st.text_input(
                        "Nueva contraseña",
                        type="password",
                        key="recovery_new_password",
                    )
                    repetir_password_recuperacion = st.text_input(
                        "Repetir nueva contraseña",
                        type="password",
                        key="recovery_new_password_2",
                    )
                    cambiar_password = st.form_submit_button(
                        "💾 CAMBIAR CONTRASEÑA",
                        width="stretch",
                    )

                if cambiar_password:
                    if nueva_password_recuperacion != repetir_password_recuperacion:
                        st.error("❌ Las contraseñas no coinciden.")
                    else:
                        _, error_password = actualizar_password_recuperacion(
                            st.session_state.get("recovery_access_token", ""),
                            nueva_password_recuperacion,
                        )
                        if error_password:
                            st.error(f"❌ {error_password}")
                        else:
                            limpiar_recuperacion()
                            st.success("✅ Contraseña cambiada correctamente. Ya puedes iniciar sesión con tu nueva contraseña.")
                            st.rerun()

    with tab_registro:
        st.markdown("### Crear cuenta")
        st.caption("Crea tu acceso personal al scanner.")

        with st.form("form_registro_usuario"):
            nuevo_email = st.text_input(
                "Correo electrónico",
                placeholder="tu@email.com",
                key="registro_email",
            )
            nueva_password = st.text_input(
                "Contraseña",
                type="password",
                key="registro_password",
            )
            repetir_password = st.text_input(
                "Repetir contraseña",
                type="password",
                key="registro_password_2",
            )
            registrar = st.form_submit_button(
                "📝 CREAR CUENTA",
                width="stretch",
            )

        if registrar:
            if nueva_password != repetir_password:
                st.error("❌ Las contraseñas no coinciden.")
            else:
                data, error = registrar_usuario(nuevo_email, nueva_password)

                if error:
                    st.error(f"❌ {error}")
                else:
                    # Si Supabase devuelve access_token, la sesión puede
                    # iniciarse inmediatamente. Si no, normalmente significa
                    # que está activada la confirmación por correo.
                    if data and data.get("access_token"):
                        _guardar_usuario_auth(data, tipo="usuario")
                        st.success("Cuenta creada correctamente.")
                        st.rerun()
                    else:
                        st.success(
                            "✅ Cuenta creada. Revisa tu correo para confirmar "
                            "la cuenta y después inicia sesión."
                        )

    with tab_admin:
            st.markdown("### Acceso del administrador")
            st.caption("Este acceso conserva el sistema de token del propietario.")

            with st.form("form_admin_token"):
                token_ingresado = st.text_input(
                    "Token de administrador",
                    type="password",
                    key="admin_token_login",
                )
                entrar_admin = st.form_submit_button(
                    "👑 VALIDAR ACCESO",
                    width="stretch",
                )

            if entrar_admin:
                token_limpio = token_ingresado.strip()
                es_valido, estado = verificar_token(token_limpio)

                if es_valido:
                    st.session_state["token_verificado"] = token_limpio
                    st.session_state["fecha_vencimiento"] = estado
                    st.session_state["tipo_acceso"] = "admin"
                    st.rerun()
                elif estado == "EXPIRADO":
                    st.error("🔒 Token expirado.")
                elif estado == "FORMATO":
                    st.error("❌ Error de configuración del token.")
                else:
                    st.error("❌ Token no válido. Acceso denegado.")

    st.stop()


# Si no existe ninguna sesión, mostramos login/registro.
if (
    "token_verificado" not in st.session_state
    and "usuario_auth" not in st.session_state
):
    pantalla_autenticacion()


# =========================================================
# IDENTIDAD ACTIVA
# =========================================================
if "token_verificado" in st.session_state:
    TOKEN_ACTIVO = st.session_state["token_verificado"]
    FECHA_VENCIMIENTO_LICENCIA = st.session_state.get(
        "fecha_vencimiento", "2099-01-01"
    )
    TIPO_ACCESO = "admin"
else:
    _usuario_actual = st.session_state.get("usuario_auth", {})
    TOKEN_ACTIVO = (
        _usuario_actual.get("user_id")
        or _usuario_actual.get("email")
        or "usuario"
    )
    FECHA_VENCIMIENTO_LICENCIA = "2099-01-01"
    TIPO_ACCESO = "usuario"


# Solo los tokens configurados como ADMIN pueden ser administradores.
ADMIN_TOKEN = st.secrets.get("ADMIN_TOKEN", None)
_ADMIN_TOKENS_RAW = st.secrets.get("ADMIN_TOKENS", "")

if isinstance(_ADMIN_TOKENS_RAW, (list, tuple, set)):
    ADMIN_TOKENS = {
        str(x).strip()
        for x in _ADMIN_TOKENS_RAW
        if str(x).strip()
    }
else:
    ADMIN_TOKENS = {
        x.strip()
        for x in str(_ADMIN_TOKENS_RAW).split(",")
        if x.strip()
    }

if ADMIN_TOKEN:
    ADMIN_TOKENS.add(str(ADMIN_TOKEN).strip())

ES_ADMIN = (
    TIPO_ACCESO == "admin"
    and bool(TOKEN_ACTIVO)
    and TOKEN_ACTIVO in ADMIN_TOKENS
)


# Barra discreta de sesión.
with st.sidebar:
    st.markdown("### 👤 Sesión")

    if ES_ADMIN:
        st.success("Administrador")
    else:
        _email_ui = st.session_state.get("usuario_auth", {}).get(
            "email", "Usuario"
        )
        st.info(_email_ui)

    if st.button(
        "🚪 CERRAR SESIÓN",
        key="cerrar_sesion_global",
        width="stretch",
    ):
        cerrar_sesion()
        st.rerun()


# ==========================================
# 🧮 LÓGICA PURA (indicadores y filtros)
# ==========================================
def formatear_numero_grande(numero):
    try:
        numero = float(numero)
    except (TypeError, ValueError):
        return "N/A"
    if numero >= 1_000_000:
        return f"{numero / 1_000_000:.1f}M"
    elif numero >= 1_000:
        return f"{numero / 1_000:.0f}K"
    return f"{numero:.0f}"


def evaluar_tecnico(velas):
    """Calcula EMA20/MACD/Bollinger sobre velas de 1 minuto.

    Señal EMA20 solicitada:
      1) la vela actual NACE (abre) por encima de la EMA20 de la vela anterior;
      2) su máximo es mayor que el máximo de la vela anterior;
      3) su mínimo es mayor que el mínimo de la vela anterior.

    La EMA20 se calcula sobre cierres. Para evitar que la EMA "se mueva"
    durante la vela que nace, se compara el OPEN actual contra la EMA20
    calculada hasta la vela anterior.
    """
    if velas is None or len(velas) < 40:
        return (False, False, False, False, None, None, None, 0,
                None, None, None, None, None, None)

    try:
        velas = velas.sort_index()
        cierres = velas["close"].astype(float).dropna()
        if len(cierres) < 40:
            return (False, False, False, False, None, None, None, 0,
                    None, None, None, None, None, None)

        # Aseguramos que OHLC y cierres correspondan a las últimas dos velas.
        if not all(col in velas.columns for col in ("open", "high", "low", "close")):
            return (False, False, False, False, None, None, None, 0,
                    None, None, None, None, None, None)

        vela_prev = velas.iloc[-2]
        vela_act = velas.iloc[-1]
        ema20 = cierres.ewm(span=20, adjust=False).mean()
        macd_line = cierres.ewm(span=12, adjust=False).mean() - cierres.ewm(span=26, adjust=False).mean()

        precio_act = float(vela_act["close"])
        precio_prev = float(vela_prev["close"])
        ema_act = float(ema20.iloc[-1])
        ema_prev = float(ema20.iloc[-2])
        macd_actual = macd_line.iloc[-1]
        macd_val = float(macd_actual) if not pd.isna(macd_actual) else None

        bb_mid = cierres.rolling(20).mean()
        bb_std = cierres.rolling(20).std()
        bb_upper = bb_mid.iloc[-1] + 2 * bb_std.iloc[-1]
        bb_upper_val = float(bb_upper) if not pd.isna(bb_upper) else None

        open_act = float(vela_act["open"])
        high_act = float(vela_act["high"])
        low_act = float(vela_act["low"])
        high_prev = float(vela_prev["high"])
        low_prev = float(vela_prev["low"])

        if pd.isna(ema_act) or ema_act <= 0 or pd.isna(ema_prev) or ema_prev <= 0:
            return (False, False, False, False, precio_act, None, macd_val, len(cierres),
                    precio_prev, ema_prev, precio_act, ema_act, bb_upper_val, None)

        # EMA20 NUEVA: vela naciendo por encima + máximo y mínimo superiores.
        estructura_alcista = bool(
            open_act > ema_prev and
            high_act > high_prev and
            low_act > low_prev
        )

        # La señal de bajada conserva una lógica simétrica para no romper
        # el selector existente de la interfaz.
        estructura_bajista = bool(
            open_act < ema_prev and
            high_act < high_prev and
            low_act < low_prev
        )

        cruzo_arriba = estructura_alcista
        cruzo_abajo = estructura_bajista
        macd_positivo = bool(macd_val is not None and macd_val > 0)
        macd_negativo = bool(macd_val is not None and macd_val < 0)
        bb_dist_pct = ((bb_upper_val - precio_act) / precio_act * 100.0) if bb_upper_val is not None and precio_act > 0 else None

        return (cruzo_arriba, cruzo_abajo, macd_positivo, macd_negativo,
                precio_act, ema_act, macd_val, len(cierres), precio_prev, ema_prev,
                precio_act, ema_act, bb_upper_val, bb_dist_pct)
    except Exception as e:
        print(f"⚠️ Error evaluando EMA20/velas: {e}")
        return (False, False, False, False, None, None, None, 0,
                None, None, None, None, None, None)


def descargar_cierres(data_client, tickers):
    """Descarga OHLC de velas de 1 minuto de Alpaca para EMA20/MACD."""
    salida = {}
    if not tickers:
        return salida

    for i in range(0, len(tickers), 50):
        lote = tickers[i:i + 50]
        try:
            # En Alpaca Basic conservamos el retraso histórico de ~20 minutos
            # que ya utilizaba la aplicación. La condición EMA20 se evalúa
            # sobre la última vela disponible de ese histórico.
            inicio = datetime.now(timezone.utc) - timedelta(days=2)
            fin = datetime.now(timezone.utc) - timedelta(minutes=20)
            solicitud = StockBarsRequest(
                symbol_or_symbols=lote,
                timeframe=TimeFrame.Minute,
                start=inicio,
                end=fin,
                limit=10000,
            )
            barras = data_client.get_stock_bars(solicitud)
            datos = getattr(barras, "df", None)
        except Exception as e:
            print(f"⚠️ Error descargando velas de Alpaca (lote {len(lote)}): {e}")
            continue

        if datos is None or datos.empty:
            continue

        try:
            if isinstance(datos.index, pd.MultiIndex):
                for ticker in lote:
                    try:
                        marco = datos.xs(ticker, level=0)[["open", "high", "low", "close"]].dropna()
                        marco = marco.sort_index()
                    except Exception:
                        continue
                    if len(marco) >= 40:
                        salida[ticker] = marco
            else:
                if all(col in datos.columns for col in ("open", "high", "low", "close")) and len(lote) == 1:
                    marco = datos[["open", "high", "low", "close"]].dropna().sort_index()
                    if len(marco) >= 40:
                        salida[lote[0]] = marco
        except Exception:
            continue
    return salida

def filtrar_resultados(filas, p):
    resultado = []
    for c in filas:
        if not (p["precio_min"] <= c["precio"] <= p["precio_max"]):
            continue
        # Etapa 1: dejamos fuera gap, float y volumen para localizar
        # exactamente qué filtro está provocando la caída a cero.
        if ETAPA_PRUEBA_FILTROS >= 4:
            if not (p["gap_min"] <= c["cambio_pct"] <= p["gap_max"]):
                continue
        if ETAPA_PRUEBA_FILTROS >= 3:
            if c["float_shares"] is not None and c["float_shares"] > p["flotacion_max"]:
                continue
        if ETAPA_PRUEBA_FILTROS >= 2:
            if c.get("volumen_dia", 0) < p.get("volumen_min", 20_000):
                continue
        if ETAPA_PRUEBA_FILTROS == 1:
            # PRUEBA 1 real: EMA20 = precio por encima de EMA20.
            if not c.get("cruzando_ema20", False):
                continue
            macd = "Positivo"
        else:
            cruce = p.get("cruce_ema", "Neutro")
            if cruce in ("Hacia arriba", "Vela nueva sobre EMA20 + HH/HL") and not c["cruzando_ema20"]:
                continue
            if cruce == "Hacia abajo" and not c["cruzando_ema20_abajo"]:
                continue
            macd = p.get("macd", "No exigir")
        if macd == "Positivo" and not c["macd_positivo"]:
            continue
        if macd == "Negativo" and not c["macd_negativo"]:
            continue
        resultado.append(c)

    claves = {
        "Actualizado": lambda x: x["actualizado"],
        "Cambio %": lambda x: x["cambio_pct"],
        "Volumen": lambda x: x["volumen_dia"],
    }
    resultado.sort(key=claves.get(p.get("orden", "Actualizado"), claves["Actualizado"]), reverse=True)
    # Tolerante a configuraciones antiguas sin top_n. Esto evita que una
    # ausencia de esa clave aborte el ciclo completo del scanner.
    try:
        limite = max(1, int(p.get("top_n", 10)))
    except (TypeError, ValueError):
        limite = 10
    return resultado[:limite]


def filtrar_eventos(eventos, p):
    """Filtros numéricos del usuario aplicados al cuadro de eventos (sin EMA/MACD/Top N)."""
    salida = []
    for e in eventos:
        if not (p["precio_min"] <= e["precio"] <= p["precio_max"]):
            continue
        if not (p["gap_min"] <= e["cambio_pct"] <= p["gap_max"]):
            continue
        if e["float_shares"] is not None and e["float_shares"] > p["flotacion_max"]:
            continue
        if e.get("volumen_dia", 0) < p.get("volumen_min", 20_000):
            continue
        salida.append(e)
    return salida


# ==========================================
# ⚡️ MOTOR COMPARTIDO (un solo hilo para TODOS los usuarios)
# ==========================================
class ServicioScanner:
    def __init__(self, api_key, secret_key, tg_token, tg_chat, fmp_api_key, filtros_dueno):
        self.api_key = api_key
        self.secret_key = secret_key
        self.tg_token = tg_token
        self.tg_chat = tg_chat
        self.fmp_api_key = fmp_api_key
        self.filtros_dueno = filtros_dueno

        self.trading = TradingClient(api_key, secret_key)
        self.data = StockHistoricalDataClient(api_key=api_key, secret_key=secret_key)

        self.encendido = True
        # Control manual del administrador: si se apaga, el horario automático NO lo vuelve a encender.
        # El horario se carga desde disco si el administrador ya lo guardó antes;
        # si no hay nada guardado, usa los valores por defecto del código.
        self.hora_inicio_auto_min, self.hora_fin_auto_min = cargar_horario_guardado()
        self.resultados = []
        self.ultima_actualizacion = None
        self.duracion_ciclo = None
        self.ultimo_error = None
        self.n_radar_base = 0
        self.universo = []
        self.universo_ts = 0.0

        self.calendario_ts = 0.0
        self.dias_mercado_cache = set()
        self.auto_en_horario = False
        self.auto_motivo = "Esperando calendario de Alpaca"
        self.float_pendientes = 0
        self.float_sin_dato = 0

        # Diagnóstico temporal del embudo de filtros (visible solo al administrador).
        self.diagnostico_filtros = {
            "radar_base": 0,
            "tras_float": 0,
            "tras_vol_rel": 0,
            "ema_arriba": 0,
            "macd_positivo": 0,
            "ema_y_macd": 0,
            "resultados": 0,
            "raw_tickers": [],
            "final_tickers_mismo_ciclo": [],
            "eliminados_post_ema_macd": [],
            "eliminados_post_ema_macd_count": 0,
            "gap_aplicado": ETAPA_PRUEBA_FILTROS >= 4,
            "gap_min": self.filtros_dueno.get("gap_min", BASE_GAP_MIN),
            "gap_max": self.filtros_dueno.get("gap_max", BASE_GAP_MAX),
        }

        self.tg_msg_id = None
        self.tg_ultimo_hash = None

        self.eventos = []                    # cuadro "Eventos en vivo" (el más nuevo primero)
        self._ultimo_precio_evento = {}
        self.historial_ciclos = []            # últimos ciclos: permite ver cuándo entran/salen candidatos
        self._raw_tickers_ciclo_anterior = set()

        # PRUEBA 6: seguimiento temporal de señales EMA20+MACD.
        # Cada señal se observa durante una ventana fija y se conserva
        # el máximo precio visto para calcular MFE. No afecta filtros.
        self.prueba6_activos = {}
        self.prueba6_completadas = []

        self.cache_tecnico = {}
        self.cache_fund = self._leer_cache_fundamentales()
        # Control específico de FMP para no martillar la API cuando devuelve HTTP 429.
        self.fmp_pausado_hasta = 0.0
        self._ultima_peticion_fmp = 0.0

        self._lock_ritmo = threading.Lock()
        self._ultima_peticion = 0.0

        # Control del hilo para permitir un reinicio limpio desde el panel de administrador.
        self._detener_hilo = threading.Event()
        self._lock_reinicio = threading.Lock()
        self._hilo = threading.Thread(target=self._bucle, daemon=True)
        self._hilo.start()

    # ---------- utilidades ----------
    def _esperar_turno(self):
        with self._lock_ritmo:
            espera = self._ultima_peticion + PAUSA_MIN_ENTRE_PETICIONES - time.monotonic()
            if espera > 0:
                time.sleep(espera)
            self._ultima_peticion = time.monotonic()

    def _leer_cache_fundamentales(self):
        try:
            with open(RUTA_CACHE_FUNDAMENTALES, "r") as f:
                return json.load(f)
        except Exception:
            return {}

    def _guardar_cache_fundamentales(self):
        try:
            with open(RUTA_CACHE_FUNDAMENTALES, "w") as f:
                json.dump(self.cache_fund, f)
        except Exception as e:
            print(f"⚠️ No se pudo guardar la caché de fundamentales: {e}")

    # ---------- horario automático ----------
    def _actualizar_calendario(self, ahora_et):
        """Consulta el calendario de Alpaca y lo guarda en memoria."""
        if self.dias_mercado_cache and time.time() - self.calendario_ts < TTL_CALENDARIO_MERCADO:
            return
        try:
            inicio = ahora_et.date()
            fin = inicio + timedelta(days=14)
            calendario = self.trading.get_calendar(
                GetCalendarRequest(start=inicio, end=fin)
            )
            self.dias_mercado_cache = {c.date for c in calendario}
            self.calendario_ts = time.time()
        except Exception as e:
            self.ultimo_error = f"Calendario Alpaca: {e}"
            print(f"⚠️ Error consultando calendario de Alpaca: {e}")

    def _esta_en_horario_automatico(self):
        """True solo de 04:00 a 16:00 ET en un día de mercado según Alpaca."""
        ahora_et = datetime.now(ET)
        self._actualizar_calendario(ahora_et)
        es_dia_mercado = ahora_et.date() in self.dias_mercado_cache
        minuto_actual = ahora_et.hour * 60 + ahora_et.minute + ahora_et.second / 60
        inicio = self.hora_inicio_auto_min
        fin = self.hora_fin_auto_min
        if inicio == fin:
            en_ventana = False
        elif inicio < fin:
            en_ventana = inicio <= minuto_actual < fin
        else:
            # Permite horarios que crucen medianoche, por ejemplo 22:00–04:00.
            en_ventana = minuto_actual >= inicio or minuto_actual < fin
        self.auto_en_horario = bool(es_dia_mercado and en_ventana)
        inicio_txt = f"{inicio // 60:02d}:{inicio % 60:02d}"
        fin_txt = f"{fin // 60:02d}:{fin % 60:02d}"
        if self.auto_en_horario:
            self.auto_motivo = f"Horario automático activo · {inicio_txt}–{fin_txt} ET"
        elif not es_dia_mercado:
            self.auto_motivo = "Fuera de día de mercado según Alpaca"
        else:
            self.auto_motivo = f"Fuera del horario automático · {inicio_txt}–{fin_txt} ET"
        return self.auto_en_horario

    def configurar_horario(self, inicio, fin):
        """Actualiza el horario automático compartido por todo el scanner y lo guarda en disco
        para que sobreviva a reinicios de la app (redeploy, inactividad, etc.)."""
        self.hora_inicio_auto_min = inicio.hour * 60 + inicio.minute
        self.hora_fin_auto_min = fin.hour * 60 + fin.minute
        guardar_horario_en_disco(self.hora_inicio_auto_min, self.hora_fin_auto_min)
        self.auto_motivo = "Horario automático actualizado; esperando el próximo ciclo"

    def reiniciar_scanner(self):
        """Reinicia de forma segura el motor compartido del scanner.

        Detiene el hilo anterior, limpia el estado de resultados/cachés de trabajo
        y crea un único hilo nuevo. No modifica las credenciales ni el horario.
        """
        with self._lock_reinicio:
            hilo_anterior = self._hilo
            self._detener_hilo.set()

            # Espera brevemente a que el hilo anterior termine su ciclo actual.
            if hilo_anterior is not None and hilo_anterior.is_alive() and hilo_anterior is not threading.current_thread():
                hilo_anterior.join(timeout=max(2.0, INTERVALO_ESCANEO_SEGUNDOS + 1.0))

            # Limpia únicamente el estado operativo que puede quedar obsoleto.
            self.resultados = []
            self.ultima_actualizacion = None
            self.duracion_ciclo = None
            self.ultimo_error = None
            self.n_radar_base = 0
            self.universo = []
            self.universo_ts = 0.0
            self.calendario_ts = 0.0
            self.dias_mercado_cache = set()
            self.auto_en_horario = False
            self.auto_motivo = "Scanner reiniciado; esperando el próximo ciclo"
            self.float_pendientes = 0
            self.float_sin_dato = 0
            self.diagnostico_filtros = {
                "radar_base": 0,
                "tras_float": 0,
                "tras_vol_rel": 0,
                "ema_arriba": 0,
                "macd_positivo": 0,
                "ema_y_macd": 0,
                "resultados": 0,
                "gap_aplicado": ETAPA_PRUEBA_FILTROS >= 4,
                "gap_min": self.filtros_dueno.get("gap_min", BASE_GAP_MIN),
                "gap_max": self.filtros_dueno.get("gap_max", BASE_GAP_MAX),
            }
            self.tg_msg_id = None
            self.tg_ultimo_hash = None
            self.eventos = []
            self._ultimo_precio_evento = {}
            self.historial_ciclos = []
            self._raw_tickers_ciclo_anterior = set()
            self.candidatos_ema_macd_actual = []
            self.prueba6_activos = {}
            self.prueba6_completadas = []
            self.finales_ema_macd_actual = []
            self.cache_tecnico = {}
            self.fmp_pausado_hasta = 0.0
            self._ultima_peticion_fmp = 0.0
            self._ultima_peticion = 0.0

            # Nuevo hilo único. Las credenciales y la configuración permanecen intactas.
            self._detener_hilo.clear()
            self._hilo = threading.Thread(target=self._bucle, daemon=True)
            self._hilo.start()

    # ---------- universo ----------
    def _cargar_universo(self):
        try:
            solicitud = GetAssetsRequest(asset_class=AssetClass.US_EQUITY, status=AssetStatus.ACTIVE)
            activos = self.trading.get_all_assets(solicitud)
            self.universo = [
                a.symbol for a in activos
                if a.tradable
                and a.exchange in ("NASDAQ", "NYSE", "AMEX", "ARCA")
                and "." not in a.symbol
                and "-" not in a.symbol
            ]
            self.universo_ts = time.time()
            print(f"🌐 Universo cargado: {len(self.universo)} tickers.")
        except Exception as e:
            self.ultimo_error = f"Universo: {e}"
            print(f"⚠️ Error cargando universo: {e}")

    # ---------- snapshots en paralelo ----------
    def _descargar_snapshots(self):
        lotes = [self.universo[i:i + TAMANO_LOTE_SNAPSHOT] for i in range(0, len(self.universo), TAMANO_LOTE_SNAPSHOT)]

        def pedir(lote):
            self._esperar_turno()
            try:
                return self.data.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=lote)) or {}
            except Exception as e:
                self.ultimo_error = f"Snapshot: {e}"
                return {}

        snapshots = {}
        with ThreadPoolExecutor(max_workers=WORKERS_SNAPSHOT) as ex:
            for parcial in ex.map(pedir, lotes):
                snapshots.update(parcial)
        return snapshots

    # ---------- float y volumen promedio (FMP + Alpaca; sin Yahoo Finance) ----------
    @staticmethod
    def _extraer_float_fmp(payload):
        """Extrae el float de las distintas formas de respuesta de FMP."""
        claves = ("floatShares", "float_shares", "freeFloatShares", "freeFloat")

        def convertir(valor):
            if valor is None or isinstance(valor, bool):
                return None
            try:
                if isinstance(valor, str):
                    valor = valor.replace(",", "").strip()
                numero = float(valor)
                return numero if numero > 0 else None
            except (TypeError, ValueError):
                return None

        def buscar(obj):
            if isinstance(obj, dict):
                for clave in claves:
                    numero = convertir(obj.get(clave))
                    if numero is not None:
                        return numero
                for valor in obj.values():
                    numero = buscar(valor)
                    if numero is not None:
                        return numero
            elif isinstance(obj, list):
                for item in obj:
                    numero = buscar(item)
                    if numero is not None:
                        return numero
            return None

        return buscar(payload)

    def _float_fmp(self, ticker):
        if not self.fmp_api_key:
            self.ultimo_error = "FMP_API_KEY no está configurada en Streamlit Secrets; no se puede obtener el float."
            return None

        ahora = time.time()
        if ahora < self.fmp_pausado_hasta:
            restante = max(1, int(self.fmp_pausado_hasta - ahora))
            minutos = restante // 60 + (1 if restante % 60 else 0)
            self.ultimo_error = (
                "FMP está en pausa por límite de solicitudes (HTTP 429). "
                f"Se reintentará en aproximadamente {minutos} min."
            )
            return None

        try:
            # FMP tiene un límite separado del ritmo de Alpaca. Espaciamos las
            # consultas para evitar una cascada de HTTP 429.
            ahora = time.time()
            espera_fmp = self._ultima_peticion_fmp + FMP_MIN_INTERVAL_SEGUNDOS - ahora
            if espera_fmp > 0:
                time.sleep(espera_fmp)
            self._ultima_peticion_fmp = time.time()
            respuesta = requests.get(
                FMP_API_URL,
                params={"symbol": ticker, "apikey": self.fmp_api_key},
                timeout=8,
            )
            if respuesta.status_code == 429:
                self.fmp_pausado_hasta = time.time() + PAUSA_FMP_429_SEGUNDOS
                self.ultimo_error = (
                    f"FMP devolvió HTTP 429 para {ticker}. "
                    "Se pausaron las consultas de float durante 15 minutos para evitar más bloqueos."
                )
                print(f"⚠️ FMP HTTP 429 para {ticker}; pausa de {PAUSA_FMP_429_SEGUNDOS}s")
                return None
            if respuesta.status_code in (401, 403):
                self.ultimo_error = (
                    f"FMP rechazó la API para {ticker} (HTTP {respuesta.status_code}). "
                    "Revisa que FMP_API_KEY sea válida y tenga acceso a shares-float."
                )
                return None
            if respuesta.status_code != 200:
                self.ultimo_error = f"FMP devolvió HTTP {respuesta.status_code} para {ticker}."
                return None
            try:
                payload = respuesta.json()
            except ValueError:
                self.ultimo_error = f"FMP devolvió una respuesta no JSON para {ticker}."
                return None
            valor = self._extraer_float_fmp(payload)
            if valor is None:
                print(f"⚠️ FMP sin float para {ticker}")
            else:
                self.ultimo_error = None
            return valor
        except Exception as e:
            self.ultimo_error = f"Error consultando FMP para {ticker}: {e}"
            print(f"⚠️ FMP float {ticker}: {e}")
            return None

    def _asegurar_fundamentales(self, tickers):
        ahora = time.time()
        faltan = []
        for t in tickers:
            e = self.cache_fund.get(t)
            if e is None:
                faltan.append(t)
                continue
            ts = float(e.get("ts", 0))
            if e.get("float") is None and ahora - ts > REINTENTO_FUNDAMENTALES:
                faltan.append(t)
            elif ahora - ts > VIGENCIA_FUNDAMENTALES:
                faltan.append(t)

        # Tope duro por ciclo.
        faltan = faltan[:MAX_FUNDAMENTALES_POR_CICLO]
        if not faltan:
            return

        def pedir(t):
            # FMP es la única fuente de FLOAT.
            float_fmp = self._float_fmp(t)
            float_final = float_fmp
            try:
                if float_final is not None:
                    float_final = float(float_final)
            except (TypeError, ValueError):
                float_final = None

            if float_fmp is not None:
                estado, fuente = "ok", "FMP"
            else:
                estado, fuente = "no_data", "FMP"

            return t, {
                "float": float_final,
                "float_source": fuente,
                "float_status": estado,
                "ts": time.time(),
            }

        with ThreadPoolExecutor(max_workers=WORKERS_FUNDAMENTALES) as ex:
            for t, entrada in ex.map(pedir, faltan):
                self.cache_fund[t] = entrada
        self._guardar_cache_fundamentales()

    # ---------- EMA20 / MACD ----------
    def _asegurar_tecnico(self, tickers):
        ahora = time.time()
        pendientes = [
            t for t in tickers
            if t not in self.cache_tecnico or ahora - self.cache_tecnico[t][0] > TTL_TECNICO_SEGUNDOS
        ]
        if not pendientes:
            return
        series = descargar_cierres(self.data, pendientes)
        for t in pendientes:
            (cruz_arriba, cruz_abajo, macd_pos, macd_neg, precio_act, ema_act,
             macd_val, barras_count, precio_prev, ema_prev, precio_actual,
             ema_actual, bb_upper, bb_dist_pct) = evaluar_tecnico(series.get(t))
            self.cache_tecnico[t] = (
                ahora, cruz_arriba, cruz_abajo, macd_pos, macd_neg,
                precio_act, ema_act, macd_val, barras_count,
                precio_prev, ema_prev, precio_actual, ema_actual,
                bb_upper, bb_dist_pct
            )

    # ---------- noticias (una sola llamada para todos) ----------
    def _noticias_recientes(self, tickers):
        if not tickers:
            return set()
        try:
            self._esperar_turno()
            desde = (datetime.now(timezone.utc) - timedelta(minutes=MINUTOS_NOTICIA_RECIENTE)).strftime("%Y-%m-%dT%H:%M:%SZ")
            respuesta = requests.get(
                "https://data.alpaca.markets/v1beta1/news",
                headers={"APCA-API-KEY-ID": self.api_key, "APCA-API-SECRET-KEY": self.secret_key},
                params={"symbols": ",".join(tickers), "start": desde, "limit": 50},
                timeout=8,
            )
            if respuesta.status_code == 200:
                con_noticia = set()
                for n in respuesta.json().get("news", []):
                    con_noticia.update(n.get("symbols", []))
                return con_noticia & set(tickers)
        except Exception:
            pass
        return set()

    # ---------- Telegram ----------
    def _enviar_telegram(self, texto_tabla):
        if not self.tg_token or not self.tg_chat:
            return
        hash_actual = hashlib.md5(texto_tabla.encode("utf-8")).hexdigest()
        if hash_actual == self.tg_ultimo_hash:
            return
        try:
            payload = {
                "chat_id": self.tg_chat,
                "text": f"⚡️ <b>SCANNER</b>\n<pre>{texto_tabla}</pre>",
                "parse_mode": "HTML",
            }
            cabeceras = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
            if self.tg_msg_id is None:
                url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
                r = requests.post(url, json=payload, headers=cabeceras, timeout=15)
                if r.status_code == 200:
                    self.tg_msg_id = r.json()["result"]["message_id"]
                    self.tg_ultimo_hash = hash_actual
                else:
                    print(f"❌ Telegram rechazó el mensaje: {r.status_code} - {r.text}")
            else:
                url = f"https://api.telegram.org/bot{self.tg_token}/editMessageText"
                payload["message_id"] = self.tg_msg_id
                r = requests.post(url, json=payload, headers=cabeceras, timeout=15)
                if r.status_code == 200 or "message is not modified" in r.text:
                    self.tg_ultimo_hash = hash_actual
                elif "not found" in r.text:
                    self.tg_msg_id = None  # el mensaje fue borrado: se envía uno nuevo en el próximo ciclo
                else:
                    print(f"❌ Error al editar en Telegram: {r.status_code} - {r.text}")
        except Exception as e:
            print(f"⚠️ Error de red con Telegram: {e}")

    def _escribir_html(self, texto_tabla):
        contenido = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>SCANNER</title>
<meta http-equiv="refresh" content="30">
<style>body {{ background:#121212; color:#00ffcc; font-family:'Courier New',monospace; padding:20px; }}
pre {{ background:#1e1e1e; padding:25px; border-radius:8px; border:1px solid #333; color:#fff; }}</style>
</head><body><h2>SCANNER</h2><pre>{texto_tabla}</pre></body></html>"""
        try:
            with open(os.path.join(os.getcwd(), NOMBRE_ARCHIVO_HTML), "w", encoding="utf-8") as f:
                f.write(contenido)
        except Exception:
            pass

    # ---------- eventos en vivo (cuadro de abajo) ----------
    def _registrar_eventos(self, candidatos):
        """Guarda un evento cada vez que cambia el último precio de un ticker del radar.
        subiendo=True (verde): el precio subió frente al evento anterior de ese ticker.
        subiendo=False (rojo): bajó. La primera vez que aparece, se usa el cambio % del día."""
        try:
            nuevos = []
            for c in candidatos:
                previo = self._ultimo_precio_evento.get(c["ticker"])
                if previo is not None and previo == c["precio"]:
                    continue
                subiendo = (c["precio"] > previo) if previo is not None else (c["cambio_pct"] >= 0)
                self._ultimo_precio_evento[c["ticker"]] = c["precio"]
                nuevos.append({
                    "ticker": c["ticker"],
                    "precio": c["precio"],
                    "cambio_pct": c["cambio_pct"],
                    "volumen_dia": c["volumen_dia"],
                    "float_shares": c["float_shares"],
                    "volumen_relativo": c["volumen_relativo"],
                    "tiene_noticia": c["tiene_noticia"],
                    "actualizado": c["actualizado"],
                    "subiendo": subiendo,
                })
            if nuevos:
                try:
                    nuevos.sort(key=lambda ev: ev["actualizado"], reverse=True)
                except Exception:
                    pass
                self.eventos = (nuevos + self.eventos)[:MAX_EVENTOS]
        except Exception as ex:
            print(f"⚠️ Error registrando eventos: {ex}")

    def _registrar_historial_ciclo(self, enriquecidos, resultados_finales):
        """Conserva una fotografía de los candidatos por ciclo para depuración.
        No altera filtros ni resultados; solo guarda evidencia de entradas/salidas.
        """
        try:
            ahora = datetime.now(ET)
            finales = []
            for c in resultados_finales:
                finales.append({
                    "ticker": c.get("ticker"),
                    "precio": c.get("precio"),
                    "cambio_pct": c.get("cambio_pct"),
                    "volumen_dia": c.get("volumen_dia"),
                    "ema20": c.get("tecnico_ema20"),
                    "macd": c.get("tecnico_macd"),
                })
            crudos = []
            for c in enriquecidos:
                if c.get("cruzando_ema20") and c.get("macd_positivo"):
                    crudos.append({
                        "ticker": c.get("ticker"),
                        "precio": c.get("precio"),
                        "cambio_pct": c.get("cambio_pct"),
                    })
            entrada = {
                "hora": ahora.strftime("%H:%M:%S ET"),
                "radar_base": self.n_radar_base,
                "tras_float": getattr(self, "n_tras_float", 0),
                "tras_volumen": len(enriquecidos),
                "ema_arriba": sum(1 for c in enriquecidos if c.get("cruzando_ema20")),
                "macd_positivo": sum(1 for c in enriquecidos if c.get("macd_positivo")),
                "ema_y_macd": len(crudos),
                "crudos": crudos,
                "finales": finales,
            }
            self.historial_ciclos = ([entrada] + list(self.historial_ciclos))[:MAX_HISTORIAL_CICLOS]
        except Exception as ex:
            print(f"⚠️ Error guardando historial de ciclos: {ex}")

    # ---------- PRUEBA 6: MFE posterior a la señal ----------
    def _actualizar_prueba6(self, candidatos, snapshots=None):
        """Observa el máximo precio posterior a cada señal EMA20+MACD.

        La ventana es fija (VENTANA_PRUEBA6_MINUTOS) y el cálculo es
        exclusivamente diagnóstico. No elimina ni modifica candidatos.
        La señal se toma en el momento en que el scanner la detecta.
        """
        try:
            ahora = datetime.now(ET)
            ahora_ts = ahora.timestamp()
            candidatos_validos = {
                c.get("ticker"): c for c in candidatos
                if c.get("ticker") and c.get("cruzando_ema20") and c.get("macd_positivo")
            }

            # Actualizar señales ya abiertas con el precio de mercado actual,
            # incluso si el ticker dejó de cumplir EMA20+MACD en este ciclo.
            # Así medimos realmente el recorrido posterior y no solo el tiempo
            # durante el cual el candidato permanece visible.
            for ticker, obs in list(self.prueba6_activos.items()):
                precio_actual = None
                snap = (snapshots or {}).get(ticker)
                if snap is not None and getattr(snap, "latest_trade", None):
                    precio_actual = getattr(snap.latest_trade, "price", None)
                if precio_actual is None:
                    precio_actual = candidatos_validos.get(ticker, {}).get("precio")
                if precio_actual is not None:
                    obs["max_precio"] = max(float(obs["max_precio"]), float(precio_actual))

                # PRUEBA 7: registrar la primera vez que el MFE alcanza cada objetivo.
                precio_senal_obs = float(obs.get("precio_senal") or 0)
                if precio_senal_obs > 0:
                    mfe_actual_pct = (float(obs["max_precio"]) - precio_senal_obs) / precio_senal_obs * 100.0
                    alcanzados = obs.setdefault("objetivos", {})
                    for objetivo in PRUEBA7_OBJETIVOS_PCT:
                        clave = f"{objetivo:.2f}"
                        if clave not in alcanzados and mfe_actual_pct >= objetivo:
                            alcanzados[clave] = ahora_ts - obs["inicio_ts"]

                transcurridos = ahora_ts - obs["inicio_ts"]
                if transcurridos >= VENTANA_PRUEBA6_MINUTOS * 60:
                    precio_senal = float(obs["precio_senal"])
                    max_precio = float(obs["max_precio"])
                    mfe_pct = ((max_precio - precio_senal) / precio_senal * 100.0) if precio_senal > 0 else None
                    obs_final = dict(obs)
                    objetivos = dict(obs.get("objetivos", {}))
                    obs_final.update({
                        "fin_hora": ahora.strftime("%H:%M:%S ET"),
                        "mfe_pct": mfe_pct,
                        "duracion_min": transcurridos / 60.0,
                        "objetivos": objetivos,
                        "alcanza_025": "0.25" in objetivos,
                        "alcanza_050": "0.50" in objetivos,
                        "alcanza_100": "1.00" in objetivos,
                        "tiempo_025_min": (objetivos.get("0.25") / 60.0) if "0.25" in objetivos else None,
                        "tiempo_050_min": (objetivos.get("0.50") / 60.0) if "0.50" in objetivos else None,
                        "tiempo_100_min": (objetivos.get("1.00") / 60.0) if "1.00" in objetivos else None,
                    })
                    self.prueba6_completadas.insert(0, obs_final)
                    self.prueba6_completadas = self.prueba6_completadas[:100]
                    del self.prueba6_activos[ticker]

            # Abrir una observación nueva solo para una señal detectada que
            # todavía no esté siendo observada.
            for ticker, c in candidatos_validos.items():
                if ticker in self.prueba6_activos:
                    continue
                precio_senal = c.get("tecnico_precio_actual")
                if precio_senal is None:
                    precio_senal = c.get("precio")
                if precio_senal is None or float(precio_senal) <= 0:
                    continue
                bb_upper = c.get("bb_upper")
                bb_dist = c.get("bb_dist_pct")
                self.prueba6_activos[ticker] = {
                    "ticker": ticker,
                    "inicio_ts": ahora_ts,
                    "inicio_hora": ahora.strftime("%H:%M:%S ET"),
                    "precio_senal": float(precio_senal),
                    "bb_upper_senal": float(bb_upper) if bb_upper is not None else None,
                    "bb_dist_inicial": float(bb_dist) if bb_dist is not None else None,
                    "max_precio": float(c.get("precio") if c.get("precio") is not None else precio_senal),
                    "objetivos": {},
                }
        except Exception as ex:
            print(f"⚠️ Error en PRUEBA 6: {ex}")

    # ---------- ciclo principal ----------
    def _ciclo(self):
        inicio = time.monotonic()
        self.ultimo_error = None

        if not self.universo or time.time() - self.universo_ts > 6 * 3600:
            self._cargar_universo()
        if not self.universo:
            return

        snapshots = self._descargar_snapshots()

        base = []
        for ticker, snap in snapshots.items():
            if not snap or not snap.latest_trade or not snap.daily_bar or not snap.previous_daily_bar:
                continue
            precio = snap.latest_trade.price
            cierre_prev = snap.previous_daily_bar.close
            if not cierre_prev or cierre_prev <= 0:
                continue
            if not (BASE_PRECIO_MIN <= precio <= BASE_PRECIO_MAX):
                continue
            cambio = ((precio - cierre_prev) / cierre_prev) * 100
            if ETAPA_PRUEBA_FILTROS >= 4 and not (BASE_GAP_MIN <= cambio <= BASE_GAP_MAX):
                continue
            base.append({
                "ticker": ticker,
                "precio": precio,
                "cambio_pct": cambio,
                "volumen_dia": snap.daily_bar.volume or 0,
                "actualizado": snap.latest_trade.timestamp,
            })

        # Conservar el total real del radar para la interfaz/diagnóstico.
        # Después se limita el enriquecimiento a MAX_ENRIQUECER para no saturar
        # las APIs, pero eso no debe convertir 532 candidatos en "0" ni en 120.
        radar_base_total = len(base)
        self.n_radar_base = radar_base_total
        base.sort(key=lambda c: c["volumen_dia"], reverse=True)
        base = base[:MAX_ENRIQUECER]

        if ETAPA_PRUEBA_FILTROS >= 3:
            self._asegurar_fundamentales([c["ticker"] for c in base])
        else:
            # Etapa 1: no consultar FMP/float. Así aislamos EMA + MACD
            # y evitamos que el límite HTTP 429 contamine la prueba.
            self.ultimo_error = None

        # PRUEBA 2: separamos el filtro de float y el de volumen en dos pasadas
        # para poder medir, en el diagnóstico, cuánto recorta CADA UNO por
        # separado (antes ambos se aplicaban en el mismo bucle y el panel
        # mostraba el mismo número para "tras_float" y "tras_vol_rel").
        tras_float = []
        for c in base:
            entrada = self.cache_fund.get(c["ticker"], {})
            float_shares = entrada.get("float")
            # En la etapa 1 no usamos float ni volumen como filtros.
            # Tampoco consultamos float en esta etapa para evitar HTTP 429 de FMP.
            # PRUEBA 3: se usa el mismo umbral que el resto de la app
            # (self.filtros_dueno["flotacion_max"], 20,000,000 por defecto)
            # en vez de BASE_FLOTACION_MAX (50,000,000), para que este
            # conteo de diagnóstico coincida con el filtro que de verdad
            # determina el resultado final en filtrar_resultados().
            if ETAPA_PRUEBA_FILTROS >= 3 and float_shares is not None and float_shares > self.filtros_dueno.get("flotacion_max", 20_000_000):
                continue
            c["float_shares"] = float_shares
            c["float_status"] = entrada.get(
                "float_status",
                "pending" if not entrada else "no_data"
            )
            c["float_source"] = entrada.get("float_source", "")
            tras_float.append(c)
        self.n_tras_float = len(tras_float)

        enriquecidos = []
        for c in tras_float:
            if ETAPA_PRUEBA_FILTROS >= 2 and c.get("volumen_dia", 0) < self.filtros_dueno.get("volumen_min", 20_000):
                continue
            # El porcentaje de subida se representa directamente con cambio_pct.
            c["volumen_relativo"] = c["cambio_pct"]
            enriquecidos.append(c)

        tickers_enr = [c["ticker"] for c in enriquecidos]
        self._asegurar_tecnico(tickers_enr)
        con_noticia = self._noticias_recientes(tickers_enr)

        for c in enriquecidos:
            tech = self.cache_tecnico.get(c["ticker"], (0, False, False, False, False, None, None, None, 0, None, None, None, None, None, None))
            _, cruz_arriba, cruz_abajo, macd_pos, macd_neg, precio_tec, ema_tec, macd_tec, barras_tec, precio_prev_tec, ema_prev_tec, precio_actual_tec, ema_actual_tec, bb_upper_tec, bb_dist_tec = tech
            c["cruzando_ema20"] = cruz_arriba
            c["cruzando_ema20_abajo"] = cruz_abajo
            c["macd_positivo"] = macd_pos
            c["macd_negativo"] = macd_neg
            c["tecnico_precio"] = precio_tec
            c["tecnico_ema20"] = ema_tec
            c["tecnico_macd"] = macd_tec
            c["tecnico_barras"] = barras_tec
            c["tecnico_precio_anterior"] = precio_prev_tec
            c["tecnico_ema20_anterior"] = ema_prev_tec
            c["tecnico_precio_actual"] = precio_actual_tec
            c["tecnico_ema20_actual"] = ema_actual_tec
            c["bb_upper"] = bb_upper_tec
            c["bb_dist_pct"] = bb_dist_tec
            c["cruce_ema20_confirmado"] = bool(cruz_arriba and precio_prev_tec is not None and ema_prev_tec is not None)
            c["tiene_noticia"] = c["ticker"] in con_noticia

        # Diagnóstico del embudo: no cambia ningún filtro ni el resultado del scanner.
        ema_arriba_count = sum(1 for c in enriquecidos if c.get("cruzando_ema20"))
        macd_positivo_count = sum(1 for c in enriquecidos if c.get("macd_positivo"))
        ema_y_macd_count = sum(
            1 for c in enriquecidos
            if c.get("cruzando_ema20") and c.get("macd_positivo")
        )
        # El filtro de volumen ya se aplicó al construir 'enriquecidos', así
        # que ese conteo ES el resultado "tras volumen". El paso previo
        # (antes de aplicar volumen) queda guardado en self.n_tras_float.
        tras_vol_rel_count = len(enriquecidos)
        tecnicos_validos = sum(1 for c in enriquecidos if c.get("tecnico_barras", 0) >= 40)
        ema_calculable = sum(1 for c in enriquecidos if c.get("tecnico_ema20") is not None)
        macd_calculable = sum(1 for c in enriquecidos if c.get("tecnico_macd") is not None)
        tickers_enr_unicos = len({c.get("ticker") for c in enriquecidos})
        # Conteo bruto que cumple EMA20 + MACD antes del límite de presentación.
        # En PRUEBA 4 top_n=50, por lo que el resultado final podrá mostrar hasta 50.
        candidatos_ema_macd_brutos = sum(
            1 for c in enriquecidos
            if c.get("cruzando_ema20") and c.get("macd_positivo")
        )
        self.diagnostico_filtros = {
            "radar_base": radar_base_total,
            "enviados_tecnico": len(enriquecidos),
            "con_40_barras": tecnicos_validos,
            "ema_calculable": ema_calculable,
            "macd_calculable": macd_calculable,
            "tras_float": getattr(self, "n_tras_float", len(enriquecidos)),
            "tras_vol_rel": tras_vol_rel_count,
            "ema_arriba": ema_arriba_count,
            "macd_positivo": macd_positivo_count,
            "ema_y_macd": ema_y_macd_count,
            "candidatos_ema_macd_brutos": candidatos_ema_macd_brutos,
            "tickers_unicos": tickers_enr_unicos,
            "duplicados": len(enriquecidos) - tickers_enr_unicos,
            "resultados": len(filtrar_resultados(enriquecidos, self.filtros_dueno)),
            "gap_aplicado": ETAPA_PRUEBA_FILTROS >= 4,
            "gap_min": self.filtros_dueno.get("gap_min", BASE_GAP_MIN),
            "gap_max": self.filtros_dueno.get("gap_max", BASE_GAP_MAX),
        }

        # Guardamos una fotografía del resultado REAL de este ciclo antes de publicar
        # la lista nueva. Esto evita perder candidatos cuando desaparecen en el siguiente ciclo.
        p_hist = dict(self.filtros_dueno)
        p_hist.update({"cruce_ema": "Hacia arriba", "macd": "Positivo", "top_n": 50, "orden": "Actualizado"})
        resultados_finales_hist = filtrar_resultados(enriquecidos, p_hist)

        # PRUEBA 6: iniciar/actualizar observaciones posteriores a la señal.
        # Esto se ejecuta antes de publicar el resultado y no modifica ningún filtro.
        self._actualizar_prueba6(enriquecidos, snapshots)

        # PRUEBA 4B: conservar las dos listas del MISMO ciclo.
        candidatos_raw_actual = [c for c in enriquecidos if c.get("cruzando_ema20") and c.get("macd_positivo")]
        self.candidatos_ema_macd_actual = list(candidatos_raw_actual)
        self.finales_ema_macd_actual = list(resultados_finales_hist)
        raw_tickers = {c.get("ticker") for c in candidatos_raw_actual}
        final_tickers = {c.get("ticker") for c in resultados_finales_hist}
        eliminados_mismo_ciclo = sorted(raw_tickers - final_tickers)

        # PRUEBA 4C: comparar candidatos EMA20+MACD con el ciclo inmediatamente anterior.
        # Esto solo diagnostica entradas/salidas naturales entre ciclos; no cambia filtros.
        raw_anterior = set(getattr(self, "_raw_tickers_ciclo_anterior", set()))
        mantenidos_entre_ciclos = sorted(raw_tickers & raw_anterior)
        entraron_este_ciclo = sorted(raw_tickers - raw_anterior)
        salieron_este_ciclo = sorted(raw_anterior - raw_tickers)
        self._raw_tickers_ciclo_anterior = set(raw_tickers)

        self.diagnostico_filtros.update({
            "raw_tickers": sorted(x for x in raw_tickers if x),
            "raw_tickers_anterior": sorted(x for x in raw_anterior if x),
            "mantenidos_entre_ciclos": mantenidos_entre_ciclos,
            "entraron_este_ciclo": entraron_este_ciclo,
            "salieron_este_ciclo": salieron_este_ciclo,
            "final_tickers_mismo_ciclo": sorted(x for x in final_tickers if x),
            "eliminados_post_ema_macd": eliminados_mismo_ciclo,
            "eliminados_post_ema_macd_count": len(eliminados_mismo_ciclo),
        })
        self._registrar_historial_ciclo(enriquecidos, resultados_finales_hist)

        self.resultados = enriquecidos
        self.float_pendientes = sum(
            1 for c in enriquecidos
            if c.get("float_shares") is None and c.get("float_status") == "pending"
        )
        self.float_sin_dato = sum(
            1 for c in enriquecidos
            if c.get("float_shares") is None and c.get("float_status") == "no_data"
        )
        self.ultima_actualizacion = datetime.now(ET)
        self.duracion_ciclo = time.monotonic() - inicio
        self._registrar_eventos(enriquecidos)

        # 🛑 TELEGRAM APAGADO DURANTE LA DEPURACIÓN.
        # No se envía nada al grupo mientras comprobamos los filtros.
        p = dict(self.filtros_dueno)
        p.update({"cruce_ema": "Hacia arriba", "macd": "Positivo", "top_n": 50, "orden": "Actualizado"})
        top = filtrar_resultados(enriquecidos, p)
        if top:
            tabla = f"{'TICK':<5}|{'PRE':>5}|{'CHG%':>4}|{'VOL':>5}|{'FLT':>5}\n" + "-" * 28 + "\n"
            for c in top:
                nombre = f"🔥{c['ticker']}" if c["tiene_noticia"] else c["ticker"]
                tabla += (f"{nombre:<5}|{c['precio']:>5.2f}|{c['cambio_pct']:>3.0f}%|"
                          f"{formatear_numero_grande(c['volumen_dia']):>5}|{formatear_numero_grande(c['float_shares']):>5}\n")
            # Telegram permanece desactivado en las pruebas.
            # self._enviar_telegram(tabla)
            self._escribir_html(tabla)

    def _bucle(self):
        while not self._detener_hilo.is_set():
            inicio = time.monotonic()
            try:
                en_horario = self._esta_en_horario_automatico()
                if self.encendido and en_horario:
                    self._ciclo()
            except Exception as e:
                self.ultimo_error = f"Ciclo: {e}"
                print(f"⚠️ Error en escaneo: {e}")
            espera = max(1.0, INTERVALO_ESCANEO_SEGUNDOS - (time.monotonic() - inicio))
            # Event.wait permite interrumpir el descanso inmediatamente al reiniciar.
            self._detener_hilo.wait(timeout=espera)


@st.cache_resource
def obtener_servicio(api_key, secret_key, tg_token, tg_chat, fmp_api_key):
    print("⚙️ Iniciando el motor del scanner (una sola vez para todos los usuarios)...")
    return ServicioScanner(api_key, secret_key, tg_token, tg_chat, fmp_api_key, cargar_config())


servicio = obtener_servicio(
    st.secrets["ALPACA_API_KEY"],
    st.secrets["ALPACA_SECRET_KEY"],
    st.secrets.get("TELEGRAM_BOT_TOKEN", None),
    st.secrets.get("TELEGRAM_CHAT_ID", "-1004440734539"),
    st.secrets.get("FMP_API_KEY", None),
)

# ==========================================
# 🎨 ESTILO OSCURO
# ==========================================
st.markdown("""
<style>
    :root {
        --ts-bg: #030303;
        --ts-panel: #090909;
        --ts-panel-2: #0d0d0d;
        --ts-gold: #d4af37;
        --ts-gold-bright: #f2d675;
        --ts-gold-dark: #7d641c;
        --ts-text: #f3f3f3;
        --ts-muted: #9a9a9a;
        --ts-red: #d64545;
        --ts-green: #37c77a;
    }

    .stApp {
        background:
            radial-gradient(circle at 50% -10%, rgba(212,175,55,.09), transparent 34%),
            linear-gradient(180deg, #080808 0%, #030303 55%, #000000 100%) !important;
        color: var(--ts-text) !important;
    }
    [data-testid="stHeader"], [data-testid="stSidebar"] {
        background: #030303 !important;
    }
    .block-container {
        max-width: 1500px;
        padding-top: .45rem;
        padding-bottom: 1.5rem;
    }

    /* Paneles institucionales */
    div[data-testid="stVerticalBlockBorderWrapper"] {
        background: linear-gradient(180deg, #0c0c0c 0%, #070707 100%) !important;
        border: 1px solid rgba(212,175,55,.32) !important;
        border-radius: 7px !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,.025), 0 8px 30px rgba(0,0,0,.28) !important;
    }
    .simple-card {
        background: linear-gradient(180deg,#0c0c0c,#060606);
        border:1px solid rgba(212,175,55,.32);
        border-radius:7px;
        padding:8px 10px;
        margin-bottom:5px;
    }
    .simple-title {
        color: var(--ts-gold-bright);
        font-size:14px;
        font-weight:800;
        margin-bottom:4px;
        letter-spacing:.25px;
    }
    .simple-status {
        display:inline-block;
        padding:3px 7px;
        border:1px solid rgba(212,175,55,.45);
        border-radius:18px;
        color:var(--ts-gold-bright);
        background:#11100a;
        font-size:11px;
        margin-right:6px;
    }
    .small-note {
        background:#0e0d09;
        border:1px solid rgba(212,175,55,.28);
        border-radius:7px;
        padding:8px 10px;
        color:#d7d0bd;
        font-size:11px;
    }

    /* Controles */
    label, [data-testid="stWidgetLabel"] p {
        color:#d7d0bd !important;
        font-weight:700 !important;
        text-transform:none;
        font-size:11px !important;
    }
    div[data-testid="stNumberInput"] input,
    div[data-testid="stTextInput"] input,
    div[data-baseweb="select"] > div,
    div[data-testid="stTimeInput"] input {
        background:#101318 !important;
        color:#f1f1f1 !important;
        border-color:#3a4048 !important;
        box-shadow:none !important;
    }
    /* Controles planos: sin halo ni marco blanco alrededor */
    div[data-testid="stNumberInput"],
    div[data-testid="stTextInput"],
    div[data-testid="stTimeInput"],
    div[data-baseweb="select"],
    div[data-testid="stToggle"],
    div[data-testid="stNumberInput"] > div,
    div[data-testid="stTextInput"] > div,
    div[data-testid="stTimeInput"] > div {
        background:transparent !important;
        box-shadow:none !important;
        border:none !important;
        outline:none !important;
    }
    div[data-baseweb="select"] * { color:#f1f1f1 !important; box-shadow:none !important; }

    /* CORRECCIÓN DEFINITIVA: eliminar el marco/fondo blanco que Streamlit/BaseWeb
       agrega alrededor de los campos compactos. El fondo oscuro queda en el
       elemento que realmente contiene el valor, no en la envoltura blanca. */
    div[data-testid="stNumberInput"] > div,
    div[data-testid="stNumberInput"] > div > div,
    div[data-testid="stTextInput"] > div,
    div[data-testid="stTextInput"] > div > div,
    div[data-testid="stTimeInput"] > div,
    div[data-testid="stTimeInput"] > div > div,
    div[data-baseweb="input"],
    div[data-baseweb="input"] > div,
    div[data-baseweb="select"],
    div[data-baseweb="select"] > div,
    div[data-baseweb="select"] > div > div,
    div[data-baseweb="select"] [role="combobox"] {
        background:transparent !important;
        background-color:transparent !important;
        border-color:transparent !important;
        box-shadow:none !important;
        outline:none !important;
    }
    div[data-testid="stNumberInput"] input,
    div[data-testid="stTextInput"] input,
    div[data-testid="stTimeInput"] input,
    div[data-baseweb="input"] input,
    div[data-baseweb="select"] [role="combobox"] {
        background:#101318 !important;
        background-color:#101318 !important;
        color:#f1f1f1 !important;
        border:1px solid #3a4048 !important;
        box-shadow:none !important;
        outline:none !important;
    }
    div[data-testid="stNumberInput"] button,
    div[data-testid="stTimeInput"] button {
        background:#101318 !important;
        color:#f1f1f1 !important;
        border-color:#3a4048 !important;
        box-shadow:none !important;
    }
    div[data-testid="stNumberInput"] svg,
    div[data-testid="stTimeInput"] svg,
    div[data-baseweb="select"] svg {
        fill:#d7d0bd !important;
        color:#d7d0bd !important;
    }

    /* Desplegables legibles en móvil y escritorio: menú oscuro + texto claro.
       BaseWeb/Streamlit puede renderizar el menú fuera del contenedor del select,
       por eso estas reglas también cubren el popover/listbox. */
    [data-baseweb="popover"],
    [data-baseweb="menu"],
    [role="listbox"],
    ul[role="listbox"] {
        background:#101318 !important;
        color:#f1f1f1 !important;
        border:1px solid #3a4048 !important;
        box-shadow:none !important;
    }
    [data-baseweb="popover"] *,
    [data-baseweb="menu"] *,
    [role="listbox"] *,
    ul[role="listbox"] * {
        color:#f1f1f1 !important;
        background-color:transparent !important;
        text-shadow:none !important;
    }
    [role="option"] {
        color:#f1f1f1 !important;
        background:#101318 !important;
        font-size:11px !important;
        line-height:1.2 !important;
    }
    [role="option"]:hover,
    [role="option"][aria-selected="true"] {
        color:#ffffff !important;
        background:#243142 !important;
    }
    /* El valor seleccionado también debe conservar contraste cuando el campo es compacto. */
    div[data-baseweb="select"] [data-baseweb="select-value"],
    div[data-baseweb="select"] input,
    div[data-baseweb="select"] span {
        color:#f1f1f1 !important;
    }
    .stButton button {
        border-radius:6px !important;
        font-weight:800 !important;
        border:1px solid rgba(212,175,55,.55) !important;
        background:#11100c !important;
        color:var(--ts-gold-bright) !important;
        box-shadow:none !important;
        outline:none !important;
    }
    .stButton button:hover {
        border-color:var(--ts-gold-bright) !important;
        box-shadow:none !important;
    }
    [data-testid="stMetricValue"] { color:var(--ts-gold-bright) !important; }

    /* Alertas */
    [data-testid="stAlert"] {
        background:#0c0b08 !important;
        border-color:rgba(212,175,55,.34) !important;
        color:#ddd6c5 !important;
    }

    /* Dataframe */
    [data-testid="stDataFrame"] {
        border:1px solid rgba(212,175,55,.28) !important;
        border-radius:7px !important;
        overflow:hidden !important;
    }

    [data-testid="stDataFrame"] {
        border:1px solid #303640 !important;
        border-radius:5px !important;
        overflow:hidden !important;
        background:#101318 !important;
    }
    [data-testid="stDataFrame"] iframe {
        background:#101318 !important;
    }

    /* Compacto tipo Finviz: poco espacio vertical y texto pequeño, pero legible. */
    div[data-testid="stVerticalBlockBorderWrapper"] > div {
        gap: .20rem !important;
    }
    div[data-testid="stHorizontalBlock"] {
        gap:.18rem !important;
        margin-bottom:1px !important;
    }
    div[data-testid="stNumberInput"],
    div[data-testid="stTextInput"],
    div[data-testid="stTimeInput"],
    div[data-baseweb="select"] {
        margin-bottom:0 !important;
    }

    .inline-field-label {
        color:#c9c9c9 !important;
        font-size:10px !important;
        font-weight:700 !important;
        line-height:1.05 !important;
        min-height:28px !important;
        display:flex !important;
        align-items:center !important;
        white-space:nowrap !important;
    }
    .inline-toggle-label {
        color:#c9c9c9 !important;
        font-size:9px !important;
        font-weight:700 !important;
        line-height:1 !important;
        margin-bottom:0 !important;
        white-space:nowrap !important;
    }
    .inline-field-label + div { margin:0 !important; }
    div[data-testid="stNumberInput"] input,
    div[data-testid="stTextInput"] input {
        width:50% !important;
        max-width:72px !important;
        min-width:42px !important;
        height:25px !important;
        min-height:25px !important;
        padding:2px 5px !important;
        font-size:10px !important;
        box-shadow:none !important;
        outline:none !important;
    }
    div[data-baseweb="select"] {
        width:50% !important;
        max-width:105px !important;
        min-width:60px !important;
    }
    div[data-baseweb="select"] {
        min-height:28px !important;
        height:28px !important;
    }
    div[data-baseweb="select"] > div {
        min-height:25px !important;
        height:25px !important;
        font-size:9px !important;
    }
    @media (max-width: 900px) {
    }
    @media (max-width: 640px) {
        .block-container {
            max-width:100% !important;
            padding-left:.18rem !important;
            padding-right:.18rem !important;
            padding-top:.18rem !important;
        }
        .simple-title { font-size:12px !important; }
        .small-note { font-size:9px !important; }
        div[data-testid="stHorizontalBlock"] {
            gap:.18rem !important;
            flex-wrap:nowrap !important;
            align-items:flex-end !important;
        }
        div[data-testid="stHorizontalBlock"] > div {
            min-width:0 !important;
        }
        div[data-testid="stNumberInput"],
        div[data-testid="stTextInput"],
        div[data-baseweb="select"],
        div[data-testid="stToggle"] {
            min-width:0 !important;
        }
        div[data-testid="stNumberInput"] input,
        div[data-testid="stTextInput"] input {
            width:50% !important;
            max-width:48px !important;
            min-width:30px !important;
            font-size:7px !important;
            min-height:21px !important;
            height:21px !important;
            padding-left:2px !important;
            padding-right:2px !important;
            box-shadow:none !important;
        }
        div[data-baseweb="select"] {
            width:50% !important;
            max-width:70px !important;
            min-width:42px !important;
        }
        div[data-baseweb="select"] > div {
            font-size:7px !important;
            min-height:21px !important;
            height:21px !important;
            padding-left:2px !important;
            padding-right:2px !important;
        }
        [role="option"],
        [data-baseweb="menu"] * {
            font-size:9px !important;
            line-height:1.15 !important;
        }
        label, [data-testid="stWidgetLabel"] p {
            font-size:6.5px !important;
            line-height:1 !important;
        }
        .stButton button {
            font-size:7px !important;
            min-height:22px !important;
            height:24px !important;
            padding:1px 4px !important;
            white-space:nowrap !important;
        }
        div[data-testid="stVerticalBlockBorderWrapper"] {
            padding:3px !important;
        }
        [data-testid="stAlert"] {
            padding:3px 6px !important;
            margin:2px 0 !important;
            font-size:8px !important;
        }
        .simple-title { margin-bottom:2px !important; }
        div[data-testid="stHorizontalBlock"] { margin-bottom:1px !important; }
        [data-testid="stDataFrame"] {
            border-radius:4px !important;
        }
        [data-testid="stDataFrame"] {
            border-radius:4px !important;
            font-size:8px !important;
        }
        div[data-testid="stVerticalBlockBorderWrapper"] {
            padding:2px !important;
        }
        .simple-title {
            font-size:10px !important;
            line-height:1 !important;
        }
        .small-note {
            padding:3px 5px !important;
            line-height:1.05 !important;
        }
    }
</style>
""", unsafe_allow_html=True)


# ==========================================
# 🖥️ DASHBOARD SUPERIOR — INTERFAZ SIMPLE
# ==========================================
try:
    servicio._esta_en_horario_automatico()
except Exception:
    pass
_hora_txt = f"{servicio.hora_inicio_auto_min//60:02d}:{servicio.hora_inicio_auto_min%60:02d} - {servicio.hora_fin_auto_min//60:02d}:{servicio.hora_fin_auto_min%60:02d} ET"
_dia_txt = "Sí (Alpaca)" if datetime.now(ET).date() in servicio.dias_mercado_cache else "No / fuera de mercado"
_estado_txt = "Scanner Activo" if servicio.encendido and servicio.auto_en_horario else ("Scanner Apagado" if not servicio.encendido else "Scanner En espera")

COLORES_LAYOUT = [
    ("Rojo", "#e53935", "#ffffff"), ("Naranja", "#fb8c00", "#000000"),
    ("Amarillo", "#fdd835", "#000000"), ("Verde", "#43a047", "#ffffff"),
    ("Turquesa", "#00acc1", "#ffffff"), ("Azul", "#1e88e5", "#ffffff"),
    ("Morado", "#8e24aa", "#ffffff"), ("Rosa", "#ec407a", "#ffffff"),
    ("Marrón", "#8d6e63", "#ffffff"), ("Gris", "#9e9e9e", "#000000"),
]
COLORES_LAYOUT_DEFECTO = COLORES_LAYOUT.copy()

# =========================================================
# 🧭 PANEL PRINCIPAL — diseño compacto tipo dashboard
# =========================================================
# 1) Preferencias + control + conexión en una sola fila.
# Los colores ya NO ocupan una columna lateral grande.
# Campos compactos tipo Finviz: etiqueta a la izquierda + control corto a la derecha.
def _campo_inline_num(parent, etiqueta, **kwargs):
    with parent:
        lab, box = st.columns([1.25, 0.75], gap="small")
        with lab:
            st.markdown(f'<div class="inline-field-label">{etiqueta}</div>', unsafe_allow_html=True)
        with box:
            return st.number_input("", label_visibility="collapsed", **kwargs)

def _campo_inline_select(parent, etiqueta, **kwargs):
    with parent:
        lab, box = st.columns([1.25, 0.75], gap="small")
        with lab:
            st.markdown(f'<div class="inline-field-label">{etiqueta}</div>', unsafe_allow_html=True)
        with box:
            return st.selectbox("", label_visibility="collapsed", **kwargs)

def _campo_inline_text(parent, etiqueta, **kwargs):
    with parent:
        lab, box = st.columns([1.25, 0.75], gap="small")
        with lab:
            st.markdown(f'<div class="inline-field-label">{etiqueta}</div>', unsafe_allow_html=True)
        with box:
            return st.text_input("", label_visibility="collapsed", **kwargs)

with st.container(border=True):
    st.markdown('<div class="simple-title">🔎 Preferencias de búsqueda</div>', unsafe_allow_html=True)
    cfg = cargar_config()

    if ETAPA_PRUEBA_FILTROS == 1:
        r1 = st.columns(4, gap="small")
        PRECIO_MIN = _campo_inline_num(r1[0], "Precio mín.", value=float(cfg["precio_min"]), step=0.5, key="f_pmin")
        PRECIO_MAX = _campo_inline_num(r1[1], "Precio máx.", value=float(cfg["precio_max"]), step=0.5, key="f_pmax")
        CRUCE_EMA = "Hacia arriba"
        _campo_inline_text(r1[2], "EMA20", value="Precio por encima", disabled=True, key="f_ema_prueba1")
        MACD_MODO = "Positivo"
        _campo_inline_text(r1[3], "MACD", value="Positivo", disabled=True, key="f_macd_prueba1")

        r2 = st.columns(4, gap="small")
        REFRESCO = _campo_inline_num(r2[0], "Refresco", value=int(cfg["intervalo_refresco"]), min_value=1, step=1, key="f_ref")
        ORDEN = _campo_inline_select(r2[1], "Ordenar", options=["Actualizado", "Cambio %", "Volumen"], key="f_orden")
        TOP_N = _campo_inline_num(r2[2], "Top N", value=50, min_value=1, max_value=100, key="f_top")
        with r2[3]:
            st.markdown('<div class="inline-toggle-label">Auto</div>', unsafe_allow_html=True)
            AUTO_ON = st.toggle("", value=True, label_visibility="collapsed", key="f_auto")

        GAP_MIN = float(cfg["gap_min"])
        GAP_MAX = float(cfg["gap_max"])
        FLOT_MAX = int(cfg["flotacion_max"])
        VOLUMEN_MIN = int(cfg["volumen_min"])
    else:
        # Cada campo conserva el estilo compacto de Finviz: título a la izquierda y caja corta a la derecha.
        r1 = st.columns(4, gap="small")
        PRECIO_MIN = _campo_inline_num(r1[0], "Precio mín.", value=float(cfg["precio_min"]), step=0.5, key="f_pmin")
        PRECIO_MAX = _campo_inline_num(r1[1], "Precio máx.", value=float(cfg["precio_max"]), step=0.5, key="f_pmax")
        GAP_MIN = _campo_inline_num(r1[2], "Gap mín.", value=float(cfg["gap_min"]), step=1.0, key="f_gmin")
        GAP_MAX = _campo_inline_num(r1[3], "Gap máx.", value=float(cfg["gap_max"]), step=10.0, key="f_gmax")

        r2 = st.columns(4, gap="small")
        FLOT_MAX = _campo_inline_num(r2[0], "Flotación", value=int(cfg["flotacion_max"]), step=1_000_000, key="f_flt")
        VOLUMEN_MIN = _campo_inline_num(r2[1], "Volumen mín.", value=int(cfg["volumen_min"]), min_value=0, step=1000, key="f_vmin")
        REFRESCO = _campo_inline_num(r2[2], "Refresco", value=int(cfg["intervalo_refresco"]), min_value=1, step=1, key="f_ref")
        TOP_N = _campo_inline_num(r2[3], "Top N", value=50, min_value=1, max_value=100, key="f_top")

        r3 = st.columns(4, gap="small")
        CRUCE_EMA = _campo_inline_select(r3[0], "Cruce EMA20", options=OPCIONES_CRUCE_EMA, index=0, key="f_cruce_ema")
        MACD_MODO = _campo_inline_select(r3[1], "MACD", options=OPCIONES_MACD, index=0, key="f_macd_modo")
        ORDEN = _campo_inline_select(r3[2], "Ordenar", options=["Actualizado", "Cambio %", "Volumen"], key="f_orden")
        with r3[3]:
            st.markdown('<div class="inline-toggle-label">Actualización</div>', unsafe_allow_html=True)
            AUTO_ON = st.toggle("", value=True, label_visibility="collapsed", key="f_auto")

params = {
    "precio_min": PRECIO_MIN, "precio_max": PRECIO_MAX, "gap_min": GAP_MIN, "gap_max": GAP_MAX,
    "flotacion_max": FLOT_MAX, "volumen_min": VOLUMEN_MIN, "cruce_ema": CRUCE_EMA, "macd": MACD_MODO,
    "orden": ORDEN, "top_n": TOP_N,
}

if ETAPA_PRUEBA_FILTROS == 1:
    st.warning("🧪 PRUEBA 1 REAL: solo Precio + EMA20 (precio por encima) + MACD positivo. Subida, Gap, Float, Volumen, RVOL y Telegram están DESACTIVADOS.")
elif ETAPA_PRUEBA_FILTROS == 2:
    st.info("🧪 PRUEBA 2: Precio + Volumen + EMA20 + MACD.")
elif ETAPA_PRUEBA_FILTROS == 3:
    st.info("🧪 PRUEBA 3: Precio + Volumen + Float + EMA20 + MACD.")
elif ETAPA_PRUEBA_FILTROS >= 4:
    st.info("🧪 PRUEBA 4: Precio + Subida + Volumen + Float + EMA20 + MACD.")

st.success("🚀 PRUEBA 7 ACTIVA: mide si cada señal alcanza +0.25%, +0.50% o +1.00% dentro de la ventana de 10 minutos. Los filtros de entrada no cambian.")

# 2) Control del scanner + conexión API/broker
# Los controles internos de operación y las credenciales del broker
# solo se muestran al administrador. El usuario mantiene únicamente
# el acceso al scanner y sus filtros de búsqueda.
if ES_ADMIN:
    control_col, broker_col, premium_col = st.columns([1.15, 1.55, 0.72], gap="small")
else:
    control_col = st.container()
    broker_col = None
    premium_col = None

with control_col:
    with st.container(border=True):
        st.markdown('<div class="simple-title">⚙️ Control del Scanner</div>', unsafe_allow_html=True)
        if ES_ADMIN:
            # Mandos principales en una sola línea para que el panel sea compacto.
            b1,b2,b3 = st.columns(3, gap="small")
            with b1:
                if st.button("🟢 ENCENDER", key="encender_scanner_dashboard", width="stretch"):
                    servicio.encendido = True
                    servicio.ultimo_error = None
                    st.rerun()
            with b2:
                if st.button("🔴 APAGAR", key="apagar_scanner_dashboard", width="stretch"):
                    servicio.encendido = False
                    servicio.auto_en_horario = False
                    st.rerun()
            with b3:
                if st.button("🔄 REINICIAR", key="reiniciar_scanner_dashboard", width="stretch"):
                    servicio.reiniciar_scanner()
                    st.success("Scanner reiniciado. El motor fue reconstruido correctamente.")
                    st.rerun()
            st.markdown("**Horario de funcionamiento (ET)**")
            h1,h2,h3 = st.columns([1,1,1], gap="small")
            with h1:
                hora_inicio_ui = st.time_input(
                    "Inicio",
                    value=dt_time(servicio.hora_inicio_auto_min // 60, servicio.hora_inicio_auto_min % 60),
                    key="hora_inicio_scanner_dashboard",
                )
            with h2:
                hora_fin_ui = st.time_input(
                    "Cierre",
                    value=dt_time(servicio.hora_fin_auto_min // 60, servicio.hora_fin_auto_min % 60),
                    key="hora_fin_scanner_dashboard",
                )
            with h3:
                st.write("")
                if st.button("💾 GUARDAR HORARIO", key="guardar_horario_dashboard", width="stretch"):
                    servicio.configurar_horario(hora_inicio_ui, hora_fin_ui)
                    st.rerun()
            st.markdown(
                f'<div class="small-note">Estado: <b>{_estado_txt}</b><br>Horario: <b>{servicio.hora_inicio_auto_min//60:02d}:{servicio.hora_inicio_auto_min%60:02d} - {servicio.hora_fin_auto_min//60:02d}:{servicio.hora_fin_auto_min%60:02d} ET</b></div>',
                unsafe_allow_html=True,
            )
        else:
            st.info("Modo usuario. El encendido/apagado y el horario solo los puede modificar el administrador.")

# Espacios fijos de renderizado: el cuadro principal de activos aparece AQUÍ,
# inmediatamente debajo de los mandos. Se rellena más abajo, después de definir
# la función, para conservar este orden visual sin mover la lógica del motor.
panel_resultados_slot = st.empty()
panel_broker_slot = st.empty()

if ES_ADMIN:
    with broker_col:
        with st.container(border=True):
            st.markdown('<div class="simple-title">🔗 Conexión API / Broker <span style="font-size:10px;background:#123d67;border-radius:12px;padding:4px 8px;">Opcional</span></div>', unsafe_allow_html=True)
            api1,api2,api3 = st.columns(3, gap="small")
            brokers_ui = ["Interactive Brokers (TWS)", "TradeZero (webhook)", "Binance (webhook)", "Quantfury (portapapeles)", "Otro (webhook)"]
            with api1:
                st.session_state.setdefault("bk_nombre", brokers_ui[0])
                _idx_b = brokers_ui.index(st.session_state["bk_nombre"]) if st.session_state["bk_nombre"] in brokers_ui else 0
                st.session_state["bk_nombre"] = st.selectbox("Broker", brokers_ui, index=_idx_b, key="bk_nombre_ui_dashboard")
            with api2:
                st.session_state.setdefault("bk_api_key", "")
                st.session_state["bk_api_key"] = st.text_input("API Key", value=st.session_state.get("bk_api_key", ""), type="password", key="bk_api_key_ui_dashboard")
            with api3:
                st.session_state.setdefault("bk_api_secret", "")
                st.session_state["bk_api_secret"] = st.text_input("Secret Key", value=st.session_state.get("bk_api_secret", ""), type="password", key="bk_api_secret_ui_dashboard")
            st.session_state.setdefault("bk_puente", "http://127.0.0.1:8765/enviar")
            st.session_state["bk_puente"] = st.text_input(
                "Puente / URL para enviar el símbolo al layout del broker",
                value=st.session_state.get("bk_puente", "http://127.0.0.1:8765/enviar"),
                key="bk_puente_ui_dashboard",
            )
            api_a, api_b = st.columns(2, gap="small")
            with api_a:
                st.toggle("Usar API del broker", value=bool(st.session_state.get("bk_api_key")), key="usar_api_broker_dashboard")
            with api_b:
                if st.button("🔌 Probar conexión", key="probar_broker_dashboard", width="stretch"):
                    st.info("La conexión se realizará mediante el puente/webhook configurado.")

    with premium_col:
        with st.container(border=True):
            st.markdown('<div class="simple-title">🔐 Modalidades</div>', unsafe_allow_html=True)
            st.markdown('<div class="small-note"><b>🟢 Web</b><br>Scanner en la nube.</div>', unsafe_allow_html=True)
            st.markdown('<div class="small-note"><b>🔵 API</b><br>Integración con broker.</div>', unsafe_allow_html=True)
            st.markdown('<div class="small-note"><b>🟡 Suscripción</b><br>Modalidad comercial.</div>', unsafe_allow_html=True)

# 3) Colores: accesibles, pero sin el bloque vertical gigante de la izquierda.
if ES_ADMIN:
    with st.expander("🎨 Configurar colores y layouts del broker", expanded=False):
        st.caption("Los colores representan los 10 layouts. Puedes cambiarlos sin ocupar espacio en la tabla principal.")
        _colores_nuevos = list(st.session_state.get("bk_colores", [bg for _n,bg,_fg in COLORES_LAYOUT_DEFECTO]))
        while len(_colores_nuevos) < len(COLORES_LAYOUT_DEFECTO):
            _colores_nuevos.append(COLORES_LAYOUT_DEFECTO[len(_colores_nuevos)][1])
        color_cols = st.columns(5, gap="small")
        for idx,(nombre,bg,_fg) in enumerate(COLORES_LAYOUT_DEFECTO):
            with color_cols[idx % 5]:
                _colores_nuevos[idx] = st.color_picker(
                    f"L{idx+1} · {nombre}",
                    _colores_nuevos[idx],
                    key=f"bk_color_dashboard_{idx}",
                )
        st.session_state["bk_colores"] = _colores_nuevos

        st.markdown(
            '<div class="small-note">⚙️ El enlace 🔗 del panel de layouts permite vincular un activo con su ventana correspondiente del broker mediante puente o webhook.</div>',
            unsafe_allow_html=True,
        )

# ==========================================
# 📊 ESTADO DEL MOTOR
# ==========================================
# El aviso de float se oculta durante la PRUEBA 1 porque Float está
# realmente desactivado y no debe contaminar el diagnóstico visual.
if ETAPA_PRUEBA_FILTROS != 1 and (getattr(servicio, "float_pendientes", 0) or getattr(servicio, "float_sin_dato", 0)):
    partes_float = []
    if servicio.float_pendientes:
        partes_float.append(f"{servicio.float_pendientes} con float pendiente")
    if servicio.float_sin_dato:
        partes_float.append(f"{servicio.float_sin_dato} sin float disponible")
    st.warning("⚠️ Float: " + " · ".join(partes_float))

@st.fragment(run_every=(f"{int(REFRESCO)}s" if AUTO_ON else None))
def panel_diagnostico_filtros():
    if ES_ADMIN and getattr(servicio, "diagnostico_filtros", None):
        d = servicio.diagnostico_filtros
        with st.expander("🔎 Diagnóstico de filtros (prueba)", expanded=True):
            st.caption("Este panel es temporal y solo informa dónde se reducen los candidatos. No modifica el scanner.")
            if ETAPA_PRUEBA_FILTROS == 1:
                st.markdown(
                    f"**Radar base:** {d.get('radar_base', 0)} → "
                    f"**enviados a técnico:** {d.get('enviados_tecnico', 0)} → "
                    f"**con ≥40 barras:** {d.get('con_40_barras', 0)} → "
                    f"**EMA20 calculable:** {d.get('ema_calculable', 0)} → "
                    f"**Precio > EMA20:** {d.get('ema_arriba', 0)} → "
                    f"**MACD calculable:** {d.get('macd_calculable', 0)} → "
                    f"**MACD positivo:** {d.get('macd_positivo', 0)} → "
                    f"**EMA20 + MACD:** {d.get('ema_y_macd', 0)} → "
                    f"**candidatos EMA20+MACD (brutos):** {d.get('candidatos_ema_macd_brutos', d.get('ema_y_macd', 0))} → "
                    f"**resultado final:** {d.get('resultados', 0)}"
                )
                st.caption(f"Tickers únicos en técnico: {d.get('tickers_unicos', 0)} · duplicados detectados: {d.get('duplicados', 0)}")
                muestra = [c for c in servicio.resultados if c.get('cruzando_ema20') and c.get('macd_positivo')]
                if muestra:
                    df_tec = pd.DataFrame([{
                        "Ticker": c["ticker"],
                        "Barras": c.get("tecnico_barras", 0),
                        "Precio": round(c.get("tecnico_precio") or c["precio"], 4),
                        "EMA20": round(c.get("tecnico_ema20"), 4) if c.get("tecnico_ema20") is not None else None,
                        "MACD": round(c.get("tecnico_macd"), 6) if c.get("tecnico_macd") is not None else None,
                    } for c in muestra])
                    st.dataframe(df_tec, hide_index=True, width="stretch")
            else:
                st.markdown(
                    f"**Radar base:** {d.get('radar_base', 0)} → "
                    f"**flotación ≤ {formatear_numero_grande(servicio.filtros_dueno.get('flotacion_max', 20_000_000))}:** {d.get('tras_float', 0)} → "
                    f"**volumen ≥ {formatear_numero_grande(servicio.filtros_dueno.get('volumen_min', 20_000))} títulos:** {d.get('tras_vol_rel', 0)} → "
                    f"**EMA20 arriba:** {d.get('ema_arriba', 0)} → "
                    f"**MACD positivo:** {d.get('macd_positivo', 0)} → "
                    f"**EMA20 + MACD:** {d.get('ema_y_macd', 0)} → "
                    f"**candidatos EMA20+MACD (brutos):** {d.get('candidatos_ema_macd_brutos', d.get('ema_y_macd', 0))} → "
                    f"**resultado final:** {d.get('resultados', 0)}"
                )
                if not d.get('gap_aplicado', ETAPA_PRUEBA_FILTROS >= 4):
                    st.info("ℹ️ PRUEBA actual: el filtro de subida mínima no se está aplicando al embudo (ETAPA_PRUEBA_FILTROS < 4). La etiqueta 'subida ≥ 3%' describe el radar base, pero no elimina candidatos en esta prueba.")

                muestra = list(getattr(servicio, "candidatos_ema_macd_actual", []))
                if muestra:
                    final_tickers = set(getattr(servicio, "diagnostico_filtros", {}).get("final_tickers_mismo_ciclo", []))
                    df_diag = pd.DataFrame([{
                        "Ticker": c["ticker"],
                        "Precio ant.": round(c.get("tecnico_precio_anterior"), 4) if c.get("tecnico_precio_anterior") is not None else None,
                        "EMA20 ant.": round(c.get("tecnico_ema20_anterior"), 4) if c.get("tecnico_ema20_anterior") is not None else None,
                        "Precio actual": round(c.get("tecnico_precio_actual"), 4) if c.get("tecnico_precio_actual") is not None else None,
                        "EMA20 actual": round(c.get("tecnico_ema20_actual"), 4) if c.get("tecnico_ema20_actual") is not None else None,
                        "Cruce EMA20": "✅ SÍ" if c.get("cruce_ema20_confirmado") else "❌ NO",
                        "MACD": round(c.get("tecnico_macd"), 6) if c.get("tecnico_macd") is not None else None,
                        "BB superior": round(c.get("bb_upper"), 4) if c.get("bb_upper") is not None else None,
                        "Dist. BB %": round(c.get("bb_dist_pct"), 2) if c.get("bb_dist_pct") is not None else None,
                        "Barras": c.get("tecnico_barras", 0),
                    } for c in muestra])
                    st.caption(f"PRUEBA 4A · {len(muestra)} candidatos brutos del MISMO ciclo · se muestran todos, sin límite Top N.")
                    st.dataframe(df_diag, hide_index=True, width="stretch")

                    # PRUEBA 5: bloque de texto simple para copiar desde el teléfono.
                    hora_ciclo = (servicio.ultima_actualizacion.strftime("%H:%M:%S")
                                  if getattr(servicio, "ultima_actualizacion", None) else "--:--:--")
                    lineas_p5 = [
                        "PRUEBA 5",
                        f"CICLO: {hora_ciclo}",
                        f"EMA20+MACD: {len(muestra)}",
                        f"FINAL: {len(final_tickers)}",
                        "",
                        "Ticker | Precio | BB superior | Dist.BB%",
                    ]
                    for c in sorted(muestra, key=lambda x: str(x.get("ticker", ""))):
                        precio_p5 = c.get("tecnico_precio_actual")
                        if precio_p5 is None:
                            precio_p5 = c.get("tecnico_precio") or c.get("precio")
                        bb_p5 = c.get("bb_upper")
                        dist_p5 = c.get("bb_dist_pct")
                        lineas_p5.append(
                            f"{c.get('ticker','')} | "
                            f"{precio_p5:.4f} | " if isinstance(precio_p5, (int, float)) else f"{c.get('ticker','')} | {precio_p5} | "
                            + (f"{bb_p5:.4f} | " if isinstance(bb_p5, (int, float)) else f"{bb_p5} | ")
                            + (f"{dist_p5:.2f}" if isinstance(dist_p5, (int, float)) else str(dist_p5))
                        )
                    st.text_area(
                        "📋 PRUEBA 5 — copia este bloque completo y pégamelo aquí",
                        value="\n".join(lineas_p5),
                        height=min(500, max(180, 105 + 24 * len(muestra))),
                        key="prueba5_copiar",
                    )

                    # PRUEBA 7: alcanzabilidad de objetivos sobre la misma señal, ventana fija.
                    completadas_p7 = list(getattr(servicio, "prueba6_completadas", []))
                    activas_p7 = list(getattr(servicio, "prueba6_activos", {}).values())
                    st.caption(
                        f"PRUEBA 7 · objetivos +0.25%, +0.50% y +1.00% durante {VENTANA_PRUEBA6_MINUTOS} minutos · "
                        f"completadas: {len(completadas_p7)} · en observación: {len(activas_p7)} · entrada sin cambios."
                    )
                    if completadas_p7:
                        df_p7 = pd.DataFrame([{
                            "Ticker": x.get("ticker"),
                            "Hora señal": x.get("inicio_hora"),
                            "Dist.BB inicial %": round(x.get("bb_dist_inicial"), 2) if x.get("bb_dist_inicial") is not None else None,
                            "MFE %": round(x.get("mfe_pct"), 2) if x.get("mfe_pct") is not None else None,
                            "≥0.25%": "✅" if x.get("alcanza_025") else "❌",
                            "≥0.50%": "✅" if x.get("alcanza_050") else "❌",
                            "≥1.00%": "✅" if x.get("alcanza_100") else "❌",
                            "Min a 0.50%": round(x.get("tiempo_050_min"), 2) if x.get("tiempo_050_min") is not None else None,
                        } for x in completadas_p7])
                        st.dataframe(df_p7, hide_index=True, width="stretch")

                        n = len(completadas_p7)
                        n025 = sum(bool(x.get("alcanza_025")) for x in completadas_p7)
                        n050 = sum(bool(x.get("alcanza_050")) for x in completadas_p7)
                        n100 = sum(bool(x.get("alcanza_100")) for x in completadas_p7)
                        pos = sum((x.get("mfe_pct") is not None and x.get("mfe_pct") > 0) for x in completadas_p7)
                        neg = sum((x.get("mfe_pct") is not None and x.get("mfe_pct") < 0) for x in completadas_p7)
                        cero = n - pos - neg
                        c1,c2,c3,c4,c5 = st.columns(5)
                        c1.metric("Completadas", n)
                        c2.metric("≥ +0.25%", f"{n025} ({n025/n*100:.1f}%)" if n else "0")
                        c3.metric("≥ +0.50%", f"{n050} ({n050/n*100:.1f}%)" if n else "0")
                        c4.metric("≥ +1.00%", f"{n100} ({n100/n*100:.1f}%)" if n else "0")
                        c5.metric("Pos / 0 / Neg", f"{pos} / {cero} / {neg}")

                    lineas_p7 = [
                        "PRUEBA 7",
                        f"VENTANA: {VENTANA_PRUEBA6_MINUTOS} MINUTOS",
                        f"COMPLETADAS: {len(completadas_p7)}",
                        f"EN OBSERVACIÓN: {len(activas_p7)}",
                        "",
                        "Ticker | Dist.BB inicial | MFE% | ≥0.25% | ≥0.50% | ≥1.00% | Min a 0.50%",
                    ]
                    for x in sorted(completadas_p7, key=lambda z: str(z.get("inicio_hora", "")), reverse=True):
                        vals = [
                            x.get("ticker", ""),
                            f"{x.get('bb_dist_inicial'):.2f}" if isinstance(x.get('bb_dist_inicial'), (int, float)) else "",
                            f"{x.get('mfe_pct'):.2f}" if isinstance(x.get('mfe_pct'), (int, float)) else "",
                            "SI" if x.get("alcanza_025") else "NO",
                            "SI" if x.get("alcanza_050") else "NO",
                            "SI" if x.get("alcanza_100") else "NO",
                            f"{x.get('tiempo_050_min'):.2f}" if isinstance(x.get('tiempo_050_min'), (int, float)) else "",
                        ]
                        lineas_p7.append(" | ".join(vals))
                    st.text_area(
                        "📋 PRUEBA 7 — copia las operaciones COMPLETADAS",
                        value="\n".join(lineas_p7),
                        height=min(600, max(180, 125 + 24 * max(1, len(completadas_p7)))),
                        key="prueba7_copiar",
                    )

                    df_4b = pd.DataFrame([{
                        "Ticker": c.get("ticker"),
                        "EMA20+MACD": "✅ SÍ",
                        "Final mismo ciclo": "✅ SÍ" if c.get("ticker") in final_tickers else "❌ NO",
                        "Estado": "Se mantiene" if c.get("ticker") in final_tickers else "ELIMINADO DESPUÉS",
                    } for c in muestra])
                    st.caption("PRUEBA 4B · mismo ciclo: distingue una eliminación real de un cambio natural entre ciclos.")
                    st.dataframe(df_4b, hide_index=True, width="stretch")
                    eliminados = getattr(servicio, "diagnostico_filtros", {}).get("eliminados_post_ema_macd", [])
                    if eliminados:
                        st.warning("⚠️ Eliminados después de EMA20+MACD en ESTE MISMO ciclo: " + ", ".join(eliminados))
                    else:
                        st.success("✅ PRUEBA 4B: ningún candidato EMA20+MACD fue eliminado después en este mismo ciclo.")

                    # PRUEBA 4C: estabilidad entre ciclos consecutivos.
                    mantenidos = d.get("mantenidos_entre_ciclos", [])
                    entraron = d.get("entraron_este_ciclo", [])
                    salieron = d.get("salieron_este_ciclo", [])
                    st.caption("PRUEBA 4C · compara EMA20+MACD del ciclo actual contra el ciclo inmediatamente anterior.")
                    if not d.get("raw_tickers_anterior"):
                        st.info("ℹ️ PRUEBA 4C: esperando un ciclo anterior comparable.")
                    else:
                        st.markdown(
                            f"**Se mantienen:** {len(mantenidos)} · **entraron ahora:** {len(entraron)} · **salieron ahora:** {len(salieron)}"
                        )
                        df_4c = pd.DataFrame([{
                            "Ticker": t,
                            "Estado 4C": "↔️ Se mantiene" if t in mantenidos else "🟢 Entró ahora"
                        } for t in sorted(set(mantenidos + entraron))] + [{
                            "Ticker": t,
                            "Estado 4C": "🔴 Salió respecto al ciclo anterior"
                        } for t in salieron])
                        if not df_4c.empty:
                            st.dataframe(df_4c, hide_index=True, width="stretch")

                if getattr(servicio, "historial_ciclos", None):
                    st.markdown("**Historial de los últimos ciclos**")
                    filas_hist = []
                    for h in servicio.historial_ciclos:
                        filas_hist.append({
                            "Hora": h.get("hora"),
                            "Radar": h.get("radar_base", 0),
                            "Volumen": h.get("tras_volumen", 0),
                            "EMA20": h.get("ema_arriba", 0),
                            "MACD+": h.get("macd_positivo", 0),
                            "EMA20+MACD": h.get("ema_y_macd", 0),
                            "Candidatos": ", ".join(x.get("ticker", "") for x in h.get("finales", [])) or "—",
                        })
                    st.dataframe(pd.DataFrame(filas_hist), hide_index=True, width="stretch")

# ==========================================
# 🖥️ TABLA DE RESULTADOS (se refresca sola sin recargar la página)
# ==========================================
def color_cambio(val):
    try:
        v = float(val)
    except (TypeError, ValueError):
        return ""
    return f"color: {'#2ecc71' if v >= 0 else '#e74c3c'}; font-weight: 700"


@st.fragment(run_every=(f"{int(REFRESCO)}s" if AUTO_ON else None))
def panel_resultados():
    filas = filtrar_resultados(list(servicio.resultados), params)

    if servicio.ultima_actualizacion:
        if ETAPA_PRUEBA_FILTROS == 1:
            detalle = (f"Última actualización: {servicio.ultima_actualizacion.strftime('%H:%M:%S')} ET"
                       f" · ciclo {servicio.duracion_ciclo:.1f}s"
                       f" · {len(servicio.universo)} tickers vigilados"
                       f" · {servicio.n_radar_base} en el radar base"
                       f" (solo precio ${BASE_PRECIO_MIN:.2f}-${BASE_PRECIO_MAX:.0f}; EMA20 arriba + MACD positivo)")
        else:
            detalle = (f"Última actualización: {servicio.ultima_actualizacion.strftime('%H:%M:%S')} ET"
                       f" · ciclo {servicio.duracion_ciclo:.1f}s"
                       f" · {len(servicio.universo)} tickers vigilados"
                       f" · {servicio.n_radar_base} en el radar base"
                       f" (precio ${BASE_PRECIO_MIN:.2f}-${BASE_PRECIO_MAX:.0f}, subida ≥ {BASE_GAP_MIN:.0f}%, float ≤ {formatear_numero_grande(BASE_FLOTACION_MAX)}, volumen actual ≥ {formatear_numero_grande(servicio.filtros_dueno.get("volumen_min", 20_000))})")
        st.caption(detalle)
    else:
        st.caption("Esperando el primer escaneo (la primera vez puede tardar un minuto)...")

    if servicio.ultimo_error:
        st.warning(f"Aviso del motor: {servicio.ultimo_error}")

    st.markdown(f"**{len(filas)} resultados** · el motor escanea cada {INTERVALO_ESCANEO_SEGUNDOS}s")

    if not filas:
        st.info("Sin candidatos que cumplan los filtros en este momento.")
        return

    df = pd.DataFrame([
        {
            "No.": i + 1,
            "Ticker": c["ticker"],
            "Precio": round(c["precio"], 2),
            "Cambio %": round(c["cambio_pct"], 1),
            "Volumen": formatear_numero_grande(c["volumen_dia"]),
            "Flotación": (
                formatear_numero_grande(c["float_shares"])
                if c["float_shares"] is not None
                else ("Pendiente" if c.get("float_status") == "pending" else "Sin dato")
            ),
            "EMA20": "✅" if c["cruzando_ema20"] else ("🔻" if c.get("cruzando_ema20_abajo") else ""),
            "MACD": "✅" if c["macd_positivo"] else ("🔻" if c.get("macd_negativo") else ""),
            "BB sup.": round(c.get("bb_upper"), 2) if c.get("bb_upper") is not None else None,
            "Dist. BB %": round(c.get("bb_dist_pct"), 1) if c.get("bb_dist_pct") is not None else None,
            "Noticia": "🔥" if c["tiene_noticia"] else "",
            "Actualizado (ET)": c["actualizado"].astimezone(ET).strftime("%H:%M:%S") if hasattr(c["actualizado"], "astimezone") else str(c["actualizado"]),
        }
        for i, c in enumerate(filas)
    ])

    styled = (
        df.style
        .map(color_cambio, subset=["Cambio %"])
        .set_properties(**{
            "background-color": "#101318",
            "color": "#e8edf2",
            "border-color": "#303640",
            "font-size": "9px",
            "padding": "2px 4px",
            "line-height": "1.05",
            "white-space": "nowrap",
        })
        .set_table_styles([
            {"selector": "th", "props": [
                ("background-color", "#171b22"),
                ("color", "#f0c75e"),
                ("font-weight", "700"),
                ("font-size", "9px"),
                ("padding", "2px 4px"),
                ("line-height", "1.0"),
                ("border-color", "#39404b"),
                ("white-space", "nowrap"),
            ]},
            {"selector": "tbody tr:nth-child(even)", "props": [("background-color", "#0d1117")]},
            {"selector": "tbody tr:hover", "props": [("background-color", "#202733")]},
        ])
    )
    seleccion = st.dataframe(
        styled, width="stretch", hide_index=True,
        on_select="rerun", selection_mode="single-row", key="tabla_resultados",
    )
    filas_sel = seleccion.selection.rows if seleccion and seleccion.selection else []
    if filas_sel:
        st.session_state["ticker_activo"] = df.iloc[filas_sel[0]]["Ticker"]


with panel_resultados_slot.container():
    panel_resultados()

# El diagnóstico técnico se mantiene debajo para no empujar la tabla principal.
panel_diagnostico_filtros()


# ==========================================
# 🔗 PANEL BROKER: 10 activos del scanner ↔ 10 colores ↔ 10 layouts del broker
# Clic en una fila = envía ese símbolo al layout del color de esa fila.
#   · Con webhook para ese color (ej. trigger de Macro Deck): se envía ahí.
#   · Sin webhook: se envía al "puente" local (URL general).
#   · Quantfury: copia el ticker al portapapeles.
# El envío lo hace TU NAVEGADOR (no el servidor), así funciona con http://127.0.0.1 en tu PC.
# No coloca órdenes: solo manda el símbolo.
# ==========================================

PANEL_BROKER_ALTO_PX = 310
PUENTE_LOCAL_POR_DEFECTO = "http://127.0.0.1:8765/enviar"
BROKERS_DISPONIBLES = [
    "Interactive Brokers (TWS)", "TradeZero (webhook)", "Binance (webhook)",
    "Quantfury (portapapeles)", "Otro (webhook)",
]
RUTA_PANEL_BROKER = os.path.join(os.getcwd(), "config_panel_broker.json")

def _texto_contraste(hex_color):
    """Elige texto negro/blanco según el brillo del color elegido."""
    try:
        h = str(hex_color).lstrip("#")
        if len(h) != 6:
            return "#000000"
        r, g, b = (int(h[i:i+2], 16) for i in (0, 2, 4))
        brillo = (r * 299 + g * 587 + b * 114) / 1000
        return "#000000" if brillo >= 150 else "#ffffff"
    except Exception:
        return "#000000"

def colores_layout_actuales():
    personalizados = st.session_state.get("bk_colores", [])
    resultado = []
    for i, (nombre, bg_def, _fg_def) in enumerate(COLORES_LAYOUT_DEFECTO):
        bg = personalizados[i] if i < len(personalizados) and personalizados[i] else bg_def
        resultado.append((nombre, bg, _texto_contraste(bg)))
    return resultado

CSS_PANEL_BROKER = (
    "body{margin:0;background:transparent;font-family:Calibri,'Segoe UI',Arial,sans-serif;}"
    ".tbl-wrap{width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch;}"
    "table{width:100%;min-width:560px;border-collapse:collapse;table-layout:fixed;}"
    "th{background:#0b0b0b;color:#d4af37;font-weight:700;font-size:15px;padding:8px 6px;"
    "border:1px solid #5d4b19;text-align:center;line-height:1.15;}"
    "th.cred{display:none;}"
    "th.cred .m{font-weight:400;font-size:12px;opacity:.9;}"
    "th.cred .bk{font-weight:400;font-size:11px;opacity:.85;margin-top:2px;}"
    "td{height:34px;border:1px solid #808080;text-align:center;font-size:14px;color:#111111;"
    "background:#ffffff;padding:0 4px;overflow:hidden;white-space:nowrap;text-overflow:ellipsis;}"
    "th.colhdr{width:32px;min-width:32px;max-width:32px;padding:4px 0;}"
    "td.col{width:32px;min-width:32px;max-width:32px;padding:0;text-align:center;}"
    "th.gearhdr,td.gear{width:30px;min-width:30px;max-width:30px;padding:0;text-align:center;}"
    "td.gear{font-size:18px;color:#d4af37;cursor:pointer;}"
    ".swatch{display:inline-block;width:14px;height:14px;border-radius:3px;border:1px solid rgba(255,255,255,.55);vertical-align:middle;}"
    "td.sym{font-weight:700;}"
    "tr.fila.ok{cursor:pointer;}"
    "tr.fila.ok:hover td:not(.col){filter:brightness(.94);}"
    "tr.pos td:not(.col){background:#0d1b13;color:#7be2a7;}"
    "tr.neg td:not(.col){background:#1b0d0d;color:#f28a8a;}"
    "tr.sel td:not(.col){box-shadow:inset 0 0 0 2px #d4af37;}"
    "#msg{margin-top:8px;padding:6px 10px;border-radius:6px;background:#15120b;color:#f0dfaa;border:1px solid #5d4b19;"
    "font-size:13px;display:none;}"
    # --- Responsivo: celular. La tabla no se aprieta, se puede deslizar horizontal ---
    "@media (max-width:640px){"
    "  .tbl-wrap{overflow:hidden;}"
    "  table{min-width:0;width:100%;table-layout:fixed;}"
    "  th{font-size:7px;padding:3px 1px;line-height:1;}"
    "  th.gearhdr,td.gear{width:18px;min-width:18px;max-width:18px;}"
    "  th.colhdr,td.col{width:16px;min-width:16px;max-width:16px;}"
    "  th:nth-child(3){width:18%;}"
    "  th:nth-child(4){width:11%;}"
    "  th:nth-child(5){width:12%;}"
    "  th:nth-child(6){width:17%;}"
    "  th:nth-child(7){width:17%;}"
    "  th:nth-child(8){width:12%;}"
    "  td{font-size:8px;height:21px;padding:0 1px;line-height:1;}"
    "  td.gear{font-size:10px;}"
    "  .swatch{width:8px;height:8px;border-radius:2px;}"
    "  #msg{font-size:8px;margin-top:3px;padding:3px 5px;}"
    "}"
)

JS_PANEL_BROKER = r"""
(function(){
  const D = JSON.parse(document.getElementById("datos").textContent);
  const msg = document.getElementById("msg");
  let temporizador = null;
  function aviso(txt, tipo){
    msg.textContent = txt;
    msg.style.display = "block";
    msg.style.background = tipo === "ok" ? "#166534" : (tipo === "err" ? "#991b1b" : "#1f2937");
    clearTimeout(temporizador);
    temporizador = setTimeout(function(){ msg.style.display = "none"; }, 6000);
  }
  function guardarSel(tk){ try { sessionStorage.setItem("sel_ticker", tk); } catch(e) {} }
  function leerSel(){ try { return sessionStorage.getItem("sel_ticker"); } catch(e) { return null; } }

  const filas = document.querySelectorAll("tr.fila");
  const previa = leerSel();
  filas.forEach(function(tr){
    const d = D.filas[+tr.dataset.i];
    if (d && d.ticker === previa) tr.classList.add("sel");
  });

  filas.forEach(function(tr){
    tr.addEventListener("click", async function(){
      const i = +tr.dataset.i;
      const d = D.filas[i];
      if (!d) return;
      filas.forEach(function(x){ x.classList.remove("sel"); });
      tr.classList.add("sel");
      guardarSel(d.ticker);
      const color = D.colores[i];
      const broker = D.cfg.broker || "";
      const payload = {simbolo: d.ticker, ticker: d.ticker, color: color, color_num: i + 1, broker: broker};

      // 1) Quantfury: copiar al portapapeles
      if (broker.indexOf("Quantfury") === 0) {
        try {
          await navigator.clipboard.writeText(d.ticker);
          aviso("✔ " + d.ticker + " copiado: pégalo en Quantfury", "ok");
        } catch(e) {
          aviso("⚠ No pude copiar automáticamente. Ticker: " + d.ticker, "err");
        }
        return;
      }

      // 2) Webhook propio de este color (ej. trigger de Macro Deck)
      const hook = ((D.webhooks || [])[i] || "").trim();
      if (hook) {
        aviso("Enviando " + d.ticker + " → " + color + "…", "info");
        try {
          await fetch(hook, {method: "POST", mode: "no-cors",
                             headers: {"Content-Type": "text/plain"}, body: JSON.stringify(payload)});
          aviso("✔ " + d.ticker + " enviado al webhook de " + color + " (sin confirmación de respuesta)", "ok");
        } catch(e) {
          aviso("⚠ No pude alcanzar el webhook de " + color, "err");
        }
        return;
      }

      // 3) Puente local general
      const puente = (D.cfg.puente || "").trim();
      if (!puente) {
        aviso("⚠ Falta la URL del puente o el webhook de " + color + " (botón ✏️)", "err");
        return;
      }
      payload.api_key = D.cfg.api_key;
      payload.api_secret = D.cfg.api_secret;
      aviso("Enviando " + d.ticker + " → " + color + "…", "info");
      try {
        const r = await fetch(puente, {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify(payload)
        });
        let j = {};
        try { j = await r.json(); } catch(e) {}
        if (r.ok && j.ok !== false) aviso("✔ " + d.ticker + " enviado al layout " + color, "ok");
        else aviso("⚠ El puente respondió con error" + (j.error ? ": " + j.error : ""), "err");
      } catch(e) {
        aviso("⚠ Sin conexión con el puente del broker (" + puente + ")", "err");
      }
    });
  });
})();
"""


def construir_html_panel_broker(filas10, cfg, colores_layout=None):
    """HTML del panel con columna compacta de colores configurables."""
    colores_layout = colores_layout or COLORES_LAYOUT_DEFECTO
    key = cfg.get("api_key") or ""
    sec = cfg.get("api_secret") or ""
    key_txt = ("••••" + key[-4:]) if key else ""
    sec_txt = "••••••••" if sec else ""
    broker_txt = html_escape(cfg.get("broker") or "") if (key or sec) else ""

    cuerpo = []
    for i, (nombre, bg, fg) in enumerate(colores_layout):
        d = filas10[i] if i < len(filas10) else None
        celda_color = f'<td class="col"><span class="swatch" style="background:{bg}"></span></td>'
        if d is None:
            cuerpo.append(f'<tr class="fila" data-i="{i}"><td class="gear" title="Vincular este activo con el layout del broker">🔗</td>{celda_color}' + "<td></td>" * 6 + "</tr>")
            continue
        clase = "ok " + ("pos" if d["subiendo"] else "neg")
        cuerpo.append(
            f'<tr class="fila {clase}" data-i="{i}">'
            f'<td class="gear" title="Vincular este activo con el layout del broker">🔗</td>'
            f'{celda_color}'
            f'<td class="sym">{html_escape(str(d["ticker"]))}{" 🔥" if d["noticia"] else ""}</td>'
            f'<td>{d["precio"]:.2f}</td>'
            f'<td>{d["cambio"]:+.1f}%</td>'
            f'<td>{d["volumen"]}</td>'
            f'<td>{d["flotacion"]}</td>'
            f'<td>{d["volrel"]:.2f}</td>'
            f'</tr>'
        )

    webhooks = list(cfg.get("webhooks") or [])[: len(colores_layout)]
    webhooks += [""] * (len(colores_layout) - len(webhooks))

    datos = {
        "filas": [
            ({"ticker": d["ticker"]} if d else None)
            for d in (list(filas10) + [None] * (len(colores_layout) - len(filas10)))[: len(colores_layout)]
        ],
        "colores": [c[1] for c in colores_layout],
        "webhooks": webhooks,
        "cfg": {
            "broker": cfg.get("broker") or "",
            "api_key": key,
            "api_secret": sec,
            "puente": cfg.get("puente") or "",
        },
    }
    datos_json = json.dumps(datos, ensure_ascii=False).replace("</", "<\\/")

    return (
        "<!DOCTYPE html><html><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        "<style>" + CSS_PANEL_BROKER + "</style></head><body>"
        "<div class='tbl-wrap'>"
        "<table><thead><tr>"
        '<th class="gearhdr">🔗</th><th class="colhdr">&nbsp;</th><th>Símbolo / Noticia</th><th>Precio</th><th>Cambio %</th>' 
        "<th>Volumen</th><th>Flotación</th><th>Vol. Relativo</th>"
        "</tr></thead><tbody>" + "".join(cuerpo) + "</tbody></table>"
        "</div>"
        '<div id="msg"></div>'
        f'<script type="application/json" id="datos">{datos_json}</script>'
        "<script>" + JS_PANEL_BROKER + "</script>"
        "</body></html>"
    )


def _direccion_por_ticker():
    """Última dirección conocida (True = subiendo / False = bajando) de cada ticker, según los eventos."""
    dirs = {}
    for ev in list(getattr(servicio, "eventos", [])):  # el más nuevo primero
        dirs.setdefault(ev["ticker"], ev["subiendo"])
    return dirs


# --- Configuración por licencia (broker, puente y webhooks; la API Key/Secret NO se guardan en disco) ---
def _clave_usuario():
    return hashlib.sha256(str(TOKEN_ACTIVO).encode("utf-8")).hexdigest()[:16]


def cargar_panel_broker():
    try:
        with open(RUTA_PANEL_BROKER, "r", encoding="utf-8") as f:
            d = json.load(f).get(_clave_usuario(), {})
    except Exception:
        d = {}
    broker = d.get("broker", BROKERS_DISPONIBLES[0])
    if broker not in BROKERS_DISPONIBLES:
        broker = BROKERS_DISPONIBLES[0]
    webhooks = [str(w) for w in list(d.get("webhooks", []))[: len(COLORES_LAYOUT_DEFECTO)]]
    webhooks += [""] * (len(COLORES_LAYOUT_DEFECTO) - len(webhooks))
    colores = [str(c) for c in list(d.get("colores", []))[: len(COLORES_LAYOUT_DEFECTO)]]
    colores += [bg for _n, bg, _fg in COLORES_LAYOUT_DEFECTO[len(colores):]]
    return {"broker": broker, "puente": d.get("puente", PUENTE_LOCAL_POR_DEFECTO), "webhooks": webhooks, "colores": colores}


def guardar_panel_broker(cfg):
    try:
        try:
            with open(RUTA_PANEL_BROKER, "r", encoding="utf-8") as f:
                todo = json.load(f)
        except Exception:
            todo = {}
        todo[_clave_usuario()] = cfg
        with open(RUTA_PANEL_BROKER, "w", encoding="utf-8") as f:
            json.dump(todo, f)
    except Exception:
        pass


if "bk_cargado" not in st.session_state:
    _ini = cargar_panel_broker()
    st.session_state.setdefault("bk_nombre", _ini["broker"])
    st.session_state.setdefault("bk_puente", _ini["puente"])
    st.session_state.setdefault("bk_api_key", "")
    st.session_state.setdefault("bk_api_secret", "")
    for _i, _w in enumerate(_ini["webhooks"]):
        st.session_state.setdefault(f"bk_wh_{_i}", _w)
    st.session_state.setdefault("bk_colores", list(_ini.get("colores", [bg for _n, bg, _fg in COLORES_LAYOUT_DEFECTO])))
    st.session_state.setdefault("_bk_guardado", _ini)
    st.session_state["bk_cargado"] = True

# --- Persistencia de configuración del broker y colores ---
_cfg_guardable = {
    "broker": st.session_state.get("bk_nombre", BROKERS_DISPONIBLES[0]),
    "puente": st.session_state.get("bk_puente", PUENTE_LOCAL_POR_DEFECTO),
    "webhooks": [st.session_state.get(f"bk_wh_{_i}", "") for _i in range(len(COLORES_LAYOUT_DEFECTO))],
    "colores": list(st.session_state.get("bk_colores", [bg for _n,bg,_fg in COLORES_LAYOUT_DEFECTO])),
}
if st.session_state.get("_bk_guardado") != _cfg_guardable:
    guardar_panel_broker(_cfg_guardable)
    st.session_state["_bk_guardado"]=_cfg_guardable


@st.fragment(run_every=(f"{int(REFRESCO)}s" if AUTO_ON else None))
def panel_broker():
    filas = filtrar_resultados(list(servicio.resultados), params)[: len(COLORES_LAYOUT)]
    dirs = _direccion_por_ticker()
    filas10 = [
        {
            "ticker": c["ticker"],
            "noticia": bool(c["tiene_noticia"]),
            "precio": c["precio"],
            "cambio": c["cambio_pct"],
            "volumen": formatear_numero_grande(c["volumen_dia"]),
            "flotacion": (
                formatear_numero_grande(c["float_shares"])
                if c["float_shares"] is not None
                else ("Pendiente" if c.get("float_status") == "pending" else "Sin dato")
            ),
            "volrel": c["volumen_relativo"],
            "subiendo": dirs.get(c["ticker"], True),
        }
        for c in filas
    ]
    if ES_ADMIN:
        cfg = {
            "broker": st.session_state.get("bk_nombre", BROKERS_DISPONIBLES[0]),
            "api_key": st.session_state.get("bk_api_key", ""),
            "api_secret": st.session_state.get("bk_api_secret", ""),
            "puente": st.session_state.get("bk_puente", ""),
            "webhooks": [st.session_state.get(f"bk_wh_{_i}", "") for _i in range(len(COLORES_LAYOUT_DEFECTO))],
        }
    else:
        # Nunca enviar credenciales, webhooks ni puentes privados al navegador
        # de un usuario normal.
        cfg = {
            "broker": "",
            "api_key": "",
            "api_secret": "",
            "puente": "",
            "webhooks": ["" for _ in range(len(COLORES_LAYOUT_DEFECTO))],
        }
    with panel_broker_slot.container():
        st.components.v1.html(
            construir_html_panel_broker(filas10, cfg, colores_layout_actuales()),
            height=PANEL_BROKER_ALTO_PX,
            scrolling=False,
        )


panel_broker()