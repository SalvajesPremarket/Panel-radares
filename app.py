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
import streamlit.components.v1 as components
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

    /* --- La carátula del scanner debe ocupar todo el ancho disponible --- */
    .block-container {
        max-width: 100% !important;
        width: 100% !important;
        padding-left: 0.35rem !important;
        padding-right: 0.35rem !important;
    }
    [data-testid="stIFrame"],
    [data-testid="stIFrame"] > iframe {
        width: 100% !important;
        max-width: 100% !important;
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
        max-width: 100% !important;
        width: 100% !important;
        padding-left: .35rem !important;
        padding-right: .35rem !important;
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
# ==============================================================================
# 🖥️ CARÁTULA FINVIZ — PRESENTACIÓN FINAL DEL SCANNER REAL
#    Esta sección solo presenta/filtra los datos del motor existente.
#    No reemplaza ni modifica el motor, sus hilos, cache, Alpaca, FMP ni pruebas.
# ==============================================================================

try:
    servicio._esta_en_horario_automatico()
except Exception:
    pass

# Valores de interfaz seguros. Se leen de query_params para que los cambios
# realizados desde la carátula puedan sobrevivir al rerun de Streamlit.
def _qtxt(nombre, defecto):
    try:
        valor = st.query_params.get(nombre, defecto)
        if isinstance(valor, list):
            valor = valor[0] if valor else defecto
        return str(valor)
    except Exception:
        return str(defecto)


def _qfloat(nombre, defecto):
    try:
        return float(_qtxt(nombre, defecto))
    except Exception:
        return float(defecto)


def _qint(nombre, defecto):
    try:
        return int(float(_qtxt(nombre, defecto)))
    except Exception:
        return int(defecto)


precio_min_ui = _qfloat("f_price_min", 0.50)
precio_max_ui = _qfloat("f_price_max", 20.00)
gap_min_ui = _qfloat("f_gap_min", 3.00)
gap_max_ui = _qfloat("f_gap_max", 50.00)
float_max_ui = _qint("f_float_max", 20_000_000)
volumen_min_ui = _qint("f_vol", 20_000)
ema_ui = _qtxt("f_ema", "Hacia arriba")
macd_ui = _qtxt("f_mac", "Positivo")
orden_ui = _qtxt("f_order", "Actualizado")

if ema_ui not in ("Hacia arriba", "Hacia abajo", "Neutro"):
    ema_ui = "Hacia arriba"
if macd_ui not in ("Positivo", "Negativo", "No exigir"):
    macd_ui = "Positivo"
if orden_ui not in ("Actualizado", "Cambio %", "Volumen"):
    orden_ui = "Actualizado"

params_ui = {
    "precio_min": precio_min_ui,
    "precio_max": precio_max_ui,
    "gap_min": gap_min_ui,
    "gap_max": gap_max_ui,
    "flotacion_max": float_max_ui,
    "volumen_min": volumen_min_ui,
    "cruce_ema": ema_ui,
    "macd": macd_ui,
    "orden": orden_ui,
    "top_n": 50,
}

try:
    filas_reales = filtrar_resultados(list(servicio.resultados), params_ui)
except Exception:
    filas_reales = list(getattr(servicio, "resultados", []) or [])


def _num(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _entero(v, default=0):
    try:
        if v is None or v == "":
            return default
        return int(float(v))
    except Exception:
        return default


def _safe_text(v, default=""):
    return html_escape(str(v if v is not None else default))


def _money(v):
    return f"${_num(v):,.2f}"


def _pct(v):
    return f"{_num(v):+.2f}%"


def _big(v):
    n = _num(v)
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.0f}K"
    return f"{n:.0f}"


def _row_html(row):
    ticker = _safe_text(row.get("ticker", ""))
    sector = _safe_text(row.get("sector", "N/A"))
    precio = _num(row.get("precio"))
    cambio = _num(row.get("cambio_pct"))
    volumen = _entero(row.get("volumen_dia"))
    flotacion = _num(row.get("float_shares")) / 1_000_000 if row.get("float_shares") else 0.0
    ema_ok = bool(row.get("cruzando_ema20"))
    ema_down = bool(row.get("cruzando_ema20_abajo"))
    mac_pos = bool(row.get("macd_positivo"))
    mac_neg = bool(row.get("macd_negativo"))
    noticia = bool(row.get("tiene_noticia"))
    fila = "fila-alza" if cambio > 0 else ("fila-baja" if cambio < 0 else "")
    ema_txt = "Sobre EMA20" if ema_ok else ("Bajo EMA20" if ema_down else "Sin patrón")
    mac_txt = "Positivo" if mac_pos else ("Negativo" if mac_neg else "Neutro")
    mac_cls = "macd-positivo" if mac_pos else ("macd-negativo" if mac_neg else "macd-neutro")
    news = " 🔥" if noticia else ""
    return (
        f"<tr class='{fila}'>"
        f"<td class='layout-col'><select class='engranaje-select' onchange='cambiarLayout(&quot;{ticker}&quot;,this)'>"
        f"<option value=''>⚙️ Layout</option>"
        f"<option value='L1'>L1 Rojo</option><option value='L2'>L2 Azul</option>"
        f"<option value='L3'>L3 Verde</option><option value='L4'>L4 Amarillo</option>"
        f"<option value='L5'>L5 Morado</option><option value='L6'>L6 Naranja</option>"
        f"<option value='L7'>L7 Blanco</option><option value='L8'>L8 Negro</option>"
        f"<option value='L9'>L9 Cian</option><option value='L10'>L10 Rosa</option>"
        f"</select></td>"
        f"<td><b>{ticker}</b>{news}</td>"
        f"<td>{sector}</td>"
        f"<td class='num-col'>{_money(precio)}</td>"
        f"<td class='num-col'>{_pct(cambio)}</td>"
        f"<td class='num-col'>{_big(volumen)}</td>"
        f"<td class='num-col'>{_pct(cambio)}</td>"
        f"<td class='num-col'>{flotacion:.2f}M</td>"
        f"<td>{ema_txt}</td>"
        f"<td class='{mac_cls}'>{mac_txt}</td></tr>"
    )


# La sección RESULTADOS / VISUALIZACIÓN mantiene siempre las 10 líneas
# horizontales del diseño. Cuando hay señales reales se colocan en las primeras
# líneas; las restantes quedan disponibles con su engranaje de Layout.
filas_visualizacion = list(filas_reales[:10])
while len(filas_visualizacion) < 10:
    filas_visualizacion.append(None)


def _row_visualizacion(item, indice):
    if item is None:
        return (
            "<tr class='fila-vacia'>"
            "<td class='layout-col'><select class='engranaje-select' onchange='cambiarLayout("",this)'>"
            "<option value=''>⚙️ Layout</option>"
            "<option value='L1'>L1 Rojo</option><option value='L2'>L2 Azul</option>"
            "<option value='L3'>L3 Verde</option><option value='L4'>L4 Amarillo</option>"
            "<option value='L5'>L5 Morado</option><option value='L6'>L6 Naranja</option>"
            "<option value='L7'>L7 Blanco</option><option value='L8'>L8 Negro</option>"
            "<option value='L9'>L9 Cian</option><option value='L10'>L10 Rosa</option>"
            "</select></td>"
            "<td><b>—</b></td><td>—</td><td class='num-col'>—</td>"
            "<td class='num-col'>—</td><td class='num-col'>—</td><td class='num-col'>—</td>"
            "<td class='num-col'>—</td><td>—</td><td class='macd-neutro'>—</td></tr>"
        )
    return _row_html(item)


rows_html = "".join(_row_visualizacion(r, i + 1) for i, r in enumerate(filas_visualizacion))

try:
    hora_ini = int(servicio.hora_inicio_auto_min)
    hora_fin = int(servicio.hora_fin_auto_min)
except Exception:
    hora_ini, hora_fin = 240, 960

_hora_txt = f"{hora_ini//60:02d}:{hora_ini%60:02d} - {hora_fin//60:02d}:{hora_fin%60:02d} ET"
_estado_txt = "ON" if servicio.encendido and servicio.auto_en_horario else ("OFF" if not servicio.encendido else "ESPERA")

start_time = f"{hora_ini//60:02d}:{hora_ini%60:02d}"
end_time = f"{hora_fin//60:02d}:{hora_fin%60:02d}"

active_val = _qtxt("c_active", "True" if getattr(servicio, "encendido", True) else "False")
lang_val = _qtxt("c_lang", "ESP")
wnd_val = _qtxt("c_wnd", "Incrustada")
broker_val = _qtxt("c_broker", st.session_state.get("bk_nombre", "Interactive Brokers"))
bridge_val = _qtxt("c_url", st.session_state.get("bk_puente", "http://localhost:8080/layout"))

h = "<!DOCTYPE html><html><head><meta charset='UTF-8'>"
h += "<meta name='viewport' content='width=device-width, initial-scale=1.0'>"
h += "<title>TradeScanner</title>"
h += "<style>"
h += "*{box-sizing:border-box;}"
h += "html,body{margin:0;padding:0;width:100%;min-height:100%;}body{background:#dcdcdc;font-family:Verdana,Arial,sans-serif;font-size:12px;color:#000;overflow-x:hidden;padding-top:8px;}"
h += ".main-container{width:100%;max-width:none;margin:0 auto;padding:6px;}"
h += ".topbar{background:#efefef;border:1px solid #777;padding:9px 10px;margin-bottom:6px;display:flex;align-items:center;justify-content:space-between;gap:10px;min-height:58px;position:sticky;top:0;z-index:1000;overflow:visible;}"
h += ".brand{font-size:22px;font-weight:900;letter-spacing:.3px;color:#111;white-space:nowrap;line-height:1.05;}.brand small{font-size:10px;font-weight:normal;color:#555;}"
h += ".status{font-weight:bold;white-space:nowrap;}.status.on{color:#08752c}.status.off{color:#a40000}.status.wait{color:#9a6b00}"
h += ".tabs{display:flex;gap:3px;overflow-x:auto;background:#c9c9c9;border:1px solid #777;padding:3px;margin-bottom:5px;white-space:nowrap;}"
h += ".tab{font-size:10px;font-weight:bold;padding:4px 9px;background:#eee;border:1px solid #777;cursor:pointer;}.tab.active{background:#fff;border-bottom:2px solid #111;}"
h += ".filtros-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:5px;background:#fff;border:1px solid #888;padding:6px;margin-bottom:6px;}"
h += ".filtro-item{min-width:0;display:flex;align-items:center;justify-content:space-between;gap:8px;background:#f1f1f1;border:1px solid #aaa;padding:5px 7px;min-height:38px;}"
h += ".filtro-item label{font-weight:bold;color:#111;font-size:10px;white-space:nowrap;}"
h += "input,select,button{font-family:Verdana,Arial,sans-serif;font-size:11px;height:27px;border:1px solid #777;background:#fff;color:#000;border-radius:0;outline:none;}"
h += "input{min-width:0;width:105px;padding:1px 4px;}select{min-width:105px;max-width:170px;padding:1px 3px;}button{cursor:pointer;background:#eaeaea;font-weight:bold;padding:2px 8px;}"
h += ".range{display:flex;gap:2px;align-items:center;}.range span{font-size:8px;color:#555;}"
h += ".logo{display:flex;align-items:center;justify-content:center;background:#e6e6e6;border:1px dashed #777;font-weight:900;color:#222;min-height:34px;font-size:14px;}"
h += ".engine{font-weight:bold;}.subline{background:#eee;border:1px solid #999;padding:5px 7px;margin-bottom:6px;font-size:10px;display:flex;gap:16px;flex-wrap:wrap;}"
h += ".result-title{background:#c9c9c9;border:1px solid #777;border-bottom:0;padding:5px 8px;font-size:11px;font-weight:900;letter-spacing:.2px;}"
h += ".table-wrapper{width:100%;overflow-x:auto;background:#fff;border:1px solid #777;}table{width:100%;min-width:930px;border-collapse:collapse;table-layout:auto;}"
h += "th{background:#c8c8c8;color:#000;font-weight:bold;padding:7px 7px;border:1px solid #888;font-size:10px;text-align:left;white-space:nowrap;}"
h += "td{padding:5px 7px;border:1px solid #aaa;font-size:11px;white-space:nowrap;height:27px;}"
h += ".fila-alza{background:#edf7e8}.fila-baja{background:#fceceb}.fila-vacia{background:#fafafa;color:#777}.num-col{text-align:right}.empty-row{text-align:center!important;padding:18px!important;color:#555;font-style:italic;}"
h += ".macd-positivo{background:#b7dca0;color:#155724;font-weight:bold;text-align:center}.macd-negativo{background:#f4b084;color:#721c24;font-weight:bold;text-align:center}.macd-neutro{background:#e2e3e5;text-align:center;}"
h += ".layout-col{width:120px;text-align:center;background:#f1f1f1;}.engranaje-select{width:112px;font-size:9px;height:21px;}"
h += ".footer-note{margin-top:4px;font-size:8px;color:#555;display:flex;justify-content:space-between;gap:8px;}"
h += ".tab-panel{display:none;background:#f7f7f7;border:1px solid #888;border-top:0;padding:7px;margin-bottom:6px;font-size:10px;}.tab-panel.active{display:block;}.panel-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:5px;}.panel-card{background:#fff;border:1px solid #aaa;padding:7px;min-height:44px;}.panel-card b{display:block;margin-bottom:3px;font-size:9px;}.panel-card span{font-size:10px;}"
h += "@media(max-width:900px){.filtros-grid{grid-template-columns:repeat(2,minmax(0,1fr));}.brand{font-size:16px;}.status{font-size:10px;white-space:normal;text-align:right;}}"
h += "@media(max-width:520px){.main-container{padding:3px 3px 8px;width:100%;}.topbar{position:sticky;top:0;min-height:86px;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:5px;padding:10px 6px;margin:0 0 5px;overflow:visible;}.brand{font-size:20px;white-space:nowrap;line-height:1.05;width:100%;text-align:center;}.brand small{display:block;font-size:8px;margin-top:3px;}.status{font-size:9px;white-space:normal;text-align:center;width:100%;line-height:1.2;}.tabs{display:grid;grid-template-columns:repeat(4,1fr);gap:2px;overflow:visible;width:100%;}.tab{font-size:8px;padding:6px 2px;flex:1 1 auto;width:100%;}.filtros-grid{grid-template-columns:1fr;gap:4px;padding:5px;}.filtro-item{min-height:34px;padding:4px 6px;gap:6px;}.filtro-item label{font-size:9px;flex:0 0 auto;}.filtro-item input,.filtro-item select{font-size:10px;height:25px;max-width:none;width:auto;min-width:120px;}.filtro-item .range{flex:1;min-width:0;}.filtro-item .range input{width:100%;min-width:70px;}.logo{min-height:38px;font-size:15px;}.subline{font-size:9px;gap:8px;padding:6px;}.result-title{font-size:10px;padding:6px 7px;}.table-wrapper{overflow-x:auto;-webkit-overflow-scrolling:touch;}.table-wrapper table{min-width:930px;}.footer-note{font-size:8px;flex-direction:column;gap:2px}.engranaje-select{width:112px;height:24px;font-size:10px}.panel-grid{grid-template-columns:1fr;gap:4px}.tab-panel{font-size:9px;padding:6px}}"
h += "</style>"
h += "<script>window.addEventListener('load',function(){try{window.scrollTo(0,0);document.documentElement.scrollTop=0;document.body.scrollTop=0;}catch(e){}});"
h += "function setQ(k,v){var q=new URLSearchParams(window.parent.location.search);q.set(k,v);window.parent.location.search='?'+q.toString();}"
h += "function pushConfig(){var q=new URLSearchParams(window.parent.location.search);"
h += "q.set('f_price_min',document.getElementById('price_min').value);q.set('f_price_max',document.getElementById('price_max').value);"
h += "q.set('f_gap_min',document.getElementById('gap_min').value);q.set('f_gap_max',document.getElementById('gap_max').value);"
h += "q.set('f_float_max',document.getElementById('float_max').value);q.set('f_vol',document.getElementById('txt_vol').value);"
h += "q.set('f_ema',document.getElementById('sel_ema').value);q.set('f_mac',document.getElementById('sel_mac').value);"
h += "q.set('f_order',document.getElementById('sel_order').value);q.set('c_active',document.getElementById('cfg_active').value);"
h += "q.set('c_start',document.getElementById('cfg_start').value);q.set('c_end',document.getElementById('cfg_end').value);"
h += "q.set('c_lang',document.getElementById('cfg_lang').value);q.set('c_wnd',document.getElementById('cfg_wnd').value);"
h += "q.set('c_broker',document.getElementById('cfg_broker').value);q.set('c_url',document.getElementById('cfg_url').value);"
h += "window.parent.location.search='?'+q.toString();}"
h += "function cambiarLayout(t,e){var v=e.value;if(!v)return;var q=new URLSearchParams(window.parent.location.search);q.set('link_ticker',t);q.set('layout_color',v);window.parent.history.replaceState(null,'','?'+q.toString());var u=document.getElementById('cfg_url').value;fetch(u,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({ticker:t,layout_color:v}),mode:'cors'}).catch(function(){});}"
h += "function showTab(id,btn){document.querySelectorAll('.tab-panel').forEach(function(p){p.classList.remove('active');});document.querySelectorAll('.tab').forEach(function(b){b.classList.remove('active');});var p=document.getElementById(id);if(p)p.classList.add('active');if(btn)btn.classList.add('active');if(id==='panel-resultados'){var r=document.getElementById('resultados-tabla');if(r)r.scrollIntoView({behavior:'smooth',block:'start'});}}"
h += "</script></head><body>"
h += "<div class='main-container'>"
h += "<div class='topbar'><div class='brand'>TRADE<span style='color:#555'>SCANNER</span> <small>PRE MARKET · REAL TIME</small></div>"
h += f"<div class='status {'on' if _estado_txt=='ON' else ('off' if _estado_txt=='OFF' else 'wait')}'>{'🟢' if _estado_txt=='ON' else ('🔴' if _estado_txt=='OFF' else '🟡')} MOTOR {_estado_txt} · HORARIO {_safe_text(_hora_txt)}</div></div>"
h += "<div class='tabs'>"
h += "<button class='tab active' onclick=\"showTab('panel-radar',this)\">RADAR</button>"
h += "<button class='tab' onclick=\"showTab('panel-tecnicos',this)\">TÉCNICOS</button>"
h += "<button class='tab' onclick=\"showTab('panel-config',this)\">CONFIGURACIÓN</button>"
h += "<button class='tab' onclick=\"showTab('panel-resultados',this)\">RESULTADOS</button>"
h += "</div>"
h += "<div id='panel-radar' class='tab-panel active'><b>RADAR</b><br>Filtros principales del radar: precio, gap, flotación y volumen.</div>"
h += "<div id='panel-tecnicos' class='tab-panel'><div class='panel-grid'>"
h += f"<div class='panel-card'><b>CRUCE EMA20</b><span>Condición actual: {_safe_text(ema_ui)} · vela nueva sobre EMA20.</span></div>"
h += f"<div class='panel-card'><b>MACD</b><span>Condición actual: {_safe_text(macd_ui)}.</span></div>"
h += f"<div class='panel-card'><b>VOLUMEN</b><span>Mínimo configurado: {_big(volumen_min_ui)}.</span></div>"
h += f"<div class='panel-card'><b>GAP</b><span>Rango configurado: {gap_min_ui:.1f}%–{gap_max_ui:.1f}%.</span></div>"
h += "</div></div>"
h += "<div id='panel-config' class='tab-panel'><div class='panel-grid'>"
h += f"<div class='panel-card'><b>MOTOR</b><span>{_safe_text(_estado_txt)} · Horario {_safe_text(_hora_txt)}</span></div>"
h += f"<div class='panel-card'><b>BROKER</b><span>{_safe_text(broker_val)}</span></div>"
h += f"<div class='panel-card'><b>VENTANA</b><span>{_safe_text(wnd_val)}</span></div>"
h += f"<div class='panel-card'><b>PUENTE DE LAYOUT</b><span>{_safe_text(bridge_val)}</span></div>"
h += "</div></div>"
h += "<div id='panel-resultados' class='tab-panel'><b>RESULTADOS EN VIVO</b><br>Las señales encontradas por el motor aparecen en la tabla de 10 líneas inferior.</div>"
h += "<div class='filtros-grid'>"
h += "<div class='logo'>TRADE SCANNER</div>"
h += f"<div class='filtro-item'><label>MOTOR</label><select id='cfg_active' onchange='pushConfig()'><option value='True' {'selected' if active_val=='True' else ''}>🟢 ON</option><option value='False' {'selected' if active_val=='False' else ''}>🔴 OFF</option></select></div>"
h += f"<div class='filtro-item'><label>HORARIO (ET)</label><div class='range'><input type='time' id='cfg_start' value='{start_time}'><span>–</span><input type='time' id='cfg_end' value='{end_time}'></div></div>"
h += f"<div class='filtro-item'><label>IDIOMA</label><select id='cfg_lang' onchange='pushConfig()'><option value='ESP' {'selected' if lang_val=='ESP' else ''}>ESP</option><option value='ENG' {'selected' if lang_val=='ENG' else ''}>ENG</option></select></div>"
h += f"<div class='filtro-item'><label>VENTANA</label><select id='cfg_wnd' onchange='pushConfig()'><option value='Incrustada' {'selected' if wnd_val=='Incrustada' else ''}>Incrustada</option><option value='Flotante' {'selected' if wnd_val=='Flotante' else ''}>Flotante</option></select></div>"
h += f"<div class='filtro-item'><label>PRECIO ($)</label><div class='range'><input type='number' step='0.01' id='price_min' value='{precio_min_ui:g}'><span>–</span><input type='number' step='0.01' id='price_max' value='{precio_max_ui:g}'></div></div>"
h += f"<div class='filtro-item'><label>GAP (%)</label><div class='range'><input type='number' step='0.1' id='gap_min' value='{gap_min_ui:g}'><span>–</span><input type='number' step='0.1' id='gap_max' value='{gap_max_ui:g}'></div></div>"
h += f"<div class='filtro-item'><label>FLOTACIÓN ≤</label><input type='number' id='float_max' value='{float_max_ui}'></div>"
h += f"<div class='filtro-item'><label>VOLUMEN ≥</label><input type='number' id='txt_vol' value='{volumen_min_ui}'></div>"
h += f"<div class='filtro-item'><label>CRUCE EMA</label><select id='sel_ema'><option value='Hacia arriba' {'selected' if ema_ui=='Hacia arriba' else ''}>Vela nueva sobre EMA20</option><option value='Hacia abajo' {'selected' if ema_ui=='Hacia abajo' else ''}>Hacia abajo</option><option value='Neutro' {'selected' if ema_ui=='Neutro' else ''}>Neutro</option></select></div>"
h += f"<div class='filtro-item'><label>MACD</label><select id='sel_mac'><option value='Positivo' {'selected' if macd_ui=='Positivo' else ''}>Positivo</option><option value='Negativo' {'selected' if macd_ui=='Negativo' else ''}>Negativo</option><option value='No exigir' {'selected' if macd_ui=='No exigir' else ''}>No exigir</option></select></div>"
h += f"<div class='filtro-item'><label>ORDENAR</label><select id='sel_order'><option value='Actualizado' {'selected' if orden_ui=='Actualizado' else ''}>Actualizado</option><option value='Cambio %' {'selected' if orden_ui=='Cambio %' else ''}>Cambio %</option><option value='Volumen' {'selected' if orden_ui=='Volumen' else ''}>Volumen</option></select></div>"
h += f"<div class='filtro-item'><label>BROKER</label><select id='cfg_broker'><option value='Interactive Brokers' {'selected' if broker_val in ('Interactive Brokers','Interactive Brokers (TWS)') else ''}>Interactive Brokers</option><option value='Tradestation' {'selected' if broker_val=='Tradestation' else ''}>Tradestation</option><option value='Otro' {'selected' if broker_val in ('Otro','Otro (webhook)') else ''}>Otro</option></select></div>"
h += f"<div class='filtro-item'><label>PUENTE</label><input type='text' id='cfg_url' value='{_safe_text(bridge_val)}' style='width:100%;'></div>"
h += "<div class='filtro-item' style='justify-content:center;'><button onclick='pushConfig()' style='width:100%;height:22px;'>APLICAR FILTROS</button></div>"
h += "</div>"
h += f"<div class='subline'><span><b>Señales:</b> {len(filas_reales)}</span><span><b>Precio:</b> ${precio_min_ui:.2f}–${precio_max_ui:.2f}</span><span><b>Gap:</b> {gap_min_ui:.1f}%–{gap_max_ui:.1f}%</span><span><b>Float:</b> ≤ {float_max_ui/1_000_000:.1f}M</span><span><b>Vol:</b> ≥ {_big(volumen_min_ui)}</span><span><b>EMA20:</b> { _safe_text(ema_ui) }</span><span><b>MACD:</b> { _safe_text(macd_ui) }</span></div>"
h += "<div class='result-title'>RESULTADOS · VISUALIZACIÓN · 10 LÍNEAS</div>"
h += "<div id='resultados-tabla' class='table-wrapper'><table><thead><tr>"
h += "<th class='layout-col'>⚙️ Layout</th><th>Ticker</th><th>Sector</th><th>Precio ($)</th><th>Cambio %</th><th>Volumen</th><th>Gap %</th><th>Flotación (M)</th><th>EMA20 (1 min)</th><th>MACD</th>"
h += "</tr></thead><tbody>" + rows_html + "</tbody></table></div>"
h += f"<div class='footer-note'><span>Motor real conectado · {len(filas_reales)} resultado(s) visible(s)</span><span>Último estado: {_safe_text(_estado_txt)} · { _safe_text(_hora_txt) }</span></div>"
h += "</div></body></html>"

# La carátula se muestra en un iframe aislado para que el CSS oscuro del shell
# anterior de Streamlit no pueda ocultarla. El motor sigue ejecutándose fuera.
components.html(h, height=1050, scrolling=True)
