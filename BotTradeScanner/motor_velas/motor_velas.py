"""
motor_velas.py
================
Motor de velas de 1 minuto en tiempo real, construido a partir del stream de
operaciones (trades) de Alpaca. No usa velas ya hechas: arma cada vela
operación por operación, y sabe en qué tramo de 20 segundos va en todo
momento (Tramo 1: seg 0-20, Tramo 2: seg 21-40, Tramo 3: seg 41-60).

Esta pieza NO toma decisiones de compra/venta. Solo observa el mercado y
expone el estado de cada vela para que otra pieza (la máquina de estados
del algoritmo) decida qué hacer.
"""

from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from threading import Lock
import statistics


# ==========================================
# 📊 ESTRUCTURA DE UNA VELA
# ==========================================
@dataclass
class Vela:
    simbolo: str
    inicio_minuto: datetime  # timestamp del minuto al que pertenece (truncado)
    apertura: float = None
    maximo: float = None
    minimo: float = None
    cierre: float = None
    volumen: float = 0.0
    num_operaciones: int = 0
    cerrada: bool = False

    def actualizar(self, precio: float, tamano: float):
        if self.apertura is None:
            self.apertura = precio
            self.maximo = precio
            self.minimo = precio
        else:
            self.maximo = max(self.maximo, precio)
            self.minimo = min(self.minimo, precio)
        self.cierre = precio
        self.volumen += tamano
        self.num_operaciones += 1

    # ---- Patrones sobre la vela EN CURSO (mientras se forma) ----
    def es_libelula_en_curso(self, margen_pct: float = 0.05) -> bool:
        """
        'Libélula' (dragonfly): abrió, se fue negativa (hizo mínimo debajo de
        apertura), y regresó cerca de su apertura. margen_pct define qué tan
        cerca de la apertura cuenta como 'regresó' (0.05% por defecto).
        """
        if self.apertura is None or self.cierre is None:
            return False
        fue_negativa = self.minimo < self.apertura
        cerca_de_apertura = abs(self.cierre - self.apertura) <= (self.apertura * margen_pct / 100)
        return fue_negativa and cerca_de_apertura

    def es_lapida_en_curso(self, margen_pct: float = 0.05) -> bool:
        """
        'Lápida' (gravestone): subió (hizo máximo arriba de apertura) y
        regresó/cayó por debajo de su apertura.
        """
        if self.apertura is None or self.cierre is None:
            return False
        fue_positiva = self.maximo > self.apertura
        regreso_o_cayo = self.cierre <= self.apertura
        return fue_positiva and regreso_o_cayo

    def es_positiva(self) -> bool:
        if self.apertura is None or self.cierre is None:
            return False
        return self.cierre > self.apertura

    def regreso_a_apertura(self, margen_pct: float = 0.05) -> bool:
        if self.apertura is None or self.cierre is None:
            return False
        return abs(self.cierre - self.apertura) <= (self.apertura * margen_pct / 100)


# ==========================================
# 🕒 CÁLCULO DE TRAMO (0-20s / 21-40s / 41-60s)
# ==========================================
def calcular_tramo(momento: datetime) -> int:
    """Devuelve 1, 2 o 3 según el segundo dentro del minuto actual."""
    segundo = momento.second
    if segundo <= 20:
        return 1
    elif segundo <= 40:
        return 2
    else:
        return 3


def truncar_al_minuto(momento: datetime) -> datetime:
    return momento.replace(second=0, microsecond=0)


# ==========================================
# 📈 INDICADORES (EMA, MACD, Bandas de Bollinger)
# ==========================================
def calcular_ema(precios: list, periodo: int):
    """EMA simple sobre una lista de cierres. Devuelve None si no hay datos
    suficientes."""
    if len(precios) < periodo:
        return None
    multiplicador = 2 / (periodo + 1)
    ema = sum(precios[:periodo]) / periodo
    for precio in precios[periodo:]:
        ema = (precio - ema) * multiplicador + ema
    return ema


def calcular_macd(precios: list, rapida=12, lenta=26, señal=9):
    """Devuelve (linea_macd, linea_señal, histograma) o (None, None, None)
    si no hay datos suficientes."""
    if len(precios) < lenta + señal:
        return None, None, None

    def serie_ema(datos, periodo):
        multiplicador = 2 / (periodo + 1)
        resultado = [sum(datos[:periodo]) / periodo]
        for precio in datos[periodo:]:
            resultado.append((precio - resultado[-1]) * multiplicador + resultado[-1])
        return resultado

    ema_rapida = serie_ema(precios, rapida)
    ema_lenta = serie_ema(precios, lenta)
    recorte = min(len(ema_rapida), len(ema_lenta))
    macd_serie = [ema_rapida[-recorte:][i] - ema_lenta[-recorte:][i] for i in range(recorte)]

    if len(macd_serie) < señal:
        return macd_serie[-1], None, None

    señal_serie = serie_ema(macd_serie, señal)
    linea_macd = macd_serie[-1]
    linea_señal = señal_serie[-1]
    histograma = linea_macd - linea_señal
    return linea_macd, linea_señal, histograma


def calcular_bandas_bollinger(precios: list, periodo=20, desviaciones=2):
    if len(precios) < periodo:
        return None, None, None
    ventana = precios[-periodo:]
    media = sum(ventana) / periodo
    desviacion_std = statistics.pstdev(ventana)
    banda_superior = media + desviaciones * desviacion_std
    banda_inferior = media - desviaciones * desviacion_std
    return banda_superior, media, banda_inferior


# ==========================================
# 🧠 MOTOR: UNA INSTANCIA POR SÍMBOLO
# ==========================================
class MotorVelasSimbolo:
    """Mantiene el historial de velas cerradas y la vela en curso de UN
    símbolo. Es seguro llamarlo desde el hilo del websocket."""

    def __init__(self, simbolo: str, historial_maximo: int = 300):
        self.simbolo = simbolo
        self.historial: list[Vela] = []
        self.vela_actual: Vela = None
        self.historial_maximo = historial_maximo
        self._lock = Lock()
        self._tramo_actual = None

    def cargar_historial(self, barras):
        """
        Carga barras históricas de 1 minuto como velas CERRADAS.
        No crea una vela_actual: esa se crea únicamente cuando llegue
        el primer trade nuevo por websocket.
        """
        with self._lock:
            nuevas = []

            for barra in barras:
                momento = barra.timestamp
                if momento.tzinfo is None:
                    momento = momento.replace(tzinfo=timezone.utc)

                vela = Vela(
                    simbolo=self.simbolo,
                    inicio_minuto=truncar_al_minuto(momento),
                    apertura=float(barra.open),
                    maximo=float(barra.high),
                    minimo=float(barra.low),
                    cierre=float(barra.close),
                    volumen=float(barra.volume),
                    num_operaciones=int(barra.trade_count or 0),
                    cerrada=True,
                )
                nuevas.append(vela)

            # Orden cronológico y solo las últimas N
            nuevas.sort(key=lambda v: v.inicio_minuto)
            self.historial = nuevas[-self.historial_maximo:]

    def procesar_trade(self, precio: float, tamano: float, momento: datetime):
        with self._lock:
            minuto = truncar_al_minuto(momento)

            if self.vela_actual is None:
                self.vela_actual = Vela(simbolo=self.simbolo, inicio_minuto=minuto)

            elif minuto > self.vela_actual.inicio_minuto:
                # Cambió el minuto: cerrar la vela anterior y abrir una nueva
                self.vela_actual.cerrada = True
                self.historial.append(self.vela_actual)
                if len(self.historial) > self.historial_maximo:
                    self.historial.pop(0)
                self.vela_actual = Vela(simbolo=self.simbolo, inicio_minuto=minuto)

            self.vela_actual.actualizar(precio, tamano)
            self._tramo_actual = calcular_tramo(momento)

    def snapshot(self) -> dict:
        """Devuelve el estado actual, incluso si todavía no llegó un trade en vivo."""
        with self._lock:
            cierres = [v.cierre for v in self.historial if v.cierre is not None]

            # Si existe vela en curso, sus precios participan en los indicadores.
            precios = cierres + (
                [self.vela_actual.cierre]
                if self.vela_actual is not None and self.vela_actual.cierre is not None
                else []
            )

            ema9 = calcular_ema(precios, 9)
            ema20 = calcular_ema(precios, 20)
            ema50 = calcular_ema(precios, 50)
            ema200 = calcular_ema(precios, 200)
            macd, señal, histograma = calcular_macd(precios)
            banda_sup, banda_media, banda_inf = calcular_bandas_bollinger(precios)

            vela_anterior = self.historial[-1] if self.historial else None

            vela_actual_data = None
            if self.vela_actual is not None:
                vela_actual_data = {
                    "apertura": self.vela_actual.apertura,
                    "maximo": self.vela_actual.maximo,
                    "minimo": self.vela_actual.minimo,
                    "cierre": self.vela_actual.cierre,
                    "es_libelula_en_curso": self.vela_actual.es_libelula_en_curso(),
                    "es_lapida_en_curso": self.vela_actual.es_lapida_en_curso(),
                    "es_positiva": self.vela_actual.es_positiva(),
                    "regreso_a_apertura": self.vela_actual.regreso_a_apertura(),
                }

            minimo_actual = (
                self.vela_actual.minimo
                if self.vela_actual is not None
                else None
            )
            maximo_actual = (
                self.vela_actual.maximo
                if self.vela_actual is not None
                else None
            )

            return {
                "simbolo": self.simbolo,
                "sin_datos": len(self.historial) == 0 and self.vela_actual is None,
                "tramo_actual": self._tramo_actual,
                "vela_actual": vela_actual_data,
                "vela_anterior": None if vela_anterior is None else {
                    "apertura": vela_anterior.apertura,
                    "maximo": vela_anterior.maximo,
                    "minimo": vela_anterior.minimo,
                    "cierre": vela_anterior.cierre,
                },
                "minimo_supera_anterior": (
                    vela_anterior is not None
                    and minimo_actual is not None
                    and minimo_actual > vela_anterior.minimo
                ),
                "maximo_supera_anterior": (
                    vela_anterior is not None
                    and maximo_actual is not None
                    and maximo_actual > vela_anterior.maximo
                ),
                "ema9": ema9,
                "ema20": ema20,
                "ema50": ema50,
                "ema200": ema200,
                "macd": macd,
                "macd_señal": señal,
                "macd_histograma": histograma,
                "banda_bollinger_superior": banda_sup,
                "banda_bollinger_media": banda_media,
                "banda_bollinger_inferior": banda_inf,
                "num_velas_historial": len(self.historial),
            }



# ==========================================
# 🌐 MOTOR GENERAL: MANEJA VARIOS SÍMBOLOS + CONEXIÓN A ALPACA
# ==========================================
class MotorVelas:
    """
    Orquesta un MotorVelasSimbolo por cada ticker que se esté vigilando,
    y mantiene la conexión al websocket de Alpaca (feed IEX, tiempo real,
    incluido en el plan gratuito).
    """

    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key
        self.motores: dict = {}
        self._stream = None
        self._simbolos_suscritos: set = set()
        self._iniciado = False
        self._historial_precargado: set = set()

        # Diagnóstico de la conexión (útil para validar que llegan datos)
        self.total_trades: int = 0
        self.ultimo_trade: datetime = None

    def _obtener_motor(self, simbolo: str) -> MotorVelasSimbolo:
        if simbolo not in self.motores:
            self.motores[simbolo] = MotorVelasSimbolo(simbolo)
        return self.motores[simbolo]

    def precargar_historial(self, simbolos: list, cantidad: int = 300):
        """
        Descarga barras históricas de 1 minuto y las coloca como velas cerradas.

        Se deja un margen de 15 minutos para no incorporar una barra que pueda
        seguir abierta/delayed en el feed. La API puede devolver menos barras
        dependiendo del plan, del feed y de la disponibilidad histórica.
        """
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame
        from alpaca.data.enums import DataFeed

        cliente = StockHistoricalDataClient(self.api_key, self.secret_key)

        ahora = datetime.now(timezone.utc)
        fin = ahora - timedelta(minutes=15)
        inicio = fin - timedelta(days=7)

        for simbolo in simbolos:
            if simbolo in self._historial_precargado:
                continue

            request = StockBarsRequest(
                symbol_or_symbols=simbolo,
                timeframe=TimeFrame.Minute,
                start=inicio,
                end=fin,
                limit=cantidad,
                feed=DataFeed.IEX,
            )

            respuesta = cliente.get_stock_bars(request)
            barras = respuesta.data.get(simbolo, [])

            motor = self._obtener_motor(simbolo)
            motor.cargar_historial(barras)

            self._historial_precargado.add(simbolo)

    async def _al_recibir_trade(self, trade):
        motor = self._obtener_motor(trade.symbol)
        momento = trade.timestamp
        if momento.tzinfo is None:
            momento = momento.replace(tzinfo=timezone.utc)
        motor.procesar_trade(precio=float(trade.price), tamano=float(trade.size), momento=momento)
        self.total_trades += 1
        self.ultimo_trade = momento

    def iniciar(self, simbolos: list):
        """Arranca la conexión websocket y se suscribe a los símbolos dados.
        Debe correr en un hilo aparte (es bloqueante). Si ya está iniciado,
        no hace nada (evita conexiones duplicadas)."""
        if self._iniciado:
            return
        self._iniciado = True

        try:
            # Primero cargamos memoria histórica para que los indicadores
            # estén disponibles antes de recibir el primer trade en vivo.
            self.precargar_historial(simbolos, cantidad=300)

            from alpaca.data.live import StockDataStream
            from alpaca.data.enums import DataFeed

            self._stream = StockDataStream(self.api_key, self.secret_key, feed=DataFeed.IEX)
            for simbolo in simbolos:
                self._stream.subscribe_trades(self._al_recibir_trade, simbolo)
                self._simbolos_suscritos.add(simbolo)
            self._stream.run()
        finally:
            # Si la conexión se cae o falla al arrancar, permitir reintentar
            self._iniciado = False
            self._stream = None

    def agregar_simbolo_en_caliente(self, simbolo: str):
        """Para agregar un ticker nuevo sin reiniciar la conexión completa."""
        if self._stream is not None and simbolo not in self._simbolos_suscritos:
            self._stream.subscribe_trades(self._al_recibir_trade, simbolo)
            self._simbolos_suscritos.add(simbolo)

    def snapshot_simbolo(self, simbolo: str) -> dict:
        if simbolo not in self.motores:
            return {"simbolo": simbolo, "sin_datos": True}
        return self.motores[simbolo].snapshot()

    def simbolos_activos(self) -> list:
        return list(self.motores.keys())
