"""Estrategia LONG PreMarketSalvajes.

IMPORTANTE:
- Esta clase NO envia ordenes.
- Solo interpreta el snapshot del MotorVelas y mantiene el estado de la
  secuencia LONG.
- SHORT no forma parte de esta estrategia en esta fase.
- La estrategia se llama PreMarketSalvajes por identificacion, pero funciona
  durante las 24 horas; el nombre NO limita el horario.

Reglas implementadas:
1. Candidato: vela nueva abre sobre EMA20, MACD positivo y EMA50/EMA200 no
   quedan entre EMA20 y la banda de Bollinger superior.
2. Entrada: durante tramo 1, la vela primero baja de su apertura y regresa a
   la apertura (libelula) -> BUY.
3. Primera vela despues de entrada: mantener en tramo 2 si sigue positiva;
   en tramo 3, cerrar si regresa a apertura.
4. Segunda vela: mantener si es positiva y su minimo es mayor al minimo de la
   vela anterior. En tramo 1, stop = cierre de la vela de entrada.
5. Tercera y siguientes: positiva + higher low -> mantener; stop = cierre de
   la vela inmediatamente anterior.
6. Bollinger superior no cierra por si sola. Si toca/pierce y sigue positiva,
   mantener. Si aparece lapida, cerrar.
7. Pullback/reentrada LONG: despues de un cierre, se puede vigilar EMA9/EMA20
   como soporte y esperar una nueva vela con higher low + higher high. Esta
   pieza no abre automaticamente una segunda posicion: devuelve la condicion
   para que la maquina de estados la autorice.
8. No se inventan reglas de igualdad para highs/lows.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class EstadoLong(str, Enum):
    ESPERANDO_CANDIDATO = "esperando_candidato"
    ESPERANDO_LIBELULA = "esperando_libelula"
    LONG_PRIMERA_VELA = "long_primera_vela"
    LONG_SEGUNDA_VELA = "long_segunda_vela"
    LONG_SIGUIENTES = "long_siguientes"
    PULLBACK_LONG = "pullback_long"
    CERRADO = "cerrado"


@dataclass(frozen=True)
class DecisionLong:
    accion: str
    estado: EstadoLong
    motivo: str
    stop_loss: float | None = None
    candidato_valido: bool = False
    pullback_reentrada: bool = False


class PreMarketSalvajesLong:
    """Maquina de reglas LONG, sin dependencia de Alpaca ni Streamlit."""

    def __init__(self) -> None:
        self.estado = EstadoLong.ESPERANDO_CANDIDATO
        self.simbolo: str | None = None
        self.precio_entrada: float | None = None
        self.cierre_vela_entrada: float | None = None
        self.stop_loss: float | None = None
        self._minimo_vela_entrada: float | None = None
        self._ultimo_minimo: float | None = None
        self._ultimo_maximo: float | None = None

    @staticmethod
    def _vela(snap: dict[str, Any]) -> dict[str, Any] | None:
        return snap.get("vela_actual")

    @staticmethod
    def _numero(snap: dict[str, Any], key: str) -> float | None:
        value = snap.get(key)
        return float(value) if value is not None else None

    def _candidato_valido(self, snap: dict[str, Any]) -> tuple[bool, str]:
        vela = self._vela(snap)
        if not vela:
            return False, "Sin vela en curso."

        apertura = vela.get("apertura")
        ema20 = self._numero(snap, "ema20_anterior") or self._numero(snap, "ema20")
        macd = self._numero(snap, "macd")
        ema50 = self._numero(snap, "ema50_anterior") or self._numero(snap, "ema50")
        ema200 = self._numero(snap, "ema200_anterior") or self._numero(snap, "ema200")
        banda_sup = self._numero(snap, "banda_bollinger_superior_anterior") or self._numero(snap, "banda_bollinger_superior")

        if apertura is None or ema20 is None:
            return False, "Faltan apertura o EMA20."

        # 'Naciendo por encima de EMA20' se interpreta literalmente como
        # apertura > EMA20.
        if not apertura > ema20:
            return False, "La nueva vela no abre por encima de EMA20."

        # MACD: solamente positivo. Histograma verde creciente NO es requisito.
        if macd is None or not macd > 0:
            return False, "MACD no es positivo."

        # Si faltan EMA50/EMA200 o Bollinger por historial insuficiente, no se
        # inventa una confirmacion.
        if ema50 is None or ema200 is None or banda_sup is None:
            return False, "Faltan EMA50/EMA200/Bollinger para validar el camino."

        # Regla literal de la estrategia: EMA50/EMA200 no deben quedar
        # entre EMA20 y ninguna de las bandas (superior o inferior).
        banda_inf = self._numero(snap, "banda_bollinger_inferior_anterior") or self._numero(snap, "banda_bollinger_inferior")
        if banda_inf is None:
            return False, "Falta Bollinger inferior para validar el camino."
        for nombre, ema in (("EMA50", ema50), ("EMA200", ema200)):
            entre_superior = ema20 < ema < banda_sup
            entre_inferior = banda_inf < ema < ema20
            if entre_superior or entre_inferior:
                return False, f"{nombre} esta entre EMA20 y una banda de Bollinger."

        return True, "Candidato LONG valido."

    def evaluar_candidato(self, snap: dict[str, Any]) -> DecisionLong:
        valido, motivo = self._candidato_valido(snap)
        if not valido:
            return DecisionLong("WAIT", self.estado, motivo, candidato_valido=False)

        self.simbolo = str(snap.get("simbolo", "")).upper() or self.simbolo
        self.estado = EstadoLong.ESPERANDO_LIBELULA
        return DecisionLong(
            "WATCH",
            self.estado,
            "Candidato valido; esperar libelula en tramo 1.",
            candidato_valido=True,
        )

    def evaluar(self, snap: dict[str, Any]) -> DecisionLong:
        """Evalua un snapshot y devuelve solo WAIT/WATCH/BUY/HOLD/EXIT."""

        if not snap or snap.get("sin_datos"):
            return DecisionLong("WAIT", self.estado, "Sin datos del motor.")

        vela = self._vela(snap)
        if not vela:
            return DecisionLong("WAIT", self.estado, "No hay vela en curso.")

        tramo = snap.get("tramo_actual")
        apertura = vela.get("apertura")
        cierre = vela.get("cierre")
        minimo = vela.get("minimo")
        maximo = vela.get("maximo")
        positiva = bool(vela.get("es_positiva"))
        regreso = bool(vela.get("regreso_a_apertura"))
        # El primer trade de una vela siempre puede tener cierre == apertura.
        # Eso no significa que haya regresado: debe existir excursion previa.
        regreso_real = (
            regreso
            and apertura is not None
            and maximo is not None
            and maximo > apertura
        )
        libelula = bool(vela.get("es_libelula_en_curso"))
        lapida = bool(vela.get("es_lapida_en_curso"))
        banda_sup = self._numero(snap, "banda_bollinger_superior")

        if self.estado == EstadoLong.ESPERANDO_CANDIDATO:
            valido, motivo = self._candidato_valido(snap)
            if not valido:
                return DecisionLong("WAIT", self.estado, motivo, candidato_valido=False)

            self.simbolo = str(snap.get("simbolo", "")).upper() or self.simbolo
            self.estado = EstadoLong.ESPERANDO_LIBELULA

            # Si el candidato y la libelula llegan en el mismo snapshot,
            # la estrategia puede confirmar la entrada inmediatamente.
            # La MaquinaDecisionesLong conserva deliberadamente el paso
            # candidato -> WATCH para separar el scanner de la entrada.
            if snap.get("tramo_actual") == 1 and bool(vela.get("es_libelula_en_curso")):
                return self._entrar_long(vela)

            return DecisionLong(
                "WATCH",
                self.estado,
                "Candidato valido; esperar libelula en tramo 1.",
                candidato_valido=True,
            )

        if self.estado == EstadoLong.ESPERANDO_LIBELULA:
            # La entrada solo puede ocurrir en el primer tramo.
            if tramo != 1:
                return DecisionLong("WAIT", self.estado, "La libelula de entrada debe formarse en tramo 1.")

            if lapida:
                return DecisionLong("WAIT", self.estado, "La vela formo lapida; no entrar.")

            if libelula:
                return self._entrar_long(vela)

            return DecisionLong("WATCH", self.estado, "Esperando libelula.")

        if self.estado == EstadoLong.LONG_PRIMERA_VELA:
            # Si el precio primero fue positivo y luego vuelve a apertura,
            # cerrar/no mantener la posicion.
            if regreso_real and tramo == 3:
                return self._cerrar("Primera vela regreso a apertura en tramo 3.")

            if lapida and tramo == 3:
                return self._cerrar("Primera vela formo lapida en tramo 3.")

            if tramo in (2, 3) and positiva:
                if tramo == 3:
                    self.estado = EstadoLong.LONG_SEGUNDA_VELA
                    return DecisionLong(
                        "HOLD",
                        self.estado,
                        "Primera vela consolida positiva; esperar segunda vela.",
                        stop_loss=self.stop_loss,
                    )
                return DecisionLong("HOLD", self.estado, "Primera vela sigue positiva.", stop_loss=self.stop_loss)

            return DecisionLong("HOLD", self.estado, "Mantener primera vela mientras no active salida.", stop_loss=self.stop_loss)

        if self.estado == EstadoLong.LONG_SEGUNDA_VELA:
            # La siguiente vela debe ser positiva y tener higher low.
            if regreso_real:
                return self._cerrar("Segunda vela regreso a su apertura.")

            anterior_min = snap.get("vela_anterior", {}).get("minimo")
            if positiva and anterior_min is not None and minimo is not None and minimo > anterior_min:
                if tramo == 1:
                    self.stop_loss = self.cierre_vela_entrada
                    return DecisionLong(
                        "HOLD",
                        self.estado,
                        "Segunda vela valida; stop sube al cierre de la vela de entrada.",
                        stop_loss=self.stop_loss,
                    )

                self.estado = EstadoLong.LONG_SIGUIENTES
                return DecisionLong(
                    "HOLD",
                    self.estado,
                    "Segunda vela valida: positiva + higher low.",
                    stop_loss=self.stop_loss,
                )

            return DecisionLong("WAIT", self.estado, "Esperando segunda vela positiva con higher low.")

        if self.estado == EstadoLong.LONG_SIGUIENTES:
            if regreso_real:
                return self._cerrar("La vela regreso a su apertura.")

            # Bollinger superior no es salida por si sola.
            toca_bollinger = (
                banda_sup is not None
                and maximo is not None
                and maximo >= banda_sup
            )

            if toca_bollinger and lapida:
                return self._cerrar("Bollinger superior + lapida: cerrar LONG.")

            if positiva and bool(snap.get("minimo_supera_anterior")):
                if snap.get("vela_anterior", {}).get("cierre") is not None:
                    self.stop_loss = float(snap["vela_anterior"]["cierre"])
                self._ultimo_minimo = minimo
                self._ultimo_maximo = maximo
                return DecisionLong(
                    "HOLD",
                    self.estado,
                    "Vela positiva con higher low; stop al cierre de la vela anterior.",
                    stop_loss=self.stop_loss,
                )

            if toca_bollinger and positiva:
                return DecisionLong(
                    "HOLD",
                    self.estado,
                    "Toco/piercio Bollinger superior pero continua positiva.",
                    stop_loss=self.stop_loss,
                )

            return DecisionLong("WAIT", self.estado, "Esperando continuacion positiva con higher low.", stop_loss=self.stop_loss)

        if self.estado == EstadoLong.PULLBACK_LONG:
            return self._evaluar_pullback(snap)

        if self.estado == EstadoLong.CERRADO:
            return DecisionLong("WAIT", self.estado, "Ciclo cerrado; buscar nuevo candidato.")

        return DecisionLong("WAIT", self.estado, "Estado no accionable.")

    def _entrar_long(self, vela: dict[str, Any]) -> DecisionLong:
        cierre = vela.get("cierre")
        minimo = vela.get("minimo")
        if cierre is None:
            return DecisionLong("WAIT", self.estado, "Libelula sin precio de cierre.")
        self.precio_entrada = float(cierre)
        self.cierre_vela_entrada = float(cierre)
        self._minimo_vela_entrada = float(minimo) if minimo is not None else None
        self.stop_loss = float(cierre)
        self.estado = EstadoLong.LONG_PRIMERA_VELA
        return DecisionLong(
            "BUY",
            self.estado,
            "Libelula confirmada: vela bajo de apertura y regreso a la apertura.",
            stop_loss=self.stop_loss,
        )

    def marcar_salida_para_pullback(self) -> None:
        """Deja el motor en modo de vigilancia de reentrada LONG."""
        self.estado = EstadoLong.PULLBACK_LONG
        self.precio_entrada = None
        self.cierre_vela_entrada = None
        self.stop_loss = None

    def _evaluar_pullback(self, snap: dict[str, Any]) -> DecisionLong:
        vela = self._vela(snap) or {}
        cierre = vela.get("cierre")
        minimo = vela.get("minimo")
        maximo = vela.get("maximo")
        ema9 = self._numero(snap, "ema9")
        ema20 = self._numero(snap, "ema20")

        soporte_ema = False
        if minimo is not None:
            if ema9 is not None and minimo <= ema9:
                soporte_ema = True
            if ema20 is not None and minimo <= ema20:
                soporte_ema = True

        hl = bool(snap.get("minimo_supera_anterior"))
        hh = bool(snap.get("maximo_supera_anterior"))
        reentrada = soporte_ema and hl and hh

        if reentrada:
            self.estado = EstadoLong.ESPERANDO_LIBELULA
            return DecisionLong(
                "WATCH",
                self.estado,
                "Pullback en EMA9/EMA20 y nueva vela con higher low + higher high; vigilar nueva entrada LONG.",
                pullback_reentrada=True,
            )

        return DecisionLong("WAIT", self.estado, "Esperando pullback a EMA9/EMA20 y nueva estructura LONG.")

    def _cerrar(self, motivo: str) -> DecisionLong:
        self.estado = EstadoLong.PULLBACK_LONG
        stop = self.stop_loss
        self.precio_entrada = None
        self.cierre_vela_entrada = None
        self.stop_loss = None
        return DecisionLong("EXIT", self.estado, motivo, stop_loss=stop)


def evaluar_premarket_salvajes(snap: dict[str, Any], estrategia: PreMarketSalvajesLong) -> dict[str, Any]:
    """Adaptador simple para consumir la estrategia desde una maquina externa."""
    decision = estrategia.evaluar(snap)
    return {
        "accion": decision.accion,
        "estado": decision.estado.value,
        "motivo": decision.motivo,
        "stop_loss": decision.stop_loss,
        "candidato_valido": decision.candidato_valido,
        "pullback_reentrada": decision.pullback_reentrada,
        "simbolo": snap.get("simbolo"),
    }
