"""Paper-only state machine for the short EMA pullback strategy.

Higher-timeframe EMA values must be supplied by TradeScanner's published
scanner_conditions. Missing EMA context always blocks entry; minute EMAs are
never substituted for daily/weekly EMAs.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any

STRATEGY_NAME = "Pullback corto ema50 ó 200 día ó semana"


def _number(mapping: dict, *keys: str) -> float | None:
    for key in keys:
        value = mapping.get(key)
        try:
            if value is not None and value != "":
                return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _gravestone(candle: dict | None) -> bool:
    """Deterministic gravestone test: small body near the low, long upper wick."""
    if not candle:
        return False
    o = _number(candle, "apertura", "open")
    h = _number(candle, "maximo", "high")
    l = _number(candle, "minimo", "low")
    c = _number(candle, "cierre", "close")
    if None in (o, h, l, c) or h <= l:
        return False
    body = abs(c - o)
    span = h - l
    upper = h - max(o, c)
    lower = min(o, c) - l
    return body <= span * 0.30 and upper >= max(body * 2.0, span * 0.50) and lower <= span * 0.15


def _dragonfly(candle: dict | None) -> bool:
    if not candle:
        return False
    o = _number(candle, "apertura", "open")
    h = _number(candle, "maximo", "high")
    l = _number(candle, "minimo", "low")
    c = _number(candle, "cierre", "close")
    if None in (o, h, l, c) or h <= l:
        return False
    body = abs(c - o)
    span = h - l
    lower = min(o, c) - l
    upper = h - max(o, c)
    return body <= span * 0.30 and lower >= max(body * 2.0, span * 0.50) and upper <= span * 0.15


def _ema_context(scanner: dict) -> tuple[float | None, list[tuple[str, float]]]:
    ema20 = _number(scanner, "ema20", "tecnico_ema20", "ema20_minuto")
    levels = []
    aliases = (
        ("EMA50 diaria", ("ema50_daily", "ema50_dia", "tecnico_ema50_dia")),
        ("EMA200 diaria", ("ema200_daily", "ema200_dia", "tecnico_ema200_dia")),
        ("EMA50 semanal", ("ema50_weekly", "ema50_semana", "tecnico_ema50_semana")),
        ("EMA200 semanal", ("ema200_weekly", "ema200_semana", "tecnico_ema200_semana")),
    )
    for label, keys in aliases:
        value = _number(scanner, *keys)
        if value is not None:
            levels.append((label, value))
    return ema20, levels


@dataclass
class PullbackCortoState:
    simbolo: str
    accion: str
    estado: str
    motivo: str
    precio: float | None = None
    stop_loss: float | None = None
    estrategia: str = STRATEGY_NAME
    position_open: bool = False


class PullbackCortoEMA:
    """Evaluates a live one-minute snapshot and tracks a simulated short."""

    def __init__(self) -> None:
        self.position_open = False
        self.entry_price: float | None = None
        self.stop_loss: float | None = None
        self.last_previous_candle: tuple | None = None
        self.last_action = "WAIT"
        self.last_reason = "Esperando candidato y contexto EMA diario/semanal."
        self.last_symbol = ""
        self.last_price: float | None = None
        self.last_candle_open: float | None = None

    @staticmethod
    def _result(symbol: str, action: str, state: str, reason: str,
                price: float | None = None, stop: float | None = None,
                opened: bool = False) -> dict:
        return asdict(PullbackCortoState(symbol, action, state, reason, price, stop,
                                         STRATEGY_NAME, opened))

    def evaluate(self, snapshot: dict[str, Any], scanner_conditions: dict | None = None) -> dict:
        symbol = str(snapshot.get("simbolo") or snapshot.get("symbol") or "").strip().upper()
        candle = snapshot.get("vela_actual") or {}
        previous = snapshot.get("vela_anterior") or {}
        price = _number(candle, "cierre", "close")
        open_price = _number(candle, "apertura", "open")
        high = _number(candle, "maximo", "high")
        low = _number(candle, "minimo", "low")
        prev_open = _number(previous, "apertura", "open")
        prev_high = _number(previous, "maximo", "high")
        prev_low = _number(previous, "minimo", "low")
        prev_close = _number(previous, "cierre", "close")
        scanner = scanner_conditions or {}
        tramo = snapshot.get("tramo_actual")
        try:
            tramo = int(tramo)
        except (TypeError, ValueError):
            tramo = 0

        if not symbol or price is None or open_price is None or high is None or low is None:
            return self._result(symbol, "WAIT", "sin_datos", "Esperando vela de un minuto y precio en vivo.", price, self.stop_loss, self.position_open)
        if snapshot.get("market_data_trade_age_sec") is not None:
            try:
                if float(snapshot["market_data_trade_age_sec"]) > 5:
                    return self._result(symbol, "WAIT", "feed_obsoleto", "Sin trades recientes; no se evalúa entrada ni salida.", price, self.stop_loss, self.position_open)
            except (TypeError, ValueError):
                return self._result(symbol, "WAIT", "feed_obsoleto", "Antigüedad del feed no válida.", price, self.stop_loss, self.position_open)

        candle_signature = (prev_open, prev_high, prev_low, prev_close)
        new_candle = candle_signature != self.last_previous_candle
        if new_candle and all(v is not None for v in (prev_open, prev_high, prev_low, prev_close)):
            self.last_previous_candle = candle_signature
            if self.position_open:
                # For a short, only lower the stop; never move it farther away.
                if prev_close > 0:
                    self.stop_loss = min(self.stop_loss, prev_close) if self.stop_loss is not None else prev_close
                # If the just-closed candle reached support/autocorrection, exit.
                body_mid = (prev_open + prev_close) / 2
                support_floor = prev_low
                if prev_close <= body_mid and prev_close >= support_floor:
                    self.position_open = False
                    self.entry_price = None
                    return self._result(symbol, "EXIT", "salida_soporte_autocorreccion",
                                         "La vela cerró en soporte/autocorrección.", price, self.stop_loss, False)

        if self.position_open:
            self.last_price = price
            if self.stop_loss is not None and price >= self.stop_loss:
                self.position_open = False
                self.entry_price = None
                return self._result(symbol, "EXIT", "stop_loss",
                                    "Precio alcanzó el stop loss; cierre inmediato del corto.", price, self.stop_loss, False)
            if tramo == 1 and price > open_price:
                self.position_open = False
                self.entry_price = None
                return self._result(symbol, "EXIT", "salida_tercio_1",
                                    "En los primeros 20 segundos superó la apertura y entró en positivo.", price, self.stop_loss, False)
            if tramo in (2, 3) and price >= open_price:
                self.position_open = False
                self.entry_price = None
                return self._result(symbol, "EXIT", f"salida_tercio_{tramo}",
                                    "El precio retrocedió hasta la apertura; cierre inmediato.", price, self.stop_loss, False)
            if _dragonfly(candle) and prev_low is not None and low > prev_low:
                self.position_open = False
                self.entry_price = None
                return self._result(symbol, "EXIT", "rebote_libelula",
                                    "Libélula con mínimo superior al de la vela anterior: posible rebote en soporte.", price, self.stop_loss, False)
            return self._result(symbol, "HOLD", f"corto_tercio_{tramo}",
                                "Mantener corto: reglas intraminuto no activaron salida.", price, self.stop_loss, True)

        ema20, levels = _ema_context(scanner)
        if ema20 is None:
            ema20 = _number(snapshot, "ema20")
        if ema20 is None or not levels:
            return self._result(symbol, "WAIT", "falta_contexto_ema",
                                "Entrada bloqueada: TradeScanner debe aportar EMA20 y EMA50/EMA200 diaria o semanal.", price)
        if price <= ema20:
            return self._result(symbol, "WAIT", "pullback_no_sobre_ema20",
                                "El pullback debe iniciar por encima de EMA20.", price)
        touched = [(label, level) for label, level in levels if prev_low is not None and prev_high is not None and prev_low <= level <= prev_high]
        if not touched:
            return self._result(symbol, "WAIT", "sin_toque_ema_mayor",
                                "La primera vela no toca EMA50/EMA200 diaria o semanal.", price)
        if prev_close is None or prev_open is None or prev_high is None or prev_low is None:
            return self._result(symbol, "WAIT", "falta_primera_vela",
                                "Esperando la primera vela cerrada que toque EMA y forme lápida negativa.", price)
        first = {"apertura": prev_open, "maximo": prev_high, "minimo": prev_low, "cierre": prev_close}
        if not (prev_close < prev_open and _gravestone(first)):
            return self._result(symbol, "WAIT", "primera_vela_no_valida",
                                "La primera vela no cerró negativa con forma de lápida.", price)
        if high >= prev_high or tramo != 1 or not _gravestone(candle):
            return self._result(symbol, "WAIT", "esperando_confirmacion_segunda_vela",
                                "Esperando lápida en los primeros 20 segundos con máximo inferior al anterior.", price)
        if prev_close <= price:
            return self._result(symbol, "WAIT", "stop_invalido",
                                "Entrada bloqueada: cierre de vela anterior no queda por encima del precio de entrada para un corto.", price)
        self.position_open = True
        self.entry_price = price
        self.stop_loss = prev_close
        self.last_symbol = symbol
        self.last_price = price
        self.last_candle_open = open_price
        return self._result(symbol, "SHORT", "corto_abierto_paper",
                            f"Entrada corta Paper confirmada: toque en {touched[0][0]} y lápida con máximo inferior.", price, self.stop_loss, True)

    def status(self) -> dict:
        return {
            "strategy": STRATEGY_NAME,
            "position_open": self.position_open,
            "symbol": self.last_symbol,
            "entry_price": self.entry_price,
            "stop_loss": self.stop_loss,
            "last_price": self.last_price,
            "last_action": self.last_action,
            "last_reason": self.last_reason,
            "real_trading_enabled": False,
        }
