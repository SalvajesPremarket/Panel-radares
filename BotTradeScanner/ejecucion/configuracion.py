"""Configuracion de ejecucion del bot LONG.

Esta capa describe COMO ejecutar una señal; la estrategia solo decide BUY/EXIT.
Por seguridad, queda deshabilitada para ordenes reales hasta una fase posterior.
El valor inicial solicitado es LIMIT al ASK.
"""
from __future__ import annotations

from dataclasses import dataclass


REFERENCIAS_LIMIT = (
    "ask",
    "ask_plus_0_01",
    "ask_plus_0_02",
    "bid",
    "bid_minus_0_01",
    "bid_minus_0_02",
    "custom",
)


@dataclass
class SesionEjecucion:
    modo: str = "limit"
    referencia_limit: str = "ask"
    offset_dolares: float = 0.0

    def validar(self, mercado: str = "regular") -> None:
        mercado = mercado.lower()
        if self.modo not in {"market", "limit"}:
            raise ValueError("modo debe ser market o limit")
        if self.referencia_limit not in REFERENCIAS_LIMIT:
            raise ValueError("referencia_limit no reconocida")
        if mercado in {"premarket", "postmarket"} and self.modo == "market":
            raise ValueError("market no esta permitido en extended hours")
        if self.modo == "limit" and self.referencia_limit == "custom" and self.offset_dolares < 0:
            raise ValueError("offset_dolares no puede ser negativo para custom")


@dataclass
class ExecutionConfig:
    premarket: SesionEjecucion
    regular: SesionEjecucion
    postmarket: SesionEjecucion
    capital_inicial: float = 600.0
    max_dolares_por_operacion: float = 120.0
    riesgo_por_operacion_pct: float = 1.0
    max_posiciones: int = 3
    max_exposicion_total_pct: float = 20.0
    max_acciones: int | None = None
    enabled: bool = False
    paper: bool = True

    @classmethod
    def por_defecto(cls) -> "ExecutionConfig":
        return cls(
            premarket=SesionEjecucion(),
            regular=SesionEjecucion(),
            postmarket=SesionEjecucion(),
        )

    def validar(self) -> None:
        self.premarket.validar("premarket")
        self.regular.validar("regular")
        self.postmarket.validar("postmarket")
        if self.capital_inicial <= 0:
            raise ValueError("capital_inicial debe ser positivo")
        if self.max_dolares_por_operacion <= 0:
            raise ValueError("max_dolares_por_operacion debe ser positivo")
        if self.riesgo_por_operacion_pct < 0:
            raise ValueError("riesgo_por_operacion_pct no puede ser negativo")
        if self.max_posiciones < 1:
            raise ValueError("max_posiciones debe ser >= 1")
        if self.max_exposicion_total_pct <= 0:
            raise ValueError("max_exposicion_total_pct debe ser positivo")
        if self.max_acciones is not None and self.max_acciones < 1:
            raise ValueError("max_acciones debe ser >= 1")


def precio_limit(
    *,
    ask: float | None,
    bid: float | None,
    referencia: str = "ask",
    offset_dolares: float = 0.0,
) -> float | None:
    """Calcula el precio limite a partir del quote actual, nunca del candle."""
    if referencia == "ask":
        return float(ask) if ask is not None and ask > 0 else None
    if referencia == "ask_plus_0_01":
        return round(float(ask) + 0.01, 4) if ask is not None and ask > 0 else None
    if referencia == "ask_plus_0_02":
        return round(float(ask) + 0.02, 4) if ask is not None and ask > 0 else None
    if referencia == "bid":
        return float(bid) if bid is not None and bid > 0 else None
    if referencia == "bid_minus_0_01":
        return round(float(bid) - 0.01, 4) if bid is not None and bid > 0 else None
    if referencia == "bid_minus_0_02":
        return round(float(bid) - 0.02, 4) if bid is not None and bid > 0 else None
    if referencia == "custom":
        if ask is None or ask <= 0:
            return None
        return round(float(ask) + float(offset_dolares), 4)
    raise ValueError(f"referencia_limit no reconocida: {referencia}")


def cantidad_por_presupuesto(
    precio: float,
    *,
    max_dolares: float,
    max_acciones: int | None = None,
) -> int:
    if precio <= 0 or max_dolares <= 0:
        return 0
    cantidad = int(max_dolares // precio)
    if max_acciones is not None:
        cantidad = min(cantidad, max_acciones)
    return max(0, cantidad)
