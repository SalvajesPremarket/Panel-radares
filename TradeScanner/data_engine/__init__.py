"""Capa de datos de mercado en tiempo real para TradeScanner.

El paquete es independiente de la interfaz Streamlit. El scanner puede adoptarlo
por etapas sin cambiar su motor actual ni su UI.
"""

from .market_cache import MarketCache
from .bar_builder import LiveBarBuilder
from .data_health import DataHealth
from .market_stream import AlpacaMarketStream

__all__ = ["MarketCache", "LiveBarBuilder", "DataHealth", "AlpacaMarketStream"]
