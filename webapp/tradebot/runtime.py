"""Opt-in, Paper-only runtime for the deployed TradeBot.

The runtime is intentionally inert unless an operator explicitly enables it
and configures Alpaca market-data credentials in the hosting environment.
No broker executor is created and real order submission remains disabled.
"""
from __future__ import annotations

import os
from threading import Event, Lock, Thread
import time
from typing import Any


class TradeBotPaperRuntime:
    def __init__(self) -> None:
        self._lock = Lock()
        self._stop = Event()
        self._thread: Thread | None = None
        self._bot: Any = None
        self._bridge: Any = None
        self._state = "disabled"
        self._last_error = ""
        self._started_at: float | None = None
        self._last_sync_at: float | None = None
        self._candidate_count = 0

    @staticmethod
    def configuration() -> dict:
        key = os.getenv("ALPACA_API_KEY", "").strip()
        secret = os.getenv("ALPACA_SECRET_KEY", "").strip()
        enabled = os.getenv("TRADESCANNER_TRADEBOT_AUTO_PAPER", "").strip().lower() in {"1", "true", "yes", "on"}
        return {
            "enabled": enabled,
            "credentials_configured": bool(key and secret),
            "feed": "iex",
            "real_trading_enabled": False,
        }

    def start_if_configured(self) -> None:
        config = self.configuration()
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            if not config["enabled"]:
                self._state = "disabled"
                return
            if not config["credentials_configured"]:
                self._state = "needs_credentials"
                self._last_error = "Configura ALPACA_API_KEY y ALPACA_SECRET_KEY en Render."
                return
            self._state = "starting"
            self._last_error = ""
            self._stop.clear()
            self._thread = Thread(target=self._run, name="tradebot-paper-runtime", daemon=True)
            self._thread.start()

    def _run(self) -> None:
        try:
            from BotTradeScanner.integracion.live_motor_bridge import MotorVelasBridge
            from BotTradeScanner.integracion.bot_long_realtime import BotLongRealtime
            from BotTradeScanner.ejecucion.configuracion import ExecutionConfig
            from webapp.api.signal_service import store

            config = self.configuration()
            bridge = MotorVelasBridge(
                api_key=os.getenv("ALPACA_API_KEY", "").strip(),
                secret_key=os.getenv("ALPACA_SECRET_KEY", "").strip(),
            )
            execution_config = ExecutionConfig.por_defecto()
            execution_config.enabled = False
            execution_config.paper = True
            bot = BotLongRealtime(
                bridge,
                intervalo_segundos=1.0,
                execution_config=execution_config,
                executor=None,
            )
            bot.iniciar()
            with self._lock:
                self._bridge = bridge
                self._bot = bot
                self._state = "running"
                self._started_at = time.time()

            while not self._stop.wait(5.0):
                signals = store.list(limit=100)
                # Strategy remains responsible for every entry/exit decision.
                bot.sync_signals(signals)
                with self._lock:
                    self._candidate_count = len({str(s.get("symbol") or s.get("simbolo") or "").upper() for s in signals if s.get("symbol") or s.get("simbolo")})
                    self._last_sync_at = time.time()
        except Exception as exc:
            with self._lock:
                self._state = "error"
                self._last_error = f"{type(exc).__name__}: {exc}"[:500]

    def status(self) -> dict:
        config = self.configuration()
        with self._lock:
            bridge = self._bridge
            thread = self._thread
            state = self._state
            last_error = self._last_error
            started_at = self._started_at
            last_sync_at = self._last_sync_at
            candidate_count = self._candidate_count
        bridge_status = {}
        if bridge is not None:
            try:
                bridge_status = bridge.status()
            except Exception as exc:
                bridge_status = {"error": f"{type(exc).__name__}: {exc}"[:300]}
        enabled = bool(config["enabled"] and config["credentials_configured"])
        return {
            "enabled": config["enabled"],
            "credentials_configured": config["credentials_configured"],
            "feed": config["feed"],
            "state": state,
            "thread_alive": bool(thread and thread.is_alive()),
            "strategy_engine_connected": bool(state == "running" and bridge_status.get("stream_connected")),
            "market_data_connected": bool(bridge_status.get("stream_connected")),
            "real_trading_enabled": False,
            "broker_order_executor_created": False,
            "candidate_count": candidate_count,
            "started_at": started_at,
            "last_signal_sync_at": last_sync_at,
            "last_error": last_error or bridge_status.get("error", ""),
            "market": bridge_status,
            "notice": (
                "Motor Paper habilitado; las decisiones dependen de señales y datos de mercado reales."
                if enabled and state == "running"
                else "Runtime automático desactivado. Para activarlo, configura credenciales de datos y TRADESCANNER_TRADEBOT_AUTO_PAPER=true."
            ),
        }

    def stop(self) -> None:
        self._stop.set()
        with self._lock:
            bot = self._bot
            self._state = "stopping"
        if bot is not None:
            try:
                bot.detener()
            except Exception:
                pass


runtime = TradeBotPaperRuntime()
