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
        self._selected_symbol = ""
        self._selected_confidence: float | None = None
        self._manual_requested = False
        self._operational_config = {
            "capital_asignado": 600.0,
            "porcentaje_operacion": 20.0,
            "stop_loss_pct": 2.0,
            "take_profit_pct": 4.0,
            "estrategia": "LongSalvajesPreMarket",
        }

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

    def configure(self, values: dict) -> dict:
        config = {
            "capital_asignado": float(values.get("capital_asignado", 600.0)),
            "porcentaje_operacion": float(values.get("porcentaje_operacion", 20.0)),
            "stop_loss_pct": float(values.get("stop_loss_pct", 2.0)),
            "take_profit_pct": float(values.get("take_profit_pct", 4.0)),
            "estrategia": str(values.get("estrategia", "LongSalvajesPreMarket")),
        }
        if config["capital_asignado"] <= 0 or config["capital_asignado"] > 1000000:
            raise ValueError("El capital debe ser mayor que cero y no superar $1,000,000.")
        if not 1 <= config["porcentaje_operacion"] <= 100:
            raise ValueError("El porcentaje por operación debe estar entre 1 y 100.")
        if not 0.1 <= config["stop_loss_pct"] <= 50:
            raise ValueError("El Stop Loss debe estar entre 0.1% y 50%.")
        if not 0.1 <= config["take_profit_pct"] <= 100:
            raise ValueError("El Take Profit debe estar entre 0.1% y 100%.")
        if config["estrategia"] != "LongSalvajesPreMarket":
            raise ValueError("Estrategia no disponible.")
        with self._lock:
            self._operational_config = config
            bot = self._bot
        if bot is not None:
            bot.configurar_riesgo(**config)
        return dict(config)

    def start_if_configured(self) -> None:
        config = self.configuration()
        if not config["enabled"]:
            with self._lock:
                if not self._manual_requested:
                    self._state = "disabled"
            return
        self._start(force=False)

    def start_manual(self, values: dict) -> dict:
        config = self.configure(values)
        with self._lock:
            self._manual_requested = True
        self._start(force=True)
        return self.status()

    def _start(self, force: bool) -> None:
        config = self.configuration()
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            if not force and not config["enabled"]:
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
            with self._lock:
                operational_config = dict(self._operational_config)
            bot.configurar_riesgo(**operational_config)
            bot.iniciar()
            with self._lock:
                self._bridge = bridge
                self._bot = bot
                self._state = "running"
                self._started_at = time.time()

            while not self._stop.wait(5.0):
                signals = store.list(limit=100)
                # Select the highest-confidence scanner symbol for new entries.
                # Existing LONG states remain managed by BotLongRealtime for exits.
                candidates_by_symbol: dict[str, dict] = {}
                for signal in signals:
                    symbol = str(signal.get("symbol") or signal.get("simbolo") or signal.get("ticker") or "").strip().upper()
                    if not symbol:
                        continue
                    try:
                        confidence = float(signal.get("confidence") or 0)
                    except (TypeError, ValueError):
                        confidence = 0.0
                    current = candidates_by_symbol.get(symbol)
                    timestamp = str(signal.get("timestamp") or signal.get("actualizado") or "")
                    current_timestamp = str((current or {}).get("timestamp") or (current or {}).get("actualizado") or "")
                    if current is None:
                        candidates_by_symbol[symbol] = signal
                    else:
                        try:
                            current_confidence = float(current.get("confidence") or 0)
                        except (TypeError, ValueError):
                            current_confidence = 0.0
                        if (confidence, timestamp) > (current_confidence, current_timestamp):
                            candidates_by_symbol[symbol] = signal
                def ranking(item):
                    signal = item[1]
                    try:
                        confidence = float(signal.get("confidence") or 0)
                    except (TypeError, ValueError):
                        confidence = 0.0
                    return confidence, str(signal.get("timestamp") or signal.get("actualizado") or "")
                ranked = sorted(candidates_by_symbol.items(), key=ranking, reverse=True)
                best = ranked[0] if ranked else None
                bot.sync_signals([best[1]] if best else [])
                with self._lock:
                    self._candidate_count = len(candidates_by_symbol)
                    self._selected_symbol = best[0] if best else ""
                    try:
                        self._selected_confidence = float(best[1].get("confidence")) if best and best[1].get("confidence") is not None else None
                    except (TypeError, ValueError):
                        self._selected_confidence = None
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
            selected_symbol = self._selected_symbol
            selected_confidence = self._selected_confidence
            manual_requested = self._manual_requested
            operational_config = dict(self._operational_config)
        bridge_status = {}
        if bridge is not None:
            try:
                bridge_status = bridge.status()
            except Exception as exc:
                bridge_status = {"error": f"{type(exc).__name__}: {exc}"[:300]}
        enabled = bool((config["enabled"] or manual_requested) and config["credentials_configured"])
        return {
            "enabled": bool(config["enabled"] or manual_requested),
            "manual_requested": manual_requested,
            "config_operativa": operational_config,
            "credentials_configured": config["credentials_configured"],
            "feed": config["feed"],
            "state": state,
            "thread_alive": bool(thread and thread.is_alive()),
            "strategy_engine_connected": bool(state == "running" and bridge_status.get("stream_connected")),
            "market_data_connected": bool(bridge_status.get("stream_connected")),
            "real_trading_enabled": False,
            "broker_order_executor_created": False,
            "candidate_count": candidate_count,
            "selected_symbol": selected_symbol,
            "selected_confidence": selected_confidence,
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

    def stop_manual(self) -> dict:
        with self._lock:
            self._manual_requested = False
        self.stop()
        with self._lock:
            if self._state != "error":
                self._state = "stopped"
        return self.status()

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
