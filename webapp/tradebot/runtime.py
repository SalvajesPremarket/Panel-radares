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
        self._short_strategy = None
        self._short_position = None
        self._last_strategy_decision = None
        self._strategy_decisions = []
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
        if config["estrategia"] not in {"LongSalvajesPreMarket", "Pullback corto ema50 ó 200 día ó semana"}:
            raise ValueError("Estrategia no disponible.")
        with self._lock:
            thread = self._thread
            running = bool(thread and thread.is_alive() and not self._stop.is_set())
            previous_strategy = self._operational_config["estrategia"]
            if running and config["estrategia"] != previous_strategy:
                raise ValueError("Apaga TradeBot antes de cambiar de estrategia.")
            self._operational_config = config
            bot = self._bot
        if config["estrategia"] == "LongSalvajesPreMarket" and bot is not None and hasattr(bot, "configurar_riesgo"):
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
            with self._lock:
                operational_config = dict(self._operational_config)
            if operational_config["estrategia"] == "Pullback corto ema50 ó 200 día ó semana":
                from webapp.tradebot.pullback_corto import PullbackCortoEMA
                self._run_pullback_corto(bridge, store, PullbackCortoEMA())
                return
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


    def _run_pullback_corto(self, bridge, store, strategy) -> None:
        """Run the short strategy against live one-minute snapshots in Paper only."""
        with self._lock:
            self._bridge = bridge
            self._bot = strategy
            self._short_strategy = strategy
            self._state = "running"
            self._started_at = time.time()
        while not self._stop.wait(0.25):
            try:
                signals = store.list(limit=100)
                by_symbol = {}
                for signal in signals:
                    symbol = str(signal.get("symbol") or signal.get("simbolo") or signal.get("ticker") or "").strip().upper()
                    if not symbol:
                        continue
                    try:
                        score = float(signal.get("confidence") or 0)
                    except (TypeError, ValueError):
                        score = 0.0
                    old = by_symbol.get(symbol)
                    try:
                        old_score = float((old or {}).get("confidence") or 0)
                    except (TypeError, ValueError):
                        old_score = 0.0
                    if old is None or score > old_score:
                        by_symbol[symbol] = signal
                ranked = sorted(by_symbol.items(), key=lambda item: float(item[1].get("confidence") or 0), reverse=True)
                with self._lock:
                    active = dict(self._short_position) if self._short_position else None
                symbol = str(active.get("symbol")) if active else (ranked[0][0] if ranked else "")
                selected = by_symbol.get(symbol) if symbol else None
                bridge.sync_results([{"ticker": symbol}] if symbol else [])
                if not symbol:
                    with self._lock:
                        self._candidate_count = 0
                        self._selected_symbol = ""
                        self._selected_confidence = None
                        self._last_error = "Esperando candidatos publicados por TradeScanner."
                    continue
                snapshot = bridge.snapshot(symbol)
                age = snapshot.get("market_data_trade_age_sec")
                if age is None or float(age) > 5.0:
                    with self._lock:
                        self._last_error = "Esperando trades recientes del feed para evaluar la estrategia corta."
                    continue
                conditions = (selected or {}).get("scanner_conditions") or {}
                decision = strategy.evaluate(snapshot, conditions)
                action = decision.get("accion")
                price = decision.get("precio")
                if action == "SHORT" and not active and price:
                    with self._lock:
                        cfg = dict(self._operational_config)
                    budget = float(cfg["capital_asignado"]) * float(cfg["porcentaje_operacion"]) / 100.0
                    quantity = int(budget / float(price))
                    if quantity > 0:
                        active = {"symbol": symbol, "entry_price": float(price), "stop_loss": decision.get("stop_loss"), "quantity": quantity, "strategy": "Pullback corto ema50 ó 200 día ó semana"}
                        with self._lock:
                            self._short_position = active
                elif action == "HOLD" and active:
                    new_stop = decision.get("stop_loss")
                    if new_stop is not None:
                        active["stop_loss"] = min(float(active["stop_loss"]), float(new_stop)) if active.get("stop_loss") is not None else float(new_stop)
                        with self._lock:
                            self._short_position = active
                elif action == "EXIT" and active:
                    pnl = (float(active["entry_price"]) - float(price)) * int(active["quantity"]) if price else 0.0
                    with self._lock:
                        self._strategy_decisions.append({"symbol": symbol, "action": "EXIT", "reason": decision.get("motivo", "Salida de estrategia"), "price": price, "entry_price": active["entry_price"], "quantity": active["quantity"], "pnl_realizado": pnl, "mode": "paper", "created_at": time.time()})
                        self._strategy_decisions = self._strategy_decisions[-100:]
                        self._short_position = None
                    active = None
                with self._lock:
                    self._last_strategy_decision = decision
                    self._strategy_decisions.append({**decision, "created_at": time.time(), "mode": "paper"})
                    self._strategy_decisions = self._strategy_decisions[-100:]
                    self._candidate_count = len(by_symbol)
                    self._selected_symbol = symbol
                    try:
                        self._selected_confidence = float((selected or {}).get("confidence")) if (selected or {}).get("confidence") is not None else None
                    except (TypeError, ValueError):
                        self._selected_confidence = None
                    self._last_sync_at = time.time()
            except Exception as exc:
                with self._lock:
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
            short_position = dict(self._short_position) if self._short_position else None
            last_strategy_decision = dict(self._last_strategy_decision) if self._last_strategy_decision else None
            strategy_decisions = list(self._strategy_decisions)
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
            "short_position_paper": short_position,
            "last_strategy_decision": last_strategy_decision,
            "strategy_decisions": strategy_decisions,
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
            strategy = self._short_strategy
            position = dict(self._short_position) if self._short_position else None
            self._state = "stopping"
        if position and strategy is not None:
            price = getattr(strategy, "last_price", None)
            if price is not None:
                pnl = (float(position["entry_price"]) - float(price)) * int(position["quantity"])
                with self._lock:
                    self._strategy_decisions.append({"symbol": position["symbol"], "action": "EXIT", "reason": "TradeBot apagado; cierre Paper al último precio observado.", "price": float(price), "entry_price": position["entry_price"], "quantity": position["quantity"], "pnl_realizado": pnl, "mode": "paper", "created_at": time.time()})
                    self._strategy_decisions = self._strategy_decisions[-100:]
                    self._short_position = None
        if bot is not None and hasattr(bot, "detener"):
            try:
                bot.detener()
            except Exception:
                pass


runtime = TradeBotPaperRuntime()
