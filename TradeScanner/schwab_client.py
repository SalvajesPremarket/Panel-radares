"""Cliente REST de Schwab para datos de mercado.

Este módulo es independiente de Alpaca y no modifica el scanner existente.
No envía órdenes. Los tokens se guardan fuera del repositorio por defecto.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests


AUTH_BASE = "https://api.schwabapi.com/v1/oauth"
MARKET_DATA_BASE = "https://api.schwabapi.com/marketdata/v1"


class SchwabAPIError(RuntimeError):
    """Error legible al llamar a la API de Schwab."""


class SchwabMarketDataClient:
    def __init__(
        self,
        app_key: str | None = None,
        app_secret: str | None = None,
        callback_url: str | None = None,
        token_path: str | Path | None = None,
        session: Any = None,
    ):
        self.app_key = (app_key or os.getenv("SCHWAB_APP_KEY") or "").strip()
        self.app_secret = (app_secret or os.getenv("SCHWAB_APP_SECRET") or "").strip()
        self.callback_url = (callback_url or os.getenv("SCHWAB_CALLBACK_URL") or "https://127.0.0.1:8189/").strip()
        default_token_path = Path.home() / ".tradescanner" / "schwab_tokens.json"
        self.token_path = Path(token_path or os.getenv("SCHWAB_TOKEN_PATH") or default_token_path).expanduser()
        self.session = session or requests.Session()
        self._tokens: dict[str, Any] = {}
        self._load_tokens()

    def _require_app_credentials(self) -> None:
        if not self.app_key or not self.app_secret:
            raise SchwabAPIError(
                "Configura SCHWAB_APP_KEY y SCHWAB_APP_SECRET en el entorno local; no los publiques en GitHub."
            )

    def _load_tokens(self) -> None:
        try:
            data = json.loads(self.token_path.read_text(encoding="utf-8"))
            self._tokens = data if isinstance(data, dict) else {}
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            self._tokens = {}

    def _save_tokens(self) -> None:
        self.token_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.token_path.with_suffix(self.token_path.suffix + ".tmp")
        temp_path.write_text(json.dumps(self._tokens), encoding="utf-8")
        try:
            os.chmod(temp_path, 0o600)
        except OSError:
            pass
        temp_path.replace(self.token_path)
        try:
            os.chmod(self.token_path, 0o600)
        except OSError:
            pass

    def authorization_url(self) -> str:
        """URL que el usuario abre para autorizar la aplicación en Schwab."""
        self._require_app_credentials()
        query = urlencode({
            "client_id": self.app_key,
            "redirect_uri": self.callback_url,
            "response_type": "code",
        })
        return f"{AUTH_BASE}/authorize?{query}"

    def exchange_code(self, authorization_code: str) -> dict[str, Any]:
        """Intercambia el código OAuth recibido en el callback por tokens."""
        self._require_app_credentials()
        code = str(authorization_code or "").strip()
        if not code:
            raise SchwabAPIError("Falta el authorization code de Schwab.")
        response = self.session.post(
            f"{AUTH_BASE}/token",
            auth=(self.app_key, self.app_secret),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": self.callback_url,
            },
            timeout=20,
        )
        self._raise_for_response(response, "intercambiar el código OAuth")
        self._tokens = response.json()
        self._tokens["obtained_at"] = time.time()
        self._save_tokens()
        return dict(self._tokens)

    def _refresh_access_token(self) -> None:
        self._require_app_credentials()
        refresh_token = self._tokens.get("refresh_token")
        if not refresh_token:
            raise SchwabAPIError("No hay refresh token. Autoriza la aplicación con authorization_url().")
        response = self.session.post(
            f"{AUTH_BASE}/token",
            auth=(self.app_key, self.app_secret),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={"grant_type": "refresh_token", "refresh_token": refresh_token},
            timeout=20,
        )
        self._raise_for_response(response, "renovar el token OAuth")
        refreshed = response.json()
        refreshed["obtained_at"] = time.time()
        # Algunos servidores no devuelven refresh_token en cada renovación.
        if not refreshed.get("refresh_token"):
            refreshed["refresh_token"] = refresh_token
        self._tokens.update(refreshed)
        self._save_tokens()

    def _access_token(self) -> str:
        token = self._tokens.get("access_token")
        obtained_at = float(self._tokens.get("obtained_at") or 0)
        expires_in = float(self._tokens.get("expires_in") or 0)
        if not token or time.time() >= obtained_at + max(0, expires_in - 60):
            self._refresh_access_token()
        token = self._tokens.get("access_token")
        if not token:
            raise SchwabAPIError("Schwab no devolvió un access token válido.")
        return str(token)

    @staticmethod
    def _raise_for_response(response: Any, action: str) -> None:
        if 200 <= int(response.status_code) < 300:
            return
        # No incluimos secretos ni cuerpos completos que pudieran contener datos sensibles.
        detail = ""
        try:
            payload = response.json()
            detail = str(payload.get("message") or payload.get("error_description") or payload.get("error") or "")
        except Exception:
            detail = ""
        suffix = f": {detail}" if detail else ""
        raise SchwabAPIError(f"Error al {action} (HTTP {response.status_code}){suffix}")

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        response = self.session.get(
            f"{MARKET_DATA_BASE}{path}",
            headers={"Authorization": f"Bearer {self._access_token()}"},
            params=params,
            timeout=20,
        )
        self._raise_for_response(response, "consultar datos de mercado")
        return response.json()

    def get_quote(self, symbol: str) -> dict[str, Any]:
        ticker = str(symbol or "").strip().upper()
        if not ticker:
            raise ValueError("El símbolo bursátil no puede estar vacío.")
        return self._get(f"/quotes/{ticker}")

    def get_quotes(self, symbols: list[str]) -> dict[str, Any]:
        tickers = list(dict.fromkeys(str(s or "").strip().upper() for s in symbols if str(s or "").strip()))
        if not tickers:
            raise ValueError("Debes indicar al menos un símbolo bursátil.")
        return self._get("/quotes", params={"symbols": ",".join(tickers)})

    def get_price_history(
        self,
        symbol: str,
        *,
        period_type: str = "day",
        period: int = 10,
        frequency_type: str = "minute",
        frequency: int = 1,
        need_extended_hours_data: bool = True,
    ) -> dict[str, Any]:
        ticker = str(symbol or "").strip().upper()
        if not ticker:
            raise ValueError("El símbolo bursátil no puede estar vacío.")
        return self._get(
            "/pricehistory",
            params={
                "symbol": ticker,
                "periodType": period_type,
                "period": period,
                "frequencyType": frequency_type,
                "frequency": frequency,
                "needExtendedHoursData": str(bool(need_extended_hours_data)).lower(),
            },
        )
