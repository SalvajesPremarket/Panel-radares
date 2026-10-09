"""Prueba manual local de OAuth y Market Data de Schwab.

Ejecutar en una computadora local, nunca como parte del despliegue público.
No coloca credenciales ni tokens en la URL o en GitHub.
"""
from __future__ import annotations

import getpass
from urllib.parse import parse_qs, urlparse

from TradeScanner.schwab_client import SchwabAPIError, SchwabMarketDataClient


def _authorization_code(value: str) -> str:
    value = value.strip()
    if "://" in value:
        params = parse_qs(urlparse(value).query)
        return (params.get("code") or [""])[0]
    if value.startswith("code="):
        return value[5:].split("&", 1)[0]
    return value


def main() -> int:
    client = SchwabMarketDataClient()
    try:
        print("\n1) Abre esta URL e inicia sesión en Schwab:\n")
        print(client.authorization_url())
        print(
            "\nDespués de autorizar, Schwab redirigirá al callback local. "
            "Si el navegador indica que no puede conectar, copia la URL completa "
            "de la barra de direcciones (incluye el parámetro code) y pégala aquí. "
            "No la compartas con nadie."
        )
        redirected = getpass.getpass("\nPega la URL de redirección o el código OAuth: ")
        code = _authorization_code(redirected)
        if not code:
            raise SchwabAPIError("No se encontró el parámetro code en la entrada.")
        client.exchange_code(code)
        symbol = input("Símbolo para probar cotización (por ejemplo AAPL): ").strip().upper()
        quote = client.get_quote(symbol)
        print(f"\nCotización recibida correctamente para {symbol}.")
        print(f"Respuesta JSON (datos de mercado): {quote}")
        print("\nPrueba completada. No se enviaron órdenes.")
        return 0
    except (SchwabAPIError, ValueError) as exc:
        print(f"\nNo se pudo completar la prueba: {exc}")
        return 1
    except KeyboardInterrupt:
        print("\nPrueba cancelada.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
