#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Panel Radar Bridge
Puente local mínimo para recibir órdenes del scanner desde el navegador.

Uso:
    python panel_radar_bridge.py

Endpoints:
    GET  /health
    POST /layout

El servidor escucha SOLO en 127.0.0.1 para no exponerlo a la red.
La integración con thinkorswim se añadirá después de validar primero
la comunicación scanner -> puente.
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import time

HOST = "127.0.0.1"
PORT = 8080
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "panel_radar_bridge_state.json")


def save_state(data):
    payload = {
        "updated_at": time.time(),
        "last_layout": data,
    }
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


class BridgeHandler(BaseHTTPRequestHandler):
    server_version = "PanelRadarBridge/1.0"

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Private-Network", "true")
        self.send_header("Access-Control-Max-Age", "600")

    def _json(self, status, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self._cors()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        path = self.path.split("?", 1)[0].rstrip("/") or "/"

        if path == "/health":
            self._json(200, {
                "ok": True,
                "service": "Panel Radar Bridge",
                "version": "1.0",
                "host": HOST,
                "port": PORT,
            })
            return

        if path == "/":
            self._json(200, {
                "ok": True,
                "service": "Panel Radar Bridge",
                "message": "Puente funcionando",
                "health": "/health",
                "layout": "/layout",
            })
            return

        if path == "/last-layout":
            try:
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self._json(200, data)
            except Exception:
                self._json(200, {"updated_at": None, "last_layout": None})
            return

        self._json(404, {"ok": False, "error": "Ruta no encontrada"})

    def do_POST(self):
        path = self.path.split("?", 1)[0].rstrip("/") or "/"

        if path != "/layout":
            self._json(404, {"ok": False, "error": "Ruta no encontrada"})
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b"{}"
            data = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            self._json(400, {"ok": False, "error": f"JSON inválido: {exc}"})
            return

        ticker = str(data.get("ticker", "")).strip().upper()
        layout = str(data.get("layout") or data.get("layout_color") or "").strip().upper()

        if not ticker:
            self._json(400, {"ok": False, "error": "Falta ticker"})
            return

        if layout not in {f"L{i}" for i in range(1, 11)}:
            self._json(400, {"ok": False, "error": "Layout inválido. Use L1-L10"})
            return

        command = {
            "broker": str(data.get("broker", "")),
            "ticker": ticker,
            "layout": layout,
            "received_at": time.time(),
        }
        save_state(command)

        print(f"[Panel Radar Bridge] {ticker} -> {layout}", flush=True)

        self._json(200, {
            "ok": True,
            "message": "Orden recibida",
            "ticker": ticker,
            "layout": layout,
        })

    def log_message(self, fmt, *args):
        print("[Panel Radar Bridge]", fmt % args, flush=True)


def main():
    server = ThreadingHTTPServer((HOST, PORT), BridgeHandler)
    print("")
    print("========================================")
    print("       PANEL RADAR BRIDGE")
    print("========================================")
    print(f"Puente: http://{HOST}:{PORT}/layout")
    print(f"Estado: http://{HOST}:{PORT}/health")
    print("")
    print("El puente está listo. No cierre esta ventana")
    print("mientras quiera usar la conexión con el scanner.")
    print("========================================")
    print("")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nPuente detenido.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
