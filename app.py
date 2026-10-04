"""Punto de entrada compatible del TradeScanner.

La aplicación real vive en TradeScanner/app.py.
Este archivo se conserva para que el despliegue actual que apunta a app.py
siga funcionando sin mover la configuración de Streamlit Cloud.
"""

from TradeScanner.app import *  # noqa: F401,F403
