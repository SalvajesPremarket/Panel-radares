"""Unified TradeScanner Web App entrypoint.

This composes the commercial web layer only. The existing Streamlit
scanner (TradeScanner/app.py) remains isolated and is not imported here.
"""
from fastapi import FastAPI

from webapp.auth.server import app as auth_app, startup as auth_startup
from webapp.account.server import app as account_app, init_account_schema
from webapp.api.server import app as api_app
from webapp.routes import app as page_app

app = FastAPI(title="TradeScanner Web App", version="0.1.0")

@app.on_event("startup")
def startup():
    # Create account/session tables before serving requests.
    auth_startup()
    init_account_schema()

# Keep API/service routes under the same origin so browser cookies work
# without exposing internal service boundaries.
app.router.routes.extend(auth_app.router.routes)
app.router.routes.extend(account_app.router.routes)
app.router.routes.extend(api_app.router.routes)
app.router.routes.extend(page_app.router.routes)

@app.get("/health")
def health():
    return {"ok": True, "service": "tradescanner-webapp"}
