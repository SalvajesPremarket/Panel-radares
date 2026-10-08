from pathlib import Path
from fastapi import FastAPI
from fastapi.responses import FileResponse

ROOT = Path(__file__).resolve().parent
app = FastAPI(title="TradeScanner Web App")

@app.get("/", include_in_schema=False)
def landing():
    return FileResponse(ROOT / "index.html")

@app.get("/login", include_in_schema=False)
def login_page():
    return FileResponse(ROOT / "login.html")

@app.get("/register", include_in_schema=False)
def register_page():
    return FileResponse(ROOT / "register.html")

@app.get("/account", include_in_schema=False)
def account_page():
    return FileResponse(ROOT / "account" / "index.html")
