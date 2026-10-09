# Schwab local bridge — primer smoke test

Este cliente está aislado del scanner actual. No sustituye Alpaca todavía y no envía órdenes.

## 1. Configurar variables localmente

En PowerShell, para la sesión actual:

```powershell
$env:SCHWAB_APP_KEY = "TU_APP_KEY"
$env:SCHWAB_APP_SECRET = "TU_APP_SECRET"
$env:SCHWAB_CALLBACK_URL = "https://127.0.0.1:8189/"
```

Usa el App Key y App Secret de Schwab. No los pegues en el chat, no los pongas en archivos versionados y no los subas a GitHub.

## 2. Ejecutar la prueba

Desde la raíz del repositorio, con las dependencias instaladas:

```powershell
python -m TradeScanner.schwab_smoke_test
```

El programa imprime la URL de autorización. Inicia sesión en Schwab y autoriza la aplicación. Después, el portal intentará volver a `https://127.0.0.1:8189/`. Si no hay un receptor local escuchando, puede aparecer un error de conexión; copia la URL de redirección de la barra del navegador y pégala en la consola local. Esa URL contiene un código OAuth temporal: no la publiques ni la compartas.

El programa intercambia el código, guarda los tokens fuera del repositorio por defecto en `~/.tradescanner/schwab_tokens.json` y consulta una cotización. No se deben guardar tokens en Streamlit Cloud ni en Render.

## 3. Límites de esta etapa

- Se prueba solo OAuth y Market Data.
- La llamada real requiere que Schwab haya habilitado Market Data Production para esta aplicación.
- La integración con el scanner y el flujo streaming aún no está conectada.
- No se envían órdenes y no se activa ejecución real.
- Si Schwab rechaza el callback o el intercambio del código, detenerse y revisar el mensaje; no modificar la aplicación aprobada sin indicación de soporte.
