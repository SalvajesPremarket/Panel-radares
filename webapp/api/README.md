# API privada de TradeScanner

La API comercial recibe señales normalizadas en `webapp/api/server.py` y las almacena mediante `webapp/api/signal_service.py`.

El scanner productivo publica sus resultados finales desde `app.py` mediante `webapp/api/publisher.py`. El envío usa `TRADESCANNER_SIGNAL_INGEST_URL` y `TRADESCANNER_SIGNAL_INGEST_SECRET`.

No se importa `app.py` desde la API ni se crea un segundo scanner. La integración de señales se mantiene en un solo sentido: el scanner existente publica candidatos finales y el servicio web los recibe.
