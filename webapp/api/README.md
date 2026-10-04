# API privada y adaptador del scanner

La API comercial consume señales normalizadas. scanner_adapter.py define la frontera con el motor real.

El adaptador recibe una instancia del servicio scanner ya existente y lee servicio.resultados. No importa app.py, no crea otro scanner y no cambia ningún filtro.

La integración final debe invocar publish_service_results(servicio, timeframe) desde el ciclo apropiado del motor. Esa llamada se hará solamente cuando se autorice explícitamente una modificación de app.py.

Mientras tanto, la API y el bot pueden probarse con publish_signal() sin tocar el motor productivo.
