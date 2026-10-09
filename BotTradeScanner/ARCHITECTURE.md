# TradeScanner Bot — Arquitectura

## Flujo
**Scanner → API privada → Bot → Motor de riesgo → Broker**

El bot no debe depender directamente de variables internas de Streamlit.

## Modos
### Señales
Recibe oportunidades y no ejecuta órdenes.

### Confirmación
Prepara la operación y solicita confirmación.

### Automático
Puede ejecutar únicamente con autorización explícita, broker conectado, riesgo válido, señal aprobada y kill switch desactivado.

## Señal normalizada
- symbol
- timeframe
- signal_type
- price
- timestamp
- source
- confidence
- scanner_conditions
- risk_context

## Riesgo
- capital máximo por operación;
- riesgo máximo por operación;
- máximo de posiciones simultáneas;
- pérdida diaria máxima;
- exposición máxima;
- stop loss;
- take profit;
- horario permitido;
- kill switch.

## Paper trading
Será obligatorio antes de permitir dinero real.

Debe conservar historial de señal, decisión, orden, ejecución, resultado y motivo de rechazo.

## Contrato operativo con TradeScanner
- TradeScanner es el único componente que recorre el universo, obtiene snapshots del mercado y selecciona/publica candidatos.
- BotTradeScanner consume esos candidatos y conserva su contexto (precio y timestamp del scanner, EMA, MACD, GAP, volumen y flotación) para auditoría y evaluación.
- El bot no debe iniciar un segundo escaneo de universo ni crear una conexión websocket independiente para esos candidatos.
- Para la evaluación intradía, el bot utiliza el motor de velas compartido que TradeScanner ya mantiene; las suscripciones se limitan a candidatos publicados y a símbolos con estado LONG activo.
- El contexto del scanner acompaña la decisión, pero no sustituye el snapshot vivo ni altera por sí solo las reglas de entrada existentes.
- Si falla el feed compartido, el bot debe registrar el motivo y no inventar precios ni tratar datos obsoletos como actuales.

## Principio
El bot debe evolucionar sin obligar a reescribir el scanner. La API será el contrato entre ambos.