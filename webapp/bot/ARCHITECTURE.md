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

## Principio
El bot debe evolucionar sin obligar a reescribir el scanner. La API será el contrato entre ambos.