# Trading Bot

El bot actual está limitado deliberadamente a señal/paper.

## Flujo

Scanner → señal normalizada → API privada → validación de riesgo → paper decision

No hay llamadas a brokers reales en esta etapa.

## Reglas iniciales

- capital máximo por operación
- riesgo máximo por operación
- posiciones simultáneas máximas
- exposición máxima
- stop loss / take profit configurables
- kill switch

Antes de cualquier ejecución real deberán existir paper trading validado, auditoría, autorización explícita, idempotencia de órdenes y controles de emergencia.
