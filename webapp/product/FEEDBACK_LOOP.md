# TradeScanner — Ciclo de mejora

## Durante el mes de prueba

Cada recomendación recibida se clasifica y conserva con trazabilidad.

### Ejemplo

Usuario:
"AMD aparece como señal pero el retroceso todavía no toca EMA50."

Clasificación:
- categoría: Scanner
- prioridad: P1/P2
- contexto: swing/pullback
- estado: reviewing

Después se comprueba:
1. qué condición produjo la señal;
2. qué timeframe estaba activo;
3. qué datos utilizó el scanner;
4. si el comportamiento es intencional;
5. si el filtro debe cambiar;
6. qué otras señales podría afectar.

## Antes de modificar el motor

La mejora debe tener:
- descripción;
- problema que resuelve;
- comportamiento actual;
- comportamiento esperado;
- riesgo de cambiarlo;
- prueba propuesta;
- criterio de aceptación.

## Resultado

Una recomendación aceptada se convierte en una tarea técnica concreta. Después se prueba antes de incorporarla al producto.

## Regla especial

Las recomendaciones relacionadas con el scanner no autorizan automáticamente cambios en `app.py`. El motor sigue protegido y cualquier modificación deberá ser solicitada y revisada explícitamente.
