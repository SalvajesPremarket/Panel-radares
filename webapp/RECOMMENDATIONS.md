# TradeScanner — Sistema de recomendaciones

## Objetivo

Durante el mes de prueba, los usuarios podrán reportar observaciones que ayuden a mejorar scanner, bot y experiencia.

## Categorías

- Scanner: señales, filtros, condiciones y resultados.
- Bot: entradas, salidas, riesgo y ejecución.
- Datos: calidad, retrasos, cobertura y APIs.
- Rendimiento: velocidad, consumo y estabilidad.
- UX: interfaz, navegación y configuración.
- Seguridad: permisos, credenciales y sesiones.

## Registro mínimo

Cada recomendación debe conservar:

- recommendation_id
- user_id
- category
- title
- description
- symbol o contexto si aplica
- created_at
- status
- priority
- admin_notes

## Estados

- new
- reviewing
- accepted
- planned
- implemented
- rejected
- duplicate

## Regla de producto

Una recomendación no cambia automáticamente el motor. Primero se analiza, se documenta el impacto y se prueba de forma aislada.

## Prioridad

**P0:** seguridad, pérdida de datos, ejecución incorrecta o riesgo financiero.

**P1:** error funcional que afecta señales o bot.

**P2:** mejora importante de precisión, rendimiento o experiencia.

**P3:** mejora cosmética o conveniencia.

## Resultado esperado

El mes de prueba debe generar una lista objetiva de mejoras priorizadas para que TradeScanner evolucione basado en uso real y no solamente en supuestos.
