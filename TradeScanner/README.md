# TradeScanner

Este directorio contiene el núcleo del scanner: motor de datos, construcción de velas, cachés y stream de mercado.

## Separación oficial de proyectos

- /app.py → único entry point de Streamlit del producto web. Es el archivo que debe usar el despliegue.
- /TradeScanner/ → código propio del scanner. No contiene la estrategia ni la lógica de ejecución del robot.
- /BotTradeScanner/ → robot de trading separado: estrategia LONG/SHORT, decisiones, riesgo, ejecución/paper y puente de integración.
- /webapp/ → servicios web auxiliares (autenticación, API, billing, recomendaciones y soporte). Se mantiene fuera del motor del scanner para no mezclar responsabilidades.
- Stage 3 → arquitectura de conexión/compartición entre estos componentes; no es otro scanner ni otro entry point.

## Regla de arquitectura

El scanner puede integrarse con BotTradeScanner mediante interfaces explícitas como MotorVelasBridge, pero la lógica del robot debe permanecer dentro de BotTradeScanner.

No se deben crear copias del scanner ni segundos app.py que puedan confundirse con el entry point oficial.

## Estado actual

La duplicación histórica de TradeScanner/app.py fue eliminada. El app.py de la raíz es actualmente el único entry point de Streamlit.

El objetivo es conservar esta separación y hacer cambios quirúrgicos, sin reescribir el motor estable del scanner.
