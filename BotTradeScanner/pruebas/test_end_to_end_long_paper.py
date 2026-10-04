"""Prueba extremo-a-extremo: estrategia LONG + maquina de decisiones + PaperBot."""

from BotTradeScanner.decision.maquina_decisiones import MaquinaDecisionesLong
from BotTradeScanner.riesgo.paper import PaperBot


def snap(
    tramo=1,
    apertura=10.0,
    maximo=10.0,
    minimo=10.0,
    cierre=10.0,
    positiva=False,
    libelula=False,
    lapida=False,
    regreso=False,
    anterior_min=9.0,
    anterior_max=10.5,
    anterior_cierre=10.2,
):
    return {
        "simbolo": "TEST",
        "sin_datos": False,
        "tramo_actual": tramo,
        "vela_actual": {
            "apertura": apertura,
            "maximo": maximo,
            "minimo": minimo,
            "cierre": cierre,
            "es_positiva": positiva,
            "es_libelula_en_curso": libelula,
            "es_lapida_en_curso": lapida,
            "regreso_a_apertura": regreso,
        },
        "vela_anterior": {
            "minimo": anterior_min,
            "maximo": anterior_max,
            "cierre": anterior_cierre,
        },
        "minimo_supera_anterior": minimo > anterior_min,
        "maximo_supera_anterior": maximo > anterior_max,
        "ema9": 10.1,
        "ema20": 9.8,
        "ema50": 7.5,
        "ema200": 6.5,
        "ema9_anterior": 10.1,
        "ema20_anterior": 9.8,
        "ema50_anterior": 7.5,
        "ema200_anterior": 6.5,
        "macd": 0.2,
        "macd_anterior": 0.2,
        "banda_bollinger_superior": 12.0,
        "banda_bollinger_superior_anterior": 12.0,
        "banda_bollinger_inferior": 8.0,
        "banda_bollinger_inferior_anterior": 8.0,
    }


def enviar(maquina, paper, snapshot, candidato_scanner=True):
    decision = maquina.evaluar(snapshot, candidato_scanner=candidato_scanner)
    registro = paper.evaluar({
        "signal_id": f"{snapshot['tramo_actual']}-{snapshot['vela_actual']['cierre']}-{decision.accion}",
        "simbolo": decision.simbolo,
        "accion": decision.accion,
        "precio": decision.precio,
        "stop_loss": decision.stop_loss,
        "motivo": decision.motivo,
        "estrategia": decision.estrategia,
    })
    return decision, registro


def test_end_to_end_long_paper_candidato_buy_stop_y_exit():
    maquina = MaquinaDecisionesLong()
    paper = PaperBot()

    # 1) Primer snapshot: TradeScanner entrega candidato.
    d, _ = enviar(
        maquina, paper,
        snap(tramo=1, apertura=10.0, maximo=10.0, minimo=9.7,
             cierre=10.0, libelula=False),
    )
    assert d.accion == "WATCH"
    assert paper.posiciones() == []

    # 2) La libelula se confirma en el mismo tramo -> BUY.
    d, p = enviar(
        maquina, paper,
        snap(tramo=1, apertura=10.0, maximo=10.0, minimo=9.7,
             cierre=10.0, libelula=True),
    )
    assert d.accion == "BUY"
    assert p["action"] == "buy"
    assert len(paper.posiciones()) == 1
    assert paper.posiciones()[0]["stop_loss"] == 10.0

    # 3) Primera vela sigue positiva.
    d, _ = enviar(
        maquina, paper,
        snap(tramo=2, apertura=10.0, maximo=10.5, minimo=9.8,
             cierre=10.3, positiva=True),
    )
    assert d.accion == "HOLD"
    assert paper.posiciones()[0]["stop_loss"] == 10.0

    # 4) Primera vela termina positiva: queda lista la segunda.
    d, _ = enviar(
        maquina, paper,
        snap(tramo=3, apertura=10.0, maximo=10.7, minimo=9.9,
             cierre=10.6, positiva=True),
    )
    assert d.accion == "HOLD"

    # 5) Segunda vela valida y fija el stop al cierre de la entrada.
    d, _ = enviar(
        maquina, paper,
        snap(tramo=1, apertura=10.6, maximo=10.9, minimo=10.0,
             cierre=10.8, positiva=True,
             anterior_min=9.9, anterior_max=10.7, anterior_cierre=10.0),
    )
    assert d.accion == "HOLD"
    assert d.stop_loss == 10.0
    assert paper.posiciones()[0]["stop_loss"] == 10.0

    # 6) Segunda vela completa: pasa a LONG_SIGUIENTES.
    d, _ = enviar(
        maquina, paper,
        snap(tramo=3, apertura=10.6, maximo=11.0, minimo=10.1,
             cierre=10.9, positiva=True,
             anterior_min=9.9, anterior_max=10.7, anterior_cierre=10.0),
    )
    assert d.accion == "HOLD"

    # 7) Tercera vela positiva + higher low: stop sube al cierre anterior.
    d, _ = enviar(
        maquina, paper,
        snap(tramo=2, apertura=10.9, maximo=11.4, minimo=10.2,
             cierre=11.3, positiva=True,
             anterior_min=10.1, anterior_max=11.0, anterior_cierre=10.9),
    )
    assert d.accion == "HOLD"
    assert d.stop_loss == 10.9
    assert paper.posiciones()[0]["stop_loss"] == 10.9

    # 8) Bollinger superior sin lapida: se mantiene abierto.
    d, _ = enviar(
        maquina, paper,
        snap(tramo=3, apertura=11.3, maximo=12.1, minimo=11.2,
             cierre=11.9, positiva=True,
             anterior_min=10.2, anterior_max=11.4, anterior_cierre=11.3),
    )
    assert d.accion == "HOLD"
    assert len(paper.posiciones()) == 1

    # 9) Bollinger + lapida: estrategia ordena EXIT y Paper cierra.
    d, p = enviar(
        maquina, paper,
        snap(tramo=3, apertura=11.9, maximo=12.2, minimo=11.8,
             cierre=11.8, positiva=False, lapida=True,
             anterior_min=11.2, anterior_max=12.1, anterior_cierre=11.9),
    )
    assert d.accion == "EXIT"
    assert p["action"] == "sell"
    assert paper.posiciones() == []
