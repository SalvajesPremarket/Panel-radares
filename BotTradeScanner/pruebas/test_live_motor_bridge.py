from threading import Thread
import time

from BotTradeScanner.integracion.live_motor_bridge import MotorVelasBridge


class FakeMotor:
    def __init__(self):
        self._stream = None
        self.precargados = []
        self.suscritos = []

    def precargar_historial(self, simbolos, cantidad=300):
        self.precargados.append((list(simbolos), cantidad))

    def agregar_simbolo_en_caliente(self, simbolo):
        assert self._stream is not None
        self.suscritos.append(simbolo)


def test_bridge_espera_stream_antes_de_marcar_candidato_como_cargado():
    motor = FakeMotor()
    bridge = MotorVelasBridge.__new__(MotorVelasBridge)
    bridge.motor = motor
    bridge._lock = __import__("threading").Lock()
    bridge._subscribe_lock = __import__("threading").Lock()
    bridge._simbolos_solicitados = {"AAPL"}
    bridge._simbolos_cargados = set()
    bridge._ultima_error = None

    def levantar_stream():
        time.sleep(0.05)
        motor._stream = object()

    Thread(target=levantar_stream, daemon=True).start()

    bridge._preparar_y_suscribir(["AAPL"])

    assert motor.precargados == [(["AAPL"], 300)]
    assert motor.suscritos == ["AAPL"]
    assert bridge._simbolos_cargados == {"AAPL"}
    assert bridge._ultima_error is None
