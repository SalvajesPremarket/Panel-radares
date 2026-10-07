from threading import Thread
import time

from BotTradeScanner.integracion.live_motor_bridge import MotorVelasBridge


class FakeMotor:
    def __init__(self):
        self._stream = None
        self.precargados = []
        self.suscritos = []
        self.desuscritos = []

    def precargar_historial(self, simbolos, cantidad=300):
        self.precargados.append((list(simbolos), cantidad))

    def agregar_simbolo_en_caliente(self, simbolo):
        assert self._stream is not None
        self.suscritos.append(simbolo)

    def quitar_simbolo_en_caliente(self, simbolo):
        self.desuscritos.append(simbolo)


def test_bridge_espera_stream_antes_de_marcar_candidato_como_cargado():
    motor = FakeMotor()
    bridge = MotorVelasBridge.__new__(MotorVelasBridge)
    bridge.motor = motor
    bridge._lock = __import__("threading").Lock()
    bridge._subscribe_lock = __import__("threading").Lock()
    bridge._simbolos_solicitados = {"AAPL"}
    bridge._simbolos_cargados = set()
    bridge._simbolos_deseados = {"AAPL"}
    bridge._ultima_error = None

    def levantar_stream():
        time.sleep(0.05)
        motor._stream = object()

    Thread(target=levantar_stream, daemon=True).start()

    bridge._actualizar_suscripciones([], ["AAPL"])

    assert motor.precargados == [(["AAPL"], 300)]
    assert motor.suscritos == ["AAPL"]
    assert bridge._simbolos_cargados == {"AAPL"}
    assert bridge._ultima_error is None


def test_bridge_rota_simbolos_fuera_del_conjunto_actual():
    motor = FakeMotor()
    motor._stream = object()
    bridge = MotorVelasBridge.__new__(MotorVelasBridge)
    bridge.motor = motor
    bridge._lock = __import__("threading").Lock()
    bridge._subscribe_lock = __import__("threading").Lock()
    bridge._simbolos_solicitados = set()
    bridge._simbolos_cargados = {"AAPL", "MSFT"}
    bridge._simbolos_deseados = {"AAPL", "MSFT"}
    bridge._ultima_error = None

    bridge._simbolos_deseados = {"NVDA"}
    bridge._simbolos_solicitados = {"NVDA"}
    bridge._actualizar_suscripciones(["AAPL", "MSFT"], ["NVDA"])

    assert motor.desuscritos == ["AAPL", "MSFT"]
    assert motor.suscritos == ["NVDA"]
    assert bridge._simbolos_cargados == {"NVDA"}


def test_bridge_compartido_no_arranca_stream_propio_ni_suscribe_alpaca():
    class FakeSharedStream:
        pass

    class FakeMotorShared:
        def __init__(self):
            self._stream = None
            self._stream_compartido = None
            self._iniciado = False
            self.precargados = []

        def conectar_stream_compartido(self, stream):
            self._stream_compartido = stream
            self._iniciado = True

        def precargar_historial(self, simbolos, cantidad=300):
            self.precargados = list(simbolos)

        def agregar_simbolo_en_caliente(self, simbolo):
            raise AssertionError("El modo compartido no debe abrir suscripciones propias")

    stream = FakeSharedStream()
    motor = FakeMotorShared()
    bridge = MotorVelasBridge("key", "secret", motor=motor, market_stream=stream)

    bridge.sync_results([{"ticker": "AAPL"}])

    assert motor._stream_compartido is stream
    assert motor._stream is None
    assert motor._iniciado is True
    deadline = time.time() + 1.0
    while time.time() < deadline and not bridge.status()["simbolos_cargados"]:
        time.sleep(0.01)

    assert bridge.status()["stream_compartido"] is True
    assert bridge.status()["simbolos_cargados"] == ["AAPL"]
    assert motor.precargados == ["AAPL"]
