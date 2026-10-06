"""Constructor de velas en memoria a partir de trades en tiempo real."""

import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass
class LiveBar: