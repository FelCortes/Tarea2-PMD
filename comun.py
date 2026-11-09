"""Constantes y utilidades compartidas por el productor y los consumidores."""
import os
from datetime import datetime

import redis

STREAM = "sensores:t2"
# MAXLEN ~ 20.000 entradas = ~8 min de atraso tolerado a 40 ev/s (20x). La hora en vivo
# son 7.200 eventos, asi que en esta tarea nunca se recorta nada sin leer. En un sistema
# real, una entrada recortada que ningun grupo leyo se pierde para siempre.
MAXLEN = 20_000
GRUPO_INGESTA = "ingesta"
GRUPO_TABLERO = "tablero"
VENTANA_MS = 10 * 60 * 1000


def conectar(port=None):
    port = port or int(os.environ.get("REDIS_PORT", 6379))
    return redis.Redis(host="localhost", port=port, decode_responses=True)


def ts_ms(iso):
    """Timestamp de MEDICION (ISO con offset) -> milisegundos epoch."""
    return int(round(datetime.fromisoformat(iso).timestamp() * 1000))


def crear_grupo(r, grupo):
    try:
        r.xgroup_create(STREAM, grupo, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
