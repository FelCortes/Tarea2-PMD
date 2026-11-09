"""Generador de eventos de sensores del Gran Concepción (Lab 06).

    python3 generador.py --parquet     # respaldo: datos/gran_concepcion_stream.parquet (necesita pandas + pyarrow)
    REDIS_URL=redis://user:pass@host:6379 python3 generador.py   # en vivo (lo que corre en el VPS)

Ambos modos usan la misma función `eventos()` con la misma semilla.
"""
import json, os, random, time
from datetime import datetime, timedelta, timezone

ESTACIONES = [
    ("CC-001", "flujo_vehicular"), ("CC-002", "flujo_vehicular"),
    ("CC-003", "ocupacion_estacionamiento"), ("CC-004", "calidad_aire"),
    ("CC-005", "flujo_vehicular"), ("TAL-001", "flujo_vehicular"),
    ("TAL-002", "ocupacion_estacionamiento"), ("TAL-003", "calidad_aire"),
    ("HUA-001", "flujo_vehicular"), ("HUA-002", "flujo_vehicular"),
    ("SPP-001", "ocupacion_estacionamiento"), ("SPP-002", "flujo_vehicular"),
    ("CHI-001", "calidad_aire"), ("PEN-001", "flujo_vehicular"),
    ("COR-001", "calidad_aire"), ("COR-002", "ocupacion_estacionamiento"),
]
RANGOS = {"flujo_vehicular": (5, 120), "ocupacion_estacionamiento": (0, 100), "calidad_aire": (5, 80)}
SEMILLA = 2026
PASO = 0.5          # segundos entre eventos (~2 ev/s)
P_TARDIA = 0.03     # lectura con timestamp atrasado 30-120 s
P_REPETIDA = 0.02   # se reenvía el evento anterior tal cual


def eventos(t0, semilla=SEMILLA, paso=PASO):
    """Genera eventos infinitos; el reloj simulado avanza `paso` s por evento desde t0."""
    rng = random.Random(semilla)
    base = {e: rng.uniform(*RANGOS[t]) for e, t in ESTACIONES}
    nivel = dict(base)
    anterior, i = None, -1
    while True:
        i += 1  # cada evento (incluso repetido) ocupa un paso del reloj
        if anterior and rng.random() < P_REPETIDA:
            yield anterior
            continue
        est, tipo = rng.choice(ESTACIONES)
        lo, hi = RANGOS[tipo]
        paso_ou = 0.1 * (base[est] - nivel[est]) + rng.gauss(0, (hi - lo) * 0.05)  # vuelve hacia su nivel base
        nivel[est] = min(hi, max(lo, nivel[est] + paso_ou))
        ts = t0 + timedelta(seconds=i * paso)
        if rng.random() < P_TARDIA:
            ts -= timedelta(seconds=rng.uniform(30, 120))
        anterior = {"estacion_id": est, "tipo_sensor": tipo,
                    "valor": round(nivel[est], 1), "timestamp": ts.isoformat(timespec="milliseconds")}
        yield anterior

