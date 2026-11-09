"""Utilidades de medicion: mediana de 5 corridas descartando la primera (calienta caches)."""
import os
import platform
import statistics
import time


def medir(fn, corridas=5):
    """Ejecuta fn corridas+1 veces; descarta la primera; devuelve (mediana_s, ultimo_resultado)."""
    tiempos, res = [], None
    for i in range(corridas + 1):
        t0 = time.perf_counter()
        res = fn()
        dt = time.perf_counter() - t0
        if i > 0:
            tiempos.append(dt)
    return statistics.median(tiempos), res


def fmt(s):
    return f"{s * 1000:.2f} ms" if s < 1 else f"{s:.2f} s"


def equipo():
    return (f"{platform.system()} {platform.release()} | {platform.processor()} | "
            f"{os.cpu_count()} hilos | Python {platform.python_version()}")
