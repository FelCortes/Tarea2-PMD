"""Parte B.2 - Productor: la hora en vivo (7.200 eventos) al stream, a 20x (1 evento / 25 ms)."""
import time

from comun import MAXLEN, STREAM, conectar
from datos import vivo

PASO = 0.025


def main():
    r = conectar()
    inicio = time.monotonic()
    n = 0
    try:
        for n, ev in enumerate(vivo(), start=1):
            r.xadd(STREAM, ev, maxlen=MAXLEN, approximate=True)
            # reloj sin deriva: el evento n sale en inicio + n*PASO
            time.sleep(max(0, inicio + n * PASO - time.monotonic()))
    finally:
        r.set("meta:producidos", n)  # lo usan los consumidores para saber cuando terminar
    print(f"productor: {n} eventos en {time.monotonic() - inicio:.1f} s", flush=True)


if __name__ == "__main__":
    main()
