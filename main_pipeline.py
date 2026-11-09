"""Corre el camino en vivo completo: limpia Redis, levanta los dos consumidores y el productor."""
import subprocess
import sys
import time

from comun import conectar


def main():
    r = conectar()
    r.flushall()  # Redis dedicado a la tarea: se parte de cero
    py = sys.executable
    t0 = time.monotonic()
    ingesta = subprocess.Popen([py, "consumidor_ingesta.py"])
    tablero = subprocess.Popen([py, "consumidor_tablero.py"])
    time.sleep(3)  # que los grupos existan antes de producir
    productor = subprocess.Popen([py, "productor.py"])
    for p in (productor, ingesta, tablero):
        p.wait()
    print(f"pipeline en vivo terminado en {time.monotonic() - t0:.1f} s")
    if any(p.returncode for p in (productor, ingesta, tablero)):
        sys.exit("algun proceso termino con error")


if __name__ == "__main__":
    main()
