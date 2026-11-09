"""Parte D.1 - El consumidor 'ingesta' muere a mitad de lote: despues del put y antes del XACK.

Partimos de cero: recarga el historico (HBase queda con 846.906 filas), vacia Redis, llena el stream
con la hora en vivo de golpe (la velocidad del productor no importa aqui), mata a ing-1 y recupera con ing-2.
"""
import subprocess
import sys

import happybase

from carga_historica import clave_lecturas, contar
from comun import GRUPO_INGESTA, STREAM, conectar
from datos import vivo

PY = sys.executable


def titulo(t):
    print(f"\n--- {t} ---", flush=True)


def main():
    r = conectar()
    titulo("0. Preparacion: historico desde cero + stream lleno")
    subprocess.run([PY, "carga_historica.py"], check=True)
    r.flushall()
    n = 0
    for n, ev in enumerate(vivo(), start=1):
        r.xadd(STREAM, ev)
    r.set("meta:producidos", n)
    print(f"stream con {r.xlen(STREAM)} entradas")

    conn = happybase.Connection("localhost", port=9090, timeout=300_000)
    tabla = conn.table("lecturas")

    titulo("1. ing-1 muere tras el put del lote 5, antes del XACK")
    p = subprocess.run([PY, "consumidor_ingesta.py", "--nombre", "ing-1", "--morir-en", "5"])
    print(f"ing-1 termino con codigo {p.returncode}")
    resumen = r.xpending(STREAM, GRUPO_INGESTA)
    print(f"XPENDING: {resumen['pending']} entradas pendientes, ids {resumen['min']} .. {resumen['max']}")
    print("por consumidor:", resumen["consumers"])
    detalle = r.xpending_range(STREAM, GRUPO_INGESTA, min="-", max="+", count=3)
    print("primeras 3 pendientes:", [(d["message_id"], d["consumer"], d["times_delivered"]) for d in detalle])
    # una fila de un evento pendiente: ya esta en HBase (el put se hizo), con 1 version
    ev_id = resumen["min"]
    ev = r.xrange(STREAM, ev_id, ev_id)[0][1]
    clave = clave_lecturas(ev)
    antes = tabla.cells(clave, b"d:valor", versions=5, include_timestamp=True)
    print(f"antes de recuperar, fila {clave.decode()}: {len(antes)} version(es) -> {antes}")
    print(f"filas en lecturas tras la muerte: {contar(conn, 'lecturas')}  (el put del lote 5 si quedo)")

    titulo("2. ing-2 recupera los pendientes (XAUTOCLAIM) y sigue con el resto")
    subprocess.run([PY, "consumidor_ingesta.py", "--nombre", "ing-2", "--recuperar"], check=True)
    resumen = r.xpending(STREAM, GRUPO_INGESTA)
    print(f"XPENDING final: {resumen['pending']}")

    titulo("3. HBase tiene exactamente lo que dicen los numeros de control?")
    for t in ("lecturas", "lecturas_tipo"):
        c = contar(conn, t)
        print(f"{t}: {c} filas (esperado 853953) -> {'OK' if c == 853953 else 'NO CALZA'}")

    titulo("4. Por que reprocesar no duplica: versiones de la celda")
    despues = tabla.cells(clave, b"d:valor", versions=5, include_timestamp=True)
    print(f"fila {clave.decode()}: {len(despues)} version(es) (valor, timestamp-HBase):")
    for v, ts in despues:
        print(f"   valor={v.decode()}  ts_celda={ts}")
    print("La clave sale de (estacion, medicion): el reproceso escribe en la MISMA fila, no crea otra.")
    print("Solo se agrega una version de la celda, con el mismo valor; una lectura normal ve la mas reciente.")


if __name__ == "__main__":
    main()
