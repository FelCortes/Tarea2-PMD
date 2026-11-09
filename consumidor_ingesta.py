"""Parte B.2 - Grupo 'ingesta': stream -> HBase (lecturas y lecturas_tipo). XACK DESPUES del put.

  python consumidor_ingesta.py                    # normal
  python consumidor_ingesta.py --morir-en 5       # D.1: muere tras el put del lote 5, antes del XACK
  python consumidor_ingesta.py --nombre ing-2 --recuperar   # D.1: reclama los pendientes de otro
"""
import argparse
import os
import time

import happybase

from carga_historica import celdas, clave_lecturas, clave_tipo
from comun import GRUPO_INGESTA, STREAM, conectar, crear_grupo

LOTE = 500


def put_lote(lote, t_lec, t_tipo):
    with t_lec.batch() as b1, t_tipo.batch() as b2:
        for _id, ev in lote:
            c = celdas(ev)
            b1.put(clave_lecturas(ev), c)
            b2.put(clave_tipo(ev), c)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nombre", default="ing-1")
    ap.add_argument("--morir-en", type=int, default=0)
    ap.add_argument("--recuperar", action="store_true")
    a = ap.parse_args()

    r = conectar()
    crear_grupo(r, GRUPO_INGESTA)
    conn = happybase.Connection("localhost", port=9090, timeout=120_000)
    t_lec, t_tipo = conn.table("lecturas"), conn.table("lecturas_tipo")

    if a.recuperar:  # reclama las entradas pendientes de consumidores muertos
        cursor, total = "0-0", 0
        while True:
            res = r.xautoclaim(STREAM, GRUPO_INGESTA, a.nombre, min_idle_time=0,
                               start_id=cursor, count=LOTE)
            cursor, lote = res[0], res[1]
            if lote:
                put_lote(lote, t_lec, t_tipo)
                r.xack(STREAM, GRUPO_INGESTA, *[i for i, _ in lote])
                total += len(lote)
            if cursor == "0-0":
                break
        print(f"{a.nombre}: recupero {total} entradas pendientes", flush=True)

    n_lotes = total_proc = vacios = 0
    while True:
        resp = r.xreadgroup(GRUPO_INGESTA, a.nombre, {STREAM: ">"}, count=LOTE, block=1000)
        if not resp:
            vacios += 1
            if vacios >= 3 and r.exists("meta:producidos"):
                break
            continue
        vacios = 0
        lote = resp[0][1]
        put_lote(lote, t_lec, t_tipo)
        n_lotes += 1
        if a.morir_en and n_lotes == a.morir_en:
            print(f"{a.nombre}: MUERE tras el put del lote {n_lotes}, sin XACK", flush=True)
            os._exit(1)
        r.xack(STREAM, GRUPO_INGESTA, *[i for i, _ in lote])  # solo despues del put
        total_proc += len(lote)
    print(f"{a.nombre}: {total_proc} entradas procesadas", flush=True)


if __name__ == "__main__":
    main()
