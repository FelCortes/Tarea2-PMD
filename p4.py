"""Parte C - Pregunta 4: dos patrones de acceso (catalogo en Redis + HBase en dos tablas)."""
import subprocess
import time

from carga_historica import hbase_shell
from comun import conectar
from medicion import equipo, fmt, medir
from p1_p3 import conectar_hbase

INI = "2026-11-05T07:00:00.000-03:00"
FIN = "2026-11-05T09:00:00.000-03:00"   # exclusivo, igual que en P3


def estadisticas(filas):
    """filas: (estacion, ts, valor) en orden de clave (= orden de tiempo). Maximo estricto: gana la primera ocurrencia."""
    acc = {}
    for est, ts, v in filas:
        a = acc.setdefault(est, {"n": 0, "suma": 0.0, "max": None, "ts_max": None})
        a["n"] += 1
        a["suma"] += v
        if a["max"] is None or v > a["max"]:
            a["max"], a["ts_max"] = v, ts
    return {e: (a["n"], round(a["suma"] / a["n"], 2), a["max"], a["ts_max"]) for e, a in sorted(acc.items())}


def p4a(r, t):
    """lecturas (estacion primero): un scan por estacion; la lista de estaciones sale del catalogo de Redis."""
    filas = []
    for e in sorted(r.smembers("tipo:calidad_aire")):
        for k, d in t.scan(row_start=f"{e}#{INI}".encode(), row_stop=f"{e}#{FIN}".encode(), columns=[b"d:valor"]):
            filas.append((e, k.decode().split("#")[1], float(d[b"d:valor"])))
    return estadisticas(filas)


def p4b(t):
    """lecturas_tipo (tipo primero): UN solo scan para todas las estaciones de calidad del aire."""
    filas = []
    for k, d in t.scan(row_start=f"calidad_aire#{INI}".encode(), row_stop=f"calidad_aire#{FIN}".encode(),
                       columns=[b"d:valor"]):
        _tipo, ts, est = k.decode().split("#")
        filas.append((est, ts, float(d[b"d:valor"])))
    return estadisticas(filas)


def tam_disco():
    hbase_shell("flush 'lecturas'\nflush 'lecturas_tipo'\nexit\n")
    time.sleep(3)
    out = {}
    for tabla in ("lecturas", "lecturas_tipo"):
        r = subprocess.run(["docker", "exec", "t2-hbase", "du", "-sk", f"/data/hbase/data/default/{tabla}"],
                           capture_output=True, text=True)
        out[tabla] = int(r.stdout.split()[0]) if r.returncode == 0 and r.stdout else None
    return out


def main():
    print("Equipo:", equipo())
    r, conn = conectar(), conectar_hbase()
    t_lec, t_tipo = conn.table("lecturas"), conn.table("lecturas_tipo")
    print("\n===== P4: PM2.5 entre 07:00 y 09:00 del 05-nov [07:00, 09:00) =====")
    ta, ra = medir(lambda: p4a(r, t_lec))
    tb, rb = medir(lambda: p4b(t_tipo))
    print(f"(a) lecturas, un rango por estacion (catalogo Redis): {fmt(ta)}   [{len(ra)} scans + 1 SMEMBERS]")
    print(f"(b) lecturas_tipo, un solo scan:                      {fmt(tb)}   [1 scan]")
    print(f"(a) es {ta / tb:.1f}x el tiempo de (b).  Mismo resultado: {ra == rb}")
    print("\nestacion | n | promedio | maximo | ts del maximo (primera ocurrencia)")
    for e, (n, avg, mx, ts) in rb.items():
        print(f"  {e} | {n} | {avg} | {mx} | {ts}")

    print("\n--- Costo de tener la segunda tabla ---")
    print("Escrituras: 2 puts por evento (lecturas + lecturas_tipo): historico 864.000 -> 1.728.000 puts; en vivo 7.200 -> 14.400.")
    d = tam_disco()
    print(f"Disco (du -sk en el contenedor, tras flush): lecturas={d['lecturas']} KB, lecturas_tipo={d['lecturas_tipo']} KB")
    if all(d.values()):
        print(f"Total con 2 tablas: {sum(d.values())} KB = {sum(d.values()) / d['lecturas']:.2f}x lo que ocuparia solo 'lecturas'")


if __name__ == "__main__":
    main()
