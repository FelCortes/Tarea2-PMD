"""Parte C - Pregunta 6 (la propia): congestion anomala.

  ¿Que estaciones de flujo vehicular tienen AHORA (promedio de su ventana de 10 min) un nivel por encima del
  percentil 90 de sus propios promedios de 10 minutos de los 5 dias de historia?

  Redis decide el "ahora" y el "cual": catalogo tipo:flujo_vehicular + ventana:* (promedio de los ultimos 10 min).
  HBase decide el "que es normal": la historia completa de cada estacion (miles de filas por estacion, en disco).
"""
from collections import defaultdict

from comun import conectar
from medicion import equipo, fmt, medir
from p1_p3 import conectar_hbase, p2_calculo

HIST_INI, HIST_FIN = "2026-11-02T00:00:00.000-03:00", "2026-11-07T00:00:00.000-03:00"   # los 5 dias historicos


def percentil(xs, p):
    xs = sorted(xs)
    return xs[int(p * (len(xs) - 1))]


def historia_hbase(t, est):
    """Promedio por bloque de 10 minutos (los primeros 15 caracteres del ts: 'YYYY-MM-DDTHH:M') de toda la historia."""
    bloques = defaultdict(list)
    n = len(est) + 1
    for k, d in t.scan(row_start=f"{est}#{HIST_INI}".encode(), row_stop=f"{est}#{HIST_FIN}".encode(),
                       columns=[b"d:valor"], batch_size=5000):
        bloques[k[n:n + 15]].append(float(d[b"d:valor"]))
    medias = [sum(v) / len(v) for v in bloques.values()]
    return {"bloques": len(medias), "p50": percentil(medias, 0.5), "p90": percentil(medias, 0.9)}


def p6(r, t):
    _T, ahora = p2_calculo(r)                                   # REDIS: ventanas de 10 min de las estaciones de flujo
    filas = []
    for est, (n, avg) in ahora.items():
        h = historia_hbase(t, est)                              # HBase: historia de esa estacion
        filas.append((est, avg, h["p50"], h["p90"], h["bloques"], avg > h["p90"]))
    return filas


def main():
    print("Equipo:", equipo())
    r, conn = conectar(), conectar_hbase()
    t = conn.table("lecturas")
    print("\n===== P6: congestion anomala (Redis = ahora, HBase = que es normal) =====")
    # tiempos por separado (mediana de 5)
    t_redis, _ = medir(lambda: p2_calculo(r))
    t_hbase, _ = medir(lambda: historia_hbase(t, "CC-001"), corridas=5)
    t_total, filas = medir(lambda: p6(r, t))
    print(f"Parte Redis (ventanas de 10 min, 8 estaciones): {fmt(t_redis)}")
    print(f"Parte HBase (historia de UNA estacion, ~54.000 filas): {fmt(t_hbase)}")
    print(f"Pregunta completa (8 estaciones): {fmt(t_total)}")
    print("\nestacion | ahora(10 min) | p50 hist. | p90 hist. | bloques de 10 min | ahora > p90?")
    for est, avg, p50, p90, nb, alerta in filas:
        print(f"  {est} | {avg:6.2f} | {p50:6.2f} | {p90:6.2f} | {nb} | {'SI  <-- ALERTA' if alerta else 'no'}")
    print(f"\nEstaciones en alerta: {[f[0] for f in filas if f[5]] or 'ninguna'}")


if __name__ == "__main__":
    main()
