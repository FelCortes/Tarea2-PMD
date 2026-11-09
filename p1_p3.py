"""Parte C - Preguntas 1, 2 y 3.   Uso: python p1_p3.py [p1] [p2] [p3]   (sin argumentos: las tres)"""
import sys
from collections import defaultdict
from datetime import datetime

import happybase

from comun import STREAM, VENTANA_MS, conectar, ts_ms
from generador import ESTACIONES
from medicion import equipo, fmt, medir

EST = [e for e, _ in ESTACIONES]
TIPO = dict(ESTACIONES)


def conectar_hbase():
    return happybase.Connection("localhost", port=9090, timeout=300_000)


# ------------------------------------------------------------------ P1
def p1_redis(r):
    pipe = r.pipeline()
    for e in EST:
        pipe.hgetall(f"ultimo:{e}")
    ultimos = dict(zip(EST, pipe.execute()))
    top5 = r.zrevrange("ranking:flujo", 0, 4, withscores=True)
    return ultimos, top5


def hacer_reverse(tabla):
    """Scan inverso de una estacion (ultima medicion). Detecta el orden start/stop que acepta el servidor."""
    def kw(est, orden):
        ini, fin = f"{est}#".encode(), f"{est}$".encode()   # '$' (0x24) es justo despues de '#' (0x23)
        return dict(row_start=fin, row_stop=ini) if orden == 0 else dict(row_start=ini, row_stop=fin)
    for orden in (0, 1):
        if list(tabla.scan(reverse=True, limit=1, **kw("CC-001", orden))):
            return lambda est: kw(est, orden)
    raise RuntimeError("no pude hacer un scan inverso")


def p1_hbase(tabla, kw):
    ultimos = {}
    for e in EST:
        for k, d in tabla.scan(reverse=True, limit=1, columns=[b"d:valor", b"d:tipo"], **kw(e)):
            ultimos[e] = {"valor": d[b"d:valor"].decode(), "tipo": d[b"d:tipo"].decode(),
                          "ts": k.decode().split("#", 1)[1]}
    flujo = [(e, float(u["valor"])) for e, u in ultimos.items() if u["tipo"] == "flujo_vehicular"]
    top5 = sorted(flujo, key=lambda x: -x[1])[:5]
    return ultimos, top5


def llegada_vs_medicion(r):
    llegada, medicion = {}, {}
    for _id, ev in r.xrange(STREAM):
        e, m = ev["estacion_id"], ts_ms(ev["timestamp"])
        llegada[e] = ev                                  # el ultimo en llegar
        if e not in medicion or m >= medicion[e][0]:
            medicion[e] = (m, ev)                        # el de mayor timestamp de medicion
    return [(e, llegada[e], medicion[e][1]) for e in EST
            if llegada[e]["timestamp"] != medicion[e][1]["timestamp"]]


def p1():
    print("\n===== P1: Ahora - Redis contra HBase =====")
    r, conn = conectar(), conectar_hbase()
    tabla = conn.table("lecturas")
    kw = hacer_reverse(tabla)
    t_r, (ult_r, top_r) = medir(lambda: p1_redis(r))
    t_h, (ult_h, top_h) = medir(lambda: p1_hbase(tabla, kw))
    print(f"Redis (16 HGETALL en pipeline + ZREVRANGE): {fmt(t_r)}")
    print(f"HBase (16 scans inversos limit=1 + orden en Python): {fmt(t_h)}")
    print(f"HBase es {t_h / t_r:.0f}x mas lento que Redis")
    print("Top 5 flujo (Redis):", [(e, v) for e, v in top_r])
    print("Top 5 flujo (HBase):", top_h)
    iguales = all(float(ult_r[e]["valor"]) == float(ult_h[e]["valor"]) for e in EST)
    print("Ultimo valor por estacion coincide entre motores:", iguales)
    for e in EST:
        print(f"  {e}: valor={ult_r[e]['valor']:>6}  tipo={ult_r[e]['tipo']}")
    dif = llegada_vs_medicion(r)
    print(f"\nEstaciones cuyo ultimo cambia segun medicion vs llegada: {[d[0] for d in dif]}")
    for e, lleg, med in dif:
        print(f"  {e}: por LLEGADA   valor={lleg['valor']}  ts_medicion={lleg['timestamp']}")
        print(f"  {e}: por MEDICION  valor={med['valor']}  ts_medicion={med['timestamp']}   <- el que guarda el tablero")


# ------------------------------------------------------------------ P2
def p2_calculo(r):
    pipe = r.pipeline()
    for e in EST:
        pipe.hget(f"ultimo:{e}", "ts")
    T = max(int(x) for x in pipe.execute())          # medicion mas reciente del stream
    flujo = sorted(r.smembers("tipo:flujo_vehicular"))
    pipe = r.pipeline()
    for e in flujo:
        pipe.zrangebyscore(f"ventana:{e}", T - VENTANA_MS, T)
    out = {}
    for e, miembros in zip(flujo, pipe.execute()):
        vals = [float(m.rsplit(":", 1)[1]) for m in miembros]
        out[e] = (len(vals), sum(vals) / len(vals))
    return T, out


def p2():
    print("\n===== P2: Ventana de 10 minutos - Redis =====")
    r = conectar()
    t, (T, out) = medir(lambda: p2_calculo(r))
    print(f"Ancla (medicion mas reciente): {datetime.fromtimestamp(T / 1000).astimezone().isoformat()}  [{T} ms]")
    print(f"Tiempo (mediana de 5): {fmt(t)}")
    for e, (n, avg) in out.items():
        print(f"  {e}: {n:>3} lecturas, promedio de flujo = {avg:.2f}")
    # memoria: ventana recortada vs. sin recortar (todas las lecturas de la estacion en la hora)
    e = "CC-001"
    con = r.memory_usage(f"ventana:{e}")
    todos = {f"{ts_ms(ev['timestamp'])}:{ev['valor']}": ts_ms(ev["timestamp"])
             for _id, ev in r.xrange(STREAM) if ev["estacion_id"] == e}
    r.zadd("tmp:sinrecorte", todos)
    sin = r.memory_usage("tmp:sinrecorte")
    r.delete("tmp:sinrecorte")
    print(f"\nMEMORY USAGE ventana:{e} recortada ({r.zcard(f'ventana:{e}')} elementos): {con} bytes")
    print(f"MEMORY USAGE sin recortar, 1 hora ({len(todos)} elementos): {sin} bytes")
    print(f"Sin recorte crece lineal con el tiempo: ~{sin / len(todos):.0f} bytes/elemento "
          f"-> 1 dia ~ {sin * 24 / 1e6:.1f} MB y 1 ano ~ {sin * 24 * 365 / 1e6:.0f} MB, SOLO esta estacion.")


# ------------------------------------------------------------------ P3
INI = b"CC-001#2026-11-04T08:00:00.000-03:00"
FIN = b"CC-001#2026-11-04T10:00:00.000-03:00"   # exclusivo


def agregar(filas):
    cuartos = defaultdict(list)
    for k, v in filas:
        ts = datetime.fromisoformat(k.decode().split("#", 1)[1])
        cuartos[f"{ts.hour:02d}:{ts.minute // 15 * 15:02d}"].append(float(v))
    return len(filas), {q: (len(v), sum(v) / len(v)) for q, v in sorted(cuartos.items())}


def p3_rango(t):
    return agregar([(k, d[b"d:valor"]) for k, d in t.scan(row_start=INI, row_stop=FIN, columns=[b"d:valor"])])


def p3_completo(t, modo):
    if modo == "servidor":
        flt = f"RowFilter(>=, 'binary:{INI.decode()}') AND RowFilter(<, 'binary:{FIN.decode()}')"
        return agregar([(k, d[b"d:valor"]) for k, d in
                        t.scan(filter=flt, columns=[b"d:valor"], batch_size=10000)])
    return agregar([(k, d[b"d:valor"]) for k, d in t.scan(columns=[b"d:valor"], batch_size=10000)
                    if INI <= k < FIN])


def p3():
    print("\n===== P3: Un rango - HBase (CC-001, 04-nov, [08:00, 10:00)) =====")
    t = conectar_hbase().table("lecturas")
    t_r, (n, cuartos) = medir(lambda: p3_rango(t))
    print(f"Scan por rango (row_start/row_stop): {fmt(t_r)} -> {n} lecturas")
    modo = "servidor"
    try:
        p3_completo(t, modo)
    except Exception as ex:
        modo = "cliente"
        print(f"(filtro en servidor no disponible: {type(ex).__name__}; se filtra en el cliente)")
    t_f, (n2, cuartos2) = medir(lambda: p3_completo(t, modo), corridas=5 if modo == "servidor" else 3)
    print(f"Tabla entera + filtro ({modo}): {fmt(t_f)} -> {n2} lecturas")
    print(f"El recorrido completo es {t_f / t_r:.0f}x mas lento. Mismo resultado: {(n, cuartos) == (n2, cuartos2)}")
    print("Promedio por cuarto de hora:")
    for q, (c, avg) in cuartos.items():
        print(f"  {q}  n={c:>3}  promedio={avg:.2f}")
    print("\nExtrapolacion: el scan por rango lee ~solo las filas pedidas (no depende del tamano de la tabla);")
    print("el recorrido completo crece lineal: con 5 anos (x365 datos) tardaria ~"
          f"{t_f * 365:.0f} s ({t_f * 365 / 60:.1f} min) en este equipo, vs. casi lo mismo del rango.")


if __name__ == "__main__":
    print("Equipo:", equipo())
    pedidas = sys.argv[1:] or ["p1", "p2", "p3"]
    for nombre in pedidas:
        globals()[nombre]()
