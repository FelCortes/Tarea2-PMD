"""Parte C - Pregunta 5: hotspotting y salting. La hora en vivo en 3 tablas, misma informacion, distinta clave.

  t5_est   estacion#ts          pre-dividida por estacion (como 'lecturas')
  t5_ts    ts#estacion          pre-dividida por dia
  t5_salt  salt#ts#estacion     salt = crc32(estacion) % 4, pre-dividida por salt
"""
import json
import re
import time
import zlib
from urllib.parse import quote
from urllib.request import urlopen

from carga_historica import SPLITS_LECTURAS, hbase_shell
from datos import vivo
from generador import ESTACIONES
from medicion import equipo, fmt, medir
from p1_p3 import conectar_hbase

EST = [e for e, _ in ESTACIONES]
DIAS = ["2026-11-03", "2026-11-04", "2026-11-05", "2026-11-06", "2026-11-07"]
INI, FIN = "2026-11-07T00:10:00.000-03:00", "2026-11-07T00:20:00.000-03:00"
TABLAS = {"t5_est": SPLITS_LECTURAS, "t5_ts": DIAS, "t5_salt": ["1", "2", "3"]}


def salt(est):
    return zlib.crc32(est.encode()) % 4   # determinista (hash() de Python cambia entre ejecuciones)


def k_est(ev): return f"{ev['estacion_id']}#{ev['timestamp']}".encode()
def k_ts(ev): return f"{ev['timestamp']}#{ev['estacion_id']}".encode()
def k_salt(ev): return f"{salt(ev['estacion_id'])}#{ev['timestamp']}#{ev['estacion_id']}".encode()


def crear(conn):
    existentes = {t.decode() for t in conn.tables()}
    cmds = []
    for t in TABLAS:
        if t in existentes:
            cmds += [f"disable '{t}'", f"drop '{t}'"]
    for t, splits in TABLAS.items():
        sp = ", ".join(f"'{s}'" for s in splits)
        cmds.append(f"create '{t}', {{NAME => 'd', VERSIONS => 1}}, SPLITS => [{sp}]")
    hbase_shell("\n".join(cmds) + "\nexit\n")


def escribir(conn):
    ts = {t: conn.table(t) for t in TABLAS}
    n = 0
    with ts["t5_est"].batch(batch_size=500) as b1, ts["t5_ts"].batch(batch_size=500) as b2, \
            ts["t5_salt"].batch(batch_size=500) as b3:
        for ev in vivo():
            c = {b"d:valor": str(ev["valor"]).encode(), b"d:tipo": ev["tipo_sensor"].encode()}
            b1.put(k_est(ev), c)
            b2.put(k_ts(ev), c)
            b3.put(k_salt(ev), c)
            n += 1
    print(f"Escritos {n} eventos en cada una de las 3 tablas")


def escrituras_por_region(conn):
    print("Esperando a que JMX publique (~15 s)...")
    time.sleep(15)
    url = "http://localhost:16010/jmx?qry=" + quote("Hadoop:service=HBase,name=RegionServer,sub=Regions", safe=":,=")
    bean = json.load(urlopen(url, timeout=30))["beans"][0]
    pat = re.compile(r"Namespace_default_table_(\w+?)_region_([0-9a-f]{32})_metric_(\w+)$")
    metricas = {}
    for k, v in bean.items():
        m = pat.match(k)
        if m:
            metricas.setdefault((m.group(1), m.group(2)), {})[m.group(3)] = v
    resultado = {}
    for t in TABLAS:
        filas = []
        for reg in conn.table(t).regions():
            enc = reg["name"].decode().rstrip(".").rsplit(".", 1)[1]
            m = metricas.get((t, enc))
            if m is None:
                continue
            w = m.get("writeRequestCount", m.get("putCount"))
            if w is None:
                raise RuntimeError(f"metricas disponibles para {t}: {sorted(m)}")
            inicio = reg["start_key"].decode() or "(inicio)"
            filas.append((inicio, int(w)))
        resultado[t] = filas
    return resultado


def consultas(conn):
    t_est, t_ts, t_salt = (conn.table(t) for t in TABLAS)

    def q_est():  # 16 rangos
        n = 0
        for e in EST:
            n += sum(1 for _ in t_est.scan(row_start=f"{e}#{INI}".encode(), row_stop=f"{e}#{FIN}".encode(),
                                           columns=[b"d:valor"]))
        return n

    def q_ts():  # 1 rango
        return sum(1 for _ in t_ts.scan(row_start=INI.encode(), row_stop=FIN.encode(), columns=[b"d:valor"]))

    def q_salt():  # 4 rangos
        n = 0
        for s in range(4):
            n += sum(1 for _ in t_salt.scan(row_start=f"{s}#{INI}".encode(), row_stop=f"{s}#{FIN}".encode(),
                                            columns=[b"d:valor"]))
        return n

    return [("t5_est   estacion#ts      (16 rangos)", q_est), ("t5_ts    ts#estacion      (1 rango)", q_ts),
            ("t5_salt  salt#ts#estacion (4 rangos)", q_salt)]


def main():
    print("Equipo:", equipo())
    conn = conectar_hbase()
    print("\n===== P5: hotspotting y salting =====")
    crear(conn)
    escribir(conn)
    por_region = escrituras_por_region(conn)

    print("\n--- Escrituras recibidas por region (7.200 puts por tabla) ---")
    for t, filas in por_region.items():
        total = sum(w for _, w in filas) or 1
        print(f"\n{t}: {len(filas)} regiones, total {total}")
        for inicio, w in sorted(filas):
            print(f"   region desde {inicio:<14} {w:>6} escrituras  {100 * w / total:5.1f} %  {'#' * int(40 * w / total)}")
        print(f"   -> la region mas cargada recibe {100 * max(w for _, w in filas) / total:.1f} % de las escrituras")

    print("\n--- Todas las lecturas de todas las estaciones, 07-nov [00:10, 00:20) ---")
    for nombre, fn in consultas(conn):
        t, n = medir(fn)
        print(f"{nombre}: {fmt(t)}  -> {n} filas")


if __name__ == "__main__":
    main()
