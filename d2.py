"""Parte D.2 - Redis se reinicia con el pipeline a medio camino (3.000 eventos producidos).

Configuracion elegida (AOF everysec + volumen, contenedor t2-redis, puerto 6379)
contra su contraria (sin persistencia, contenedor t2-redis-nop, puerto 6380).
Ademas: AOF con `docker kill` (SIGKILL) inmediato, para ver la ventana real de everysec.
"""
import itertools
import subprocess
import time

import happybase

from comun import STREAM, conectar, crear_grupo, ts_ms
from consumidor_ingesta import put_lote
from consumidor_tablero import LUA
from datos import vivo

N = 3000
LOTE = 500
D2 = ["docker", "compose", "-p", "t2d2", "-f", "docker-compose.d2.yml"]


def sh(*cmd):
    subprocess.run(list(cmd), check=True, capture_output=True)


def esperar(port, seg=40):
    t0 = time.time()
    while time.time() - t0 < seg:
        try:
            r = conectar(port)
            r.ping()
            return r
        except Exception:
            time.sleep(0.5)
    raise RuntimeError("Redis no volvio")


def foto(r):
    if r.exists(STREAM):
        xlen = r.xlen(STREAM)
        grupos = {g["name"]: f"pendientes={g['pending']} lag={g.get('lag')}" for g in r.xinfo_groups(STREAM)}
    else:
        xlen, grupos = 0, {}
    return {
        "stream (XLEN)": xlen,
        "grupos": grupos or "(ninguno)",
        "ultimo:* (claves)": len(list(r.scan_iter("ultimo:*"))),
        "ventana:* (claves)": len(list(r.scan_iter("ventana:*"))),
        "ventana:CC-001 (elementos)": r.zcard("ventana:CC-001"),
        "ranking:flujo (estaciones)": r.zcard("ranking:flujo"),
    }


def preparar(r, tablas):
    """3.000 eventos al stream; tablero los procesa todos; ingesta lleva 1.500 a HBase (con ACK)
    y deja otro lote de 500 leido pero sin confirmar (en vuelo)."""
    r.flushall()
    for ev in itertools.islice(vivo(), N):
        r.xadd(STREAM, ev)
    crear_grupo(r, "ingesta")
    crear_grupo(r, "tablero")
    script = r.register_script(LUA)
    while True:
        resp = r.xreadgroup("tablero", "tab-1", {STREAM: ">"}, count=LOTE)
        if not resp:
            break
        lote = resp[0][1]
        pipe = r.pipeline()
        for _id, ev in lote:
            script(keys=[f"ultimo:{ev['estacion_id']}", "ranking:flujo", f"ventana:{ev['estacion_id']}",
                         f"tipo:{ev['tipo_sensor']}"],
                   args=[ev["estacion_id"], ev["valor"], ts_ms(ev["timestamp"]), ev["tipo_sensor"]], client=pipe)
        pipe.execute()
        r.xack(STREAM, "tablero", *[i for i, _ in lote])
    for _ in range(3):  # 3 lotes completos: put en HBase y luego XACK
        lote = r.xreadgroup("ingesta", "ing-1", {STREAM: ">"}, count=LOTE)[0][1]
        put_lote(lote, *tablas)
        r.xack(STREAM, "ingesta", *[i for i, _ in lote])
    r.xreadgroup("ingesta", "ing-1", {STREAM: ">"}, count=LOTE)  # en vuelo: leido, sin put ni XACK


def escenario(nombre, port, reiniciar, tablas):
    print(f"\n=========== {nombre} ===========")
    r = esperar(port)
    preparar(r, tablas)
    antes = foto(r)
    g = r.xinfo_groups(STREAM)
    ing = next(x for x in g if x["name"] == "ingesta")
    print(f"Antes: {N} producidos, 1500 ya en HBase (confirmados). "
          f"En Redis y NO en HBase: {ing['pending'] + (ing.get('lag') or 0)} "
          f"({ing['pending']} en vuelo + {ing.get('lag')} sin entregar)")
    reiniciar()
    r = esperar(port)
    despues = foto(r)
    print(f"{'':32}{'ANTES':<38}DESPUES del restart")
    for k in antes:
        print(f"  {k:<30}{str(antes[k]):<38}{despues[k]}")


def kill_aof():
    print("\n=========== AOF everysec + docker kill (SIGKILL) inmediato ===========")
    evs = list(itertools.islice(vivo(), N))
    for i in range(3):
        r = esperar(6379)
        r.flushall()
        t0 = time.time()
        for ev in evs:
            r.xadd(STREAM, ev)
        dt = time.time() - t0
        sh("docker", "kill", "t2-redis")
        sh("docker", "start", "t2-redis")
        r = esperar(6379)
        print(f"  intento {i + 1}: {N} XADD en {dt:.2f} s, kill inmediato -> XLEN tras reinicio = "
              f"{r.xlen(STREAM) if r.exists(STREAM) else 0}  (perdidos: {N - (r.xlen(STREAM) if r.exists(STREAM) else 0)})")


def main():
    conn = happybase.Connection("localhost", port=9090, timeout=300_000)
    tablas = (conn.table("lecturas"), conn.table("lecturas_tipo"))
    sh(*D2, "up", "-d", "--wait")
    try:
        escenario("A) AOF everysec + volumen (la elegida) - docker compose restart redis", 6379,
                  lambda: sh("docker", "compose", "restart", "redis"), tablas)
        escenario("B) SIN persistencia (la contraria) - docker restart", 6380,
                  lambda: sh("docker", "restart", "t2-redis-nop"), tablas)
        kill_aof()
    finally:
        sh(*D2, "down")
    print("\nOJO: Redis (6379) quedo con datos del experimento. Para restaurar el tablero: python main_pipeline.py")


if __name__ == "__main__":
    main()
