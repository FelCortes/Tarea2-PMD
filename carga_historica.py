"""Parte B.1 - Carga historica (864.000 eventos) directo a HBase, sin pasar por Redis.

Tablas (familia 'd', VERSIONS=5 para poder mostrar versiones en la Parte D):
  lecturas       row key = estacion_id # timestamp_medicion
                 splits: una region por comuna (prefijo del estacion_id)
  lecturas_tipo  row key = tipo_sensor # timestamp_medicion # estacion_id
                 splits: una region por tipo de sensor

La clave sale de la MEDICION (estacion + timestamp), nunca de la llegada:
un evento repetido o tardio cae en la misma fila y la sobreescribe (idempotente).
"""
import subprocess
import time

import happybase

from datos import historico

CONTENEDOR = "t2-hbase"
SPLITS_LECTURAS = ["CHI-001", "COR-001", "HUA-001", "PEN-001", "SPP-001", "TAL-001"]
SPLITS_TIPO = ["flujo_vehicular", "ocupacion_estacionamiento"]
TAM_BATCH = 5000


def clave_lecturas(ev):
    return f"{ev['estacion_id']}#{ev['timestamp']}".encode()


def clave_tipo(ev):
    return f"{ev['tipo_sensor']}#{ev['timestamp']}#{ev['estacion_id']}".encode()


def celdas(ev):
    return {b"d:valor": str(ev["valor"]).encode(), b"d:tipo": ev["tipo_sensor"].encode()}


def hbase_shell(comandos):
    """happybase (Thrift) no permite SPLITS al crear tablas: se usa el shell de HBase."""
    r = subprocess.run(["docker", "exec", "-i", CONTENEDOR, "hbase", "shell", "-n"],
                       input=comandos, text=True, capture_output=True)
    if r.returncode != 0:
        raise RuntimeError(f"hbase shell fallo:\n{r.stdout}\n{r.stderr}")


def crear_tablas(conn):
    existentes = {t.decode() for t in conn.tables()}
    cmds = []
    for nombre in ("lecturas", "lecturas_tipo"):
        if nombre in existentes:  # el script final debe poder correr desde cero y repetirse
            cmds += [f"disable '{nombre}'", f"drop '{nombre}'"]
    fam = "{NAME => 'd', VERSIONS => 5}"
    sp1 = ", ".join(f"'{s}'" for s in SPLITS_LECTURAS)
    sp2 = ", ".join(f"'{s}'" for s in SPLITS_TIPO)
    cmds.append(f"create 'lecturas', {fam}, SPLITS => [{sp1}]")
    cmds.append(f"create 'lecturas_tipo', {fam}, SPLITS => [{sp2}]")
    hbase_shell("\n".join(cmds) + "\nexit\n")


def contar(conn, tabla):
    t = conn.table(tabla)
    return sum(1 for _ in t.scan(filter="FirstKeyOnlyFilter() AND KeyOnlyFilter()", batch_size=10000))


def main():
    conn = happybase.Connection("localhost", port=9090, timeout=120_000)
    print("Creando tablas pre-divididas...")
    crear_tablas(conn)
    t_lec, t_tipo = conn.table("lecturas"), conn.table("lecturas_tipo")

    print("Cargando historico (batch)...")
    n = 0
    t0 = time.perf_counter()
    with t_lec.batch(batch_size=TAM_BATCH) as b1, t_tipo.batch(batch_size=TAM_BATCH) as b2:
        for ev in historico():
            c = celdas(ev)
            b1.put(clave_lecturas(ev), c)
            b2.put(clave_tipo(ev), c)
            n += 1
            if n % 100_000 == 0:
                print(f"  {n:>7} eventos  ({n / (time.perf_counter() - t0):,.0f} ev/s)", flush=True)
    dt = time.perf_counter() - t0
    print(f"Carga: {n} eventos en {dt:.1f} s -> {n / dt:,.0f} eventos/s "
          f"({2 * n / dt:,.0f} filas escritas/s, 2 tablas)")

    print("Contando filas (FirstKeyOnlyFilter AND KeyOnlyFilter)...")
    for tabla in ("lecturas", "lecturas_tipo"):
        print(f"  {tabla}: {contar(conn, tabla)} filas (esperado historico: 846906)")


if __name__ == "__main__":
    main()
