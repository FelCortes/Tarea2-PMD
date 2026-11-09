"""Parte B.3 - Verificacion final: compara el estado del pipeline con los numeros de control."""
import sys

import happybase

from carga_historica import contar
from comun import STREAM, conectar

# Numeros de control del enunciado (historico + en vivo)
GENERADOS = 871_200
REPETIDOS = 17_243
CLAVES_DISTINTAS = 853_953
COLISIONES = 4  # mismo estacion+ms, distinto valor: HBase se queda con el ultimo escrito


def main():
    r = conectar()
    ok = True
    producidos = int(r.get("meta:producidos") or 0)
    print(f"Eventos producidos al stream (hora en vivo): {producidos}  (esperado 7200)")
    ok &= producidos == 7200
    print(f"XLEN {STREAM}: {r.xlen(STREAM)}")

    for g in r.xinfo_groups(STREAM):
        print(f"Grupo {g['name']:<8} consumidores={g['consumers']} procesadas(entries-read)="
              f"{g.get('entries-read')} XPENDING={g['pending']} lag={g.get('lag')}")
        ok &= g["pending"] == 0 and g.get("entries-read") == producidos

    conn = happybase.Connection("localhost", port=9090, timeout=120_000)
    for tabla in ("lecturas", "lecturas_tipo"):
        n = contar(conn, tabla)
        print(f"HBase {tabla}: {n} filas  (esperado {CLAVES_DISTINTAS})")
        if n != CLAVES_DISTINTAS:
            ok = False
            print(f"  !! faltan/sobran {CLAVES_DISTINTAS - n} filas")

    print(f"\nExplicacion: {GENERADOS} generados - {REPETIDOS} repetidos tal cual - {COLISIONES} "
          f"colisiones de clave (misma estacion y ms, valor distinto; gana el ultimo escrito) "
          f"= {GENERADOS - REPETIDOS - COLISIONES} = {CLAVES_DISTINTAS} claves distintas.")

    ultimos = list(r.scan_iter("ultimo:*"))
    print(f"\nTablero Redis: {len(ultimos)} ultimo:*, ranking:flujo={r.zcard('ranking:flujo')} "
          f"estaciones, ventana:*={len(list(r.scan_iter('ventana:*')))}, "
          f"tipos={sorted(k for k in r.scan_iter('tipo:*'))}")
    ok &= len(ultimos) == 16

    print("\nRESULTADO:", "TODO CALZA" if ok else "HAY DIFERENCIAS (revisar arriba)")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
