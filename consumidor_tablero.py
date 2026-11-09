"""Parte B.2 - Grupo 'tablero': mantiene en Redis el estado de AHORA.

  ultimo:<estacion>   hash  valor, ts (medicion, ms), tipo
  ranking:flujo       zset  estacion -> ultimo valor (solo flujo_vehicular)
  ventana:<estacion>  zset  score = ts medicion (ms), member = "ts:valor"; ultimos 10 min (solo flujo)
  tipo:<tipo>         set   estaciones de cada tipo (catalogo)

Ultimo POR MEDICION: un script Lua (atomico) compara el ts de medicion con el guardado y solo
actualiza si el evento no es mas antiguo. Una lectura tardia no hace retroceder el tablero.
"""
import argparse

from comun import GRUPO_TABLERO, STREAM, VENTANA_MS, conectar, crear_grupo, ts_ms

LUA = """
local ts = tonumber(ARGV[3])
local cur = redis.call('HGET', KEYS[1], 'ts')
if (not cur) or tonumber(cur) <= ts then
  redis.call('HSET', KEYS[1], 'valor', ARGV[2], 'ts', ARGV[3], 'tipo', ARGV[4])
  if ARGV[4] == 'flujo_vehicular' then redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1]) end
end
redis.call('SADD', KEYS[4], ARGV[1])
if ARGV[4] == 'flujo_vehicular' then
  redis.call('ZADD', KEYS[3], ts, ARGV[3] .. ':' .. ARGV[2])
  local top = redis.call('ZREVRANGE', KEYS[3], 0, 0, 'WITHSCORES')
  redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', '(' .. (tonumber(top[2]) - %d))
end
return 1
""" % VENTANA_MS

LOTE = 500


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nombre", default="tab-1")
    a = ap.parse_args()

    r = conectar()
    crear_grupo(r, GRUPO_TABLERO)
    script = r.register_script(LUA)

    total = vacios = 0
    while True:
        resp = r.xreadgroup(GRUPO_TABLERO, a.nombre, {STREAM: ">"}, count=LOTE, block=1000)
        if not resp:
            vacios += 1
            if vacios >= 3 and r.exists("meta:producidos"):
                break
            continue
        vacios = 0
        lote = resp[0][1]
        pipe = r.pipeline()
        for _id, ev in lote:
            est, tipo = ev["estacion_id"], ev["tipo_sensor"]
            script(keys=[f"ultimo:{est}", "ranking:flujo", f"ventana:{est}", f"tipo:{tipo}"],
                   args=[est, ev["valor"], ts_ms(ev["timestamp"]), tipo], client=pipe)
        pipe.execute()
        r.xack(STREAM, GRUPO_TABLERO, *[i for i, _ in lote])
        total += len(lote)
    print(f"{a.nombre}: {total} entradas procesadas", flush=True)


if __name__ == "__main__":
    main()
