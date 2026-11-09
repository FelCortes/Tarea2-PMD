# Tarea 2 · Redis + HBase: del stream a la historia

Procesamiento Masivo de Datos (USM 2026-2). Pipeline completo: los eventos de 16 estaciones del Gran Concepción
entran a un stream de Redis; el grupo `ingesta` los baja a HBase y el grupo `tablero` mantiene el estado de
"ahora" en Redis. Las preguntas P1–P6 y los experimentos D.1/D.2 se responden desde el motor que corresponde.

## 1. Cómo reproducir desde cero

**Requisitos:** Docker Desktop (≈4 GB de RAM asignados, ≈3 GB de disco) y Python 3.10+. No se usa el VPS del curso.

```bash
./run.sh        # Linux / Mac
run.bat         # Windows (CMD)
```

El script hace, en orden: crea el `venv` e instala `requirements.txt` → `docker compose up -d --build --wait`
(construye la imagen de HBase la primera vez, ≈340 MB, y espera a los healthchecks de Redis y HBase) →
`carga_historica.py` → `main_pipeline.py` → `verificar.py`, que debe terminar con `TODO CALZA`.

**Cuánto tarda** (PC de desarrollo, ver §5): build de HBase una sola vez (unos minutos, depende de la red);
carga histórica 64–94 s según la corrida (864.000 eventos, 9.200–13.500 eventos/s); camino en vivo ≈187 s (la hora
en vivo a 20× dura 180 s por diseño); verificación unos segundos. Total ≈ 5–6 minutos con la imagen ya construida.

Los datos **no** se versionan: se generan con `generador.py` (semilla 2026, `t0 = 2026-11-02T00:00-03:00`) a
través de `datos.py` (primeros 864.000 eventos = histórico; los 7.200 siguientes = hora en vivo).

### Preguntas y experimentos (cada script imprime resultado, tiempo —mediana de 5 corridas descartando la primera— y el equipo)

| Script | Qué responde | Resultados guardados |
|---|---|---|
| `p1_p3.py` | P1 ahora (Redis vs HBase), P2 ventana 10 min, P3 rango | `resultados_p1_p3.txt` |
| `p4.py` | P4 dos patrones de acceso + costo de la segunda tabla | `resultados_p4.txt` |
| `p5.py` | P5 hotspotting y salting (JMX de regiones) | `resultados_p5.txt` |
| `p6.py` | P6 pregunta propia: congestión anómala | `resultados_p6.txt` |
| `d1.py` | D.1 el consumidor muere entre put y XACK | `resultados_d1.txt` |
| `d2.py` | D.2 Redis se reinicia (AOF vs sin persistencia; usa `docker-compose.d2.yml`) | `resultados_d2.txt` |

**Orden:** `p1_p3`, `p4` y `p6` leen el tablero de Redis, así que van **después** de `main_pipeline.py`.
`d1.py` y `d2.py` hacen `FLUSHALL` en Redis (son experimentos destructivos): después de ellos hay que volver a correr
`python main_pipeline.py` para reconstruir el tablero antes de P1–P4/P6.

## 2. Archivos

`docker-compose.yml` (redis + hbase, volúmenes con nombre, healthchecks) · `Dockerfile` (HBase 2.5 standalone + Thrift) ·
`comun.py` (constantes) · `datos.py`/`generador.py` (datos con semilla) · `carga_historica.py` · `productor.py` ·
`consumidor_ingesta.py` · `consumidor_tablero.py` · `main_pipeline.py` · `verificar.py` · `medicion.py` · `p*.py`, `d*.py`.

## 3. Decisiones de diseño

### Row keys y regiones (HBase)

| Tabla | Row key | Rápido | Lento | Regiones (SPLITS) |
|---|---|---|---|---|
| `lecturas` | `estacion_id#timestamp` | lecturas de una estación en un rango de tiempo (1 scan) | un tipo o todas las estaciones en un rango (hay que hacer un scan por estación) | 7: cortes en `CHI-001, COR-001, HUA-001, PEN-001, SPP-001, TAL-001` (una región por comuna) |
| `lecturas_tipo` | `tipo_sensor#timestamp#estacion_id` | un tipo de sensor en un rango de tiempo (1 scan) | el historial de una sola estación | 3: cortes en `flujo_vehicular, ocupacion_estacionamiento` (una región por tipo) |

* **Idempotencia:** la clave sale de la **medición** (estación + timestamp de medición), nunca de la llegada. Un evento
  repetido o tardío escribe en la misma fila y la sobreescribe; reprocesar un lote no duplica filas (D.1). Por eso la
  cuenta final es 853.953 filas (claves distintas), no 871.200 eventos.
* **Timestamps** ISO con milisegundos y el mismo offset (`-03:00`): ordenan correctamente como bytes.
* **Familia `d`, `VERSIONS => 5`:** para poder mostrar en D.1 las versiones que deja un reproceso.
* Los 4 pares con misma estación y mismo milisegundo pero distinto valor colisionan; HBase se queda con el último escrito.
* **Cortes:** con las tablas pre-divididas la carga no parte con una sola región. El límite es que hay solo 7 comunas
  y 3 tipos: en `lecturas_tipo` la clave empieza por el tipo y, dentro de cada tipo, crece con el tiempo (ver P5).

### Redis

* **Stream** `sensores:t2`, `XADD` con `MAXLEN ~ 20.000`: a 40 ev/s (20×) son ≈8 minutos de atraso tolerado por un
  consumidor. La hora en vivo son 7.200 entradas, así que aquí nada se recorta sin leerse. En un sistema real, una
  entrada recortada que ningún grupo leyó **se pierde para siempre**: el MAXLEN debe superar el peor atraso esperado.
* **Dos grupos independientes** (`ingesta`, `tablero`) sobre el mismo stream; cada uno ve todo.
* **`ingesta`:** lee en lotes de 500, hace `put` en las dos tablas de HBase y **después** `XACK`. Si muere entre ambos,
  el lote queda en XPENDING y otro consumidor lo reclama con `XAUTOCLAIM`.
* **`tablero`:** un script Lua atómico por evento. **Último por medición:** compara el `ts` de medición con el del hash
  `ultimo:<estacion>` y solo actualiza si el evento no es más antiguo; una lectura tardía no hace retroceder el tablero.
  En empate de `ts` gana el último en llegar (igual que HBase, "last write wins"). Estructuras: `ultimo:<estacion>`
  (hash valor/ts/tipo), `ranking:flujo` (zset), `ventana:<estacion>` (zset, score = ts de medición en ms, member
  `ts:valor` para que un repetido no cuente dos veces, recortado a 10 min respecto de la medición más reciente de la
  estación), `tipo:<tipo>` (set, catálogo).
* **Persistencia: AOF con `appendfsync everysec` y volumen** (`--appendonly yes --appendfsync everysec`). Redis es el
  buffer del pipeline en vivo: con RDB (snapshots periódicos) una caída pierde minutos de eventos que aún no llegaron
  a HBase. AOF registra cada escritura y, sincronizando cada segundo, el peor caso por caída del sistema es ≈1 segundo
  de datos (≈2 lecturas a ritmo real), sin pagar el costo de `always` en cada `XADD`. Evidencia en D.2.

## 4. Qué no funcionó (y cómo se resolvió)

* El `Dockerfile` heredado tenía las etiquetas del `hbase-site.xml` como `<n>` en vez de `<name>` (HBase las ignoraba) y el
  compose montaba el volumen en `/hbase-data` aunque HBase escribe en `/data`: el volumen no guardaba nada. Corregido.
* `happybase` no permite `SPLITS` al crear tablas: las tablas se crean con `hbase shell` vía `docker exec`.
* La primera ejecución de P4 dio un catálogo vacío porque `d1.py` hace `FLUSHALL` y el tablero no se había reconstruido
  (de ahí el orden indicado en §1).
* En P5 el total de escrituras por tabla fue 7.048 y no 7.200: `happybase` junta en un mismo lote los puts a la misma fila,
  y los repetidos llegan pegados al original.
* Las mediciones varían entre corridas (la carga histórica tardó 64 s y 94 s; P4(b) 58 ms y 140 ms): lo que se compara son
  órdenes de magnitud, no milisegundos.
* En D.2, `docker kill` con AOF no perdió eventos en 3 intentos. No prueba que `everysec` no pierda nada: un SIGKILL
  del proceso no descarta lo que Redis ya entregó al sistema operativo; la ventana de ≈1 s aplica a una caída de la
  máquina o corte de energía, que no se puede simular con Docker.

## 5. Alcance de las mediciones

Equipo: Windows 10, AMD64 (Family 23 Model 96), 8 hilos, Python 3.13, Redis 7 y HBase 2.5.15 **standalone** en Docker.
HBase standalone en un PC no es un cluster: los tiempos sirven para comparar órdenes de magnitud y la forma en que
escala cada consulta, pero no predicen latencias de un cluster. En particular, con un solo RegionServer el
hotspotting de P5 se ve en los contadores de escrituras por región y no en la latencia.
