# Pilar 2 — Infraestructura de servicios distribuidos para una blockchain escalable

Arquitectura distribuida para una blockchain simple con Proof-of-Work usando el minero CUDA del Pilar 1.

**Tecnologías:** Python 3.11+, RabbitMQ, Redis, Docker, Kubernetes (GKE).

---

## 2.1 — Schemas de Transacción y Bloque

### Decisión de diseño: Transaction

```python
@dataclass
class Transaction:
    sender: str
    receiver: str
    amount: float
    timestamp: float
```

Cada transacción representa una transferencia de valor entre dos usuarios. Su identificador único (`tx_id`) se deriva como SHA-256 del contenido serializado, lo que garantiza determinismo e inmutabilidad.

**Validaciones:**
- `sender` y `receiver` no vacíos y distintos entre sí
- `amount` positivo

**Compromiso documentado:** No se implementan firmas digitales. En una blockchain real cada transacción estaría firmada con la clave privada del emisor para garantizar autenticación no-repudiable. Esta simplificación es aceptable porque:
1. El coordinador centralizado (NCT) valida la legitimidad de las transacciones
2. La inmutabilidad de la cadena se garantiza mediante hash chaining + PoW
3. Agregar PKI (ECDSA, firma, verificación) aumenta ~30% el código sin mejorar los conceptos centrales de la materia

### Decisión de diseño: Block

```python
@dataclass
class Block:
    index: int
    timestamp: float
    transactions: list[Transaction]
    previous_hash: str         # SHA-256 del bloque anterior
    difficulty: int            # ceros requeridos para PoW
    nonce: int                 # solución del PoW
    hash: str                  # SHA-256 de este bloque (post-minado)
```

El bloque utiliza dos valores hash con roles distintos:

| Concepto | Algoritmo | Contenido | Propósito |
|---|---|---|---|
| `fingerprint` | SHA-256 | Bloque **sin** nonce | Base string para el minero CUDA |
| `hash` | SHA-256 | Bloque **con** nonce | Identificador final del bloque, usado para encadenamiento |

**Proof-of-Work:** El minero CUDA recibe el `fingerprint` como base string y busca un nonce tal que `MD5(fingerprint + str(nonce))` comience con `difficulty` ceros. La verificación se realiza en Python con `hashlib.md5`.

**Bloque génesis (index=0):**
- `previous_hash = "0" * 64` (sin bloque anterior real)
- Sin transacciones
- Sin PoW (difficulty=0, nonce=0)

### Serialización

Ambos objetos se serializan a JSON con `sort_keys=True` para garantizar determinismo en los hashes. Esto permite almacenarlos directamente en Redis y reconstruirlos sin ambigüedad.

### Tests

Archivo: `tests/test_block.py`

Los tests cubren:
- Creación y validación del bloque génesis
- Creación de transacciones y cálculo determinista de `tx_id`
- Serialización/deserialización (roundtrip)
- Validación estructural de bloques (encadenamiento correcto e incorrecto)
- Verificación de PoW (rechazo de nonces que no cumplen la dificultad)
- Salida JSON indentada para inspección visual

Ejecución:
```bash
cd pilar2 && python3 -m unittest tests/test_block.py -v
```

---

## 2.2 — Minero CUDA como servicio

### Decisión de diseño: MinerService

El binario CUDA del Pilar 1 (`md5_range`) es un programa CLI que recibe argumentos, mina, imprime el resultado, y termina. Para integrarlo en un sistema distribuido necesitamos un wrapper que lo encapsule como un servicio llamable desde Python.

```python
svc = MinerService(binary_path="./md5_range", timeout_seconds=300)
result = svc.mine(block_fingerprint, "0000", 0, 10_000_000)
# → MinerResult(nonce=10941, hash="0000b8d7...") | None
```

### ¿Por qué subprocess y no una librería?

- El minero CUDA ya está implementado, testeado, y validado en el Pilar 1
- Reimplementar MD5 + grid-stride loop en Python con PyCUDA duplicaría esfuerzo
- El binario compilado con `nvcc` tiene overhead de inicialización de ~0.4s; el subprocess paga ese costo una vez por tarea, lo cual es aceptable para dificultades ≥ 4
- Mantiene separación clara de responsabilidades: Pilar 1 = computación GPU, Pilar 2 = distribución

### Integración con el flujo de minado

```
Coordinador (NCT)                Worker (MinerService)
     │                                  │
     │  fingerprint + target_prefix     │
     ├─────────────────────────────────▶│
     │                                  │  subprocess.run([
     │                                  │    md5_range,
     │                                  │    fingerprint,
     │                                  │    target_prefix,
     │                                  │    range_min,
     │                                  │    range_max
     │                                  │  ])
     │                                  │       │
     │                                  │       ▼ parse stdout
     │  MinerResult(nonce, hash)        │
     │◀─────────────────────────────────┤
```

La comunicación provisional es una llamada directa a `MinerService.mine()`. En el paso 2.4 esta función se conecta a una cola de RabbitMQ sin cambiar su interfaz.

### Tests

Archivo: `tests/test_miner.py`

```bash
cd pilar2 && python3 -m unittest tests/test_miner.py -v
```

Los tests cubren:
- Parseo de stdout del minero (solución encontrada, no encontrada, salida corrupta)
- Ejecución exitosa con mock de subprocess
- Timeout del subprocess
- Crash del binario CUDA
- Verificación de que los argumentos se transmiten correctamente al subprocess

---

## 2.3 — Infraestructura base: Redis + Docker Compose

### Docker Compose

Archivo: `docker-compose.yml` (raíz de pilar2/)

```yaml
services:
  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]
    volumes: [redis_data:/data]
    command: redis-server --appendonly yes
```

- **Redis 7 Alpine** — imagen liviana (~30 MB)
- **AOF persistence** (`--appendonly yes`) — los datos sobreviven reinicios del contenedor
- **Named volume** (`redis_data`) — persistencia en disco del host
- **Healthcheck** — `redis-cli ping` cada 5s para que servicios dependientes esperen antes de iniciar

En pasos siguientes se agregan RabbitMQ y los servicios Python al mismo archivo.

Levantar:
```bash
cd pilar2 && docker compose up -d
```

### Módulo chain_store

Archivo: `pilar2/storage/chain_store.py`

API de persistencia contra Redis:

| Función | Operación Redis | Descripción |
|---|---|---|
| `save_block(client, block)` | `RPUSH` | Agrega bloque al final de la cadena |
| `get_block(client, index)` | `LINDEX` | Obtiene bloque por índice (0 = génesis) |
| `get_latest_block(client)` | `LLEN` + `LINDEX` | Último bloque minado |
| `get_chain_height(client)` | `LLEN` | Cantidad de bloques en la cadena |
| `validate_chain(client)` | Itera toda la lista | Valida integridad estructural de cada bloque |

**Estructura en Redis:**
```
blockchain:blocks  →  List  →  [JSON(block0), JSON(block1), ...]
```

La cadena se modela como una Redis List — append-only, ordenada, y atómica. Cada elemento es un bloque serializado a JSON con `sort_keys=True` (determinístico).

**Conexión:** `connect()` lee `REDIS_URL` del entorno (default `redis://localhost:6379`). El import de `redis-py` es lazy — solo se carga al llamar a `connect()`, no al importar el módulo. Esto permite correr los tests unitarios sin Redis instalado.

Configuración en `storage/.env`:
```
REDIS_URL=redis://redis:6379
```

### Tests

Archivo: `tests/test_chain_store.py`

```bash
cd pilar2 && python3 -m unittest tests/test_chain_store.py -v
```

Los tests cubren:
- `save_block` → `RPUSH` con payload JSON correcto
- `get_block` → deserialización correcta desde JSON
- `get_block` con índice inexistente → `None`
- Roundtrip completo con FakeClient (lista en memoria)
- `validate_chain` sobre cadena vacía, cadena válida de 2 bloques
- `validate_chain` detecta cadena rota (`previous_hash` incorrecto)

---

## 2.4 — Mensajería asincrónica con RabbitMQ

### Topología

Un único **topic exchange** (`blockchain`) con tres bindings. Esto cumple "arquitectura híbrida de colas y tópicos":

| Queue | Binding | Patrón | Propósito |
|---|---|---|---|
| `mining_tasks` | `task.*` | Cola (work queue) | NCT publica tareas con rangos particionados → workers colaboran |
| `mining_results` | `result.*` | Cola | Workers publican soluciones → NCT consume la primera válida |
| `{anon}` por worker | `control` | Tópico (pub/sub) | NCT emite abort → todos los workers frenan simultáneamente |

**Colaboración vs competencia:** Los workers *colaboran* porque cada uno recibe un subrango distinto del espacio de nonces. No compiten por el mismo trabajo — dividen el espacio y el primero que encuentra publica.

### Mensajes

```
┌─────────────────────────────────────────────┐
│  TaskMessage (NCT → workers)                │
│  {                                          │
│    task_id, block_index, fingerprint,       │
│    difficulty (int), range_min, range_max   │
│  }                                          │
├─────────────────────────────────────────────┤
│  ResultMessage (worker → NCT)               │
│  {                                          │
│    task_id, block_index, worker_id,         │
│    nonce, hash (MD5)                        │
│  }                                          │
├─────────────────────────────────────────────┤
│  ControlMessage (NCT → workers, broadcast)  │
│  {                                          │
│    action: "abort", task_id                 │
│  }                                          │
└─────────────────────────────────────────────┘
```

### Conversión de difficulty

El NCT envía `difficulty` como entero (ej: `4`). La conversión a string de ceros (`"0000"`) ocurre exclusivamente en el worker antes de invocar al binario CUDA:

```python
target_prefix = "0" * task.difficulty
```

### Docker Compose

RabbitMQ se agregó como segundo servicio:

```yaml
rabbitmq:
  image: rabbitmq:3-management-alpine
  ports: ["5672:5672", "15672:15672"]  # AMQP + Management UI
```

El puerto `15672` expone la consola de administración web (útil para debug y para la defensa).

### Tests

Archivo: `tests/test_broker.py`

```bash
cd pilar2 && uv run python -m unittest tests/test_broker.py -v
```

Los tests cubren:
- Serialización/deserialización de TaskMessage, ResultMessage, ControlMessage
- `declare_topology`: creación de exchange + queues + bindings correctos
- `publish_tasks`: particionado de nonce space (3 workers, edge case de resto)
- `consume_result`: polling con resultado encontrado y timeout
- `broadcast_abort`: publicación de mensaje de control
- `setup_control_listener`: cola anónima + callback recibe mensaje correctamente
- `publish_result`: routing key correcta (`result.{worker_id}`)
- `start_consuming_tasks`: QoS prefetch=1 + ack manual después de procesar

---

## 2.5 — Nodo Coordinador (NCT)

### Arquitectura de threads

El NCT ejecuta tres loops concurrentes con `threading`:

| Thread | Responsabilidad |
|---|---|
| **Block loop** | Acumula transacciones → crea bloque → publica tareas de minería → espera resultado → expande rango en timeout |
| **Result loop** | Consume `mining_results` → verifica PoW → completa y persiste bloque → broadcast abort → señaliza `block_mined` |
| **Health loop** | Servidor HTTP en `:8080` con `GET /health`, `GET /status`, `POST /transaction` |

Se usa `threading` (no asyncio) porque el cuello de botella es I/O de red (RabbitMQ, Redis), no CPU.

### Sincronización

El estado compartido se maneja con `NCTState`:

- `threading.Event("block_mined")` — el result loop lo activa, el block loop espera
- `threading.Event("shutdown")` — señal de apagado para todos los threads
- `threading.Lock` — protege `current_block`/`fingerprint`/`difficulty` y el pool de transacciones

### Ciclo de vida de un bloque

```
accumulate_transactions()           ← espera BLOCK_SIZE txs o BLOCK_TIMEOUT
    │
create_block(index, txs, prev_hash)
    │
fingerprint = block.fingerprint     ← SHA-256 sin nonce
    │
publish_tasks(N ranges)
    │
block_mined.wait(timeout)           ← espera resultado del result loop
    │
    ├── mined → log, next block
    └── timeout → nonce_space × 2, republicar, volver a esperar
```

### Verificación de PoW

Dos chequeos independientes en `handle_result()`:

1. `MD5(fingerprint + nonce) == result.hash` (integridad)
2. `result.hash.startswith("0" * difficulty)` (dificultad)

El `result.hash` es el MD5 (32 chars) del PoW. El `block.hash` (SHA-256, 64 chars) se computa **después** con `block.compute_hash()` y se usa para encadenamiento.

### Filtro de resultados stale

Si llega un resultado para un bloque que ya fue minado (otro worker encontró la solución justo después del abort), se descarta comparando `result.block_index` con el bloque actual.

### Endpoints HTTP

| Método | Ruta | Respuesta |
|---|---|---|
| `GET` | `/health` | `{"status": "ok"}` |
| `GET` | `/status` | `{"chain_height": N, "pending_transactions": M, "current_block": X}` |
| `POST` | `/transaction` | `{"tx_id": "..."}` (body: `{"sender", "receiver", "amount"}`) |

Implementado con `http.server` de stdlib — sin dependencias extra.

### Configuración

Variables de entorno (archivo `nct/.env`):

| Variable | Default | Descripción |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379` | Conexión a Redis |
| `RABBITMQ_URL` | `amqp://localhost:5672/` | Conexión a RabbitMQ |
| `WORKER_COUNT` | `2` | Cantidad de workers (fijo en 2.5) |
| `BLOCK_SIZE` | `5` | Transacciones por bloque |
| `BLOCK_TIMEOUT` | `30` | Segundos máx. esperando transacciones |
| `DIFFICULTY` | `4` | Ceros requeridos en PoW |
| `NONCE_SPACE` | `1_000_000_000` | Rango inicial de búsqueda |
| `PORT` | `8080` | Puerto HTTP |

### Docker

```dockerfile
FROM python:3.12-alpine
COPY shared/ broker/ storage/ nct/ /app/
RUN pip install redis pika
ENV PYTHONPATH=/app
CMD ["python", "-m", "nct.nct"]
```

El servicio `nct` en `docker-compose.yml` depende de Redis y RabbitMQ con `condition: service_healthy`.

### Tests

Archivo: `tests/test_nct.py`

```bash
cd pilar2 && uv run python -m unittest tests/test_nct.py -v
```

Los tests cubren:
- `verify_pow_result`: nonce válido, hash incorrecto, dificultad no alcanzada
- `accumulate_transactions`: pool lleno, timeout, shutdown
- `handle_result`: rechazo stale, aceptación válida + efectos (persist, abort, señal), rechazo hash inválido
- `NCTState`: operaciones del pool, drenado con límite, set/get de bloque actual

---

## 2.6 — Worker + Keep-alive dinámico

### Worker Service

El worker es un proceso Python que se conecta a RabbitMQ, consume tareas de `mining_tasks`, ejecuta el binario CUDA vía `MinerService`, y publica resultados en `mining_results`.

**Ciclo de vida de una tarea:**

```
recibe TaskMessage de mining_tasks
    │
target_prefix = "0" * task.difficulty     ← conversión int→string
    │
self.miner.mine(fingerprint, target_prefix, range_min, range_max)
    │
    ├── abort recibido → descarta resultado, ack
    │
    └── solución encontrada → publica ResultMessage a result.{worker_id}
             └── no encontrada → log warning, ack
```

Configuración (`worker/.env`):

| Variable | Default | Descripción |
|---|---|---|
| `WORKER_ID` | `worker-{uuid}` | Identificador único |
| `RABBITMQ_URL` | `amqp://localhost:5672/` | Conexión a RabbitMQ |
| `MINER_BINARY` | `./md5_range` | Path al binario CUDA |
| `HEARTBEAT_INTERVAL` | `5` | Segundos entre heartbeats |

### Keep-alive (registro dinámico)

El worker envía heartbeats periódicos a la cola `worker_registry` con routing key `worker.heartbeat`:

```json
{"worker_id": "worker-1", "action": "heartbeat", "timestamp": 1717...}
```

El NCT consume estos mensajes en el result loop y mantiene un registro en `NCTState`:

```python
state.update_worker(data["worker_id"])          # guarda timestamp
worker_count = state.get_active_worker_count()  # cuenta workers vivos (últimos 15s)
```

Workers que no envían heartbeat en `HEARTBEAT_TIMEOUT` (15s) se consideran caídos y se excluyen del conteo.

### Cambios en el NCT

- **`block_loop`**: usa `state.get_active_worker_count()` en lugar de `config.worker_count`. Si no hay workers activos, espera y reintenta.
- **`result_loop`**: ahora consume dos colas — `mining_results` y `worker_registry`.
- **Topología**: nueva cola `worker_registry` bindeada al exchange `blockchain` con `worker.*`.

### Docker

```yaml
worker:
  build: { context: ., dockerfile: worker/Dockerfile }
  depends_on: { rabbitmq: { condition: service_healthy } }
  deploy: { replicas: 2 }
```

El worker no depende de Redis — solo habla con RabbitMQ y el binario CUDA local.

### Tests

Archivo: `tests/test_worker.py`

```bash
cd pilar2 && uv run python -m unittest tests/test_worker.py -v
```

Los tests cubren:
- `NCTState.update_worker`: registro individual y múltiple
- `NCTState.get_active_worker_count`: workers activos, expiración por timeout
- `NCTState.active_workers_snapshot`: orden alfabético
- Actualización de heartbeat resetea el timer de expiración

---

## 2.7 — Endpoints de Health/Status y Logging

### Endpoints de salud para cada servicio

Cada servicio expone un endpoint HTTP mínimo con `http.server` (stdlib, sin dependencias):

| Servicio | Puerto | Endpoints |
|---|---|---|
| NCT | `8080` | `GET /health`, `GET /status`, `POST /transaction` |
| Worker | `8081` (configurable) | `GET /health`, `GET /status` |
| Redis | `6379` | Healthcheck interno (Docker) |
| RabbitMQ | `15672` | Management UI + healthcheck interno (Docker) |

**Respuestas:**

```
GET /health → {"status": "ok", "worker_id": "worker-1", "uptime_seconds": 42.3}
GET /status → {"worker_id": "worker-1", "current_task": "...", "tasks_processed": 5, ...}
```

Los workers se identifican con `WORKER_ID` (env var) o un UUID autogenerado. Esto permite distinguir instancias en los logs y en el panel de RabbitMQ.

### Logging a disco

Ambos servicios (NCT y Worker) soportan logging dual: stdout + archivo.

```python
# Configurable via env var LOG_FILE
LOG_FILE=/var/log/nct.log     # NCT
LOG_FILE=/var/log/worker-1.log # Worker
```

Si `LOG_FILE` no está definido, el log va solo a stdout (modo desarrollo). En Docker, se define apuntando a un path dentro del contenedor.

Formato: `2026-06-03 14:22:01,234 [nct.nct] INFO Block 1 mined by worker-1 (nonce=10941)`

### Decisión de diseño: sin Prometheus/Grafana

La consigna menciona observabilidad (U5.5) pero para el alcance del TP:

- Los endpoints `/health` + `/status` cubren el monitoreo básico requerido por la consigna ("endpoint público para cada servicio que permita verificar el estado")
- Prometheus + Grafana se agregarían como servicios externos al `docker-compose.yml` (no requieren código Python)
- Los logs en disco son el mecanismo de auditoría ("registros de actividades en memoria y disco")

### Docker Compose

El compose ahora expone puertos de health para todos los servicios Python:

```
nct       → :8080  (health + transaction API)
worker-1  → :8081  (health)
worker-2  → :8082  (health)
redis     → :6379
rabbitmq  → :5672 (AMQP) + :15672 (Management UI)
```

Cada worker es un servicio independiente con su propio `WORKER_ID`, `HEALTH_PORT` y `LOG_FILE`. Esto permite verificar cada instancia individualmente.

### Tests

Archivo: `tests/test_health.py`

```bash
cd pilar2 && uv run python -m unittest tests/test_health.py -v
```

Los tests cubren:
- `GET /health` → 200 + JSON con `status: "ok"`
- `GET /status` → 200 + JSON con campos esperados
- Ruta inexistente → 404
- Default `HEALTH_PORT` = 8081

---

## 2.8 — Pool Coordinator + Fanout de tareas

### Cambio de arquitectura

El NCT ya no particiona el nonce space. Publica **un solo mensaje** con el rango completo `[0, NONCE_SPACE]`. Gracias al topic exchange con múltiples colas bindeadas a `task.mining`, cada consumidor recibe una copia:

```
NCT ──task.mining──▶ Exchange: blockchain (topic)
                       │
                       ├──▶ pool-a.inbox ──▶ Pool A particiona → 2 workers
                       ├──▶ pool-b.inbox ──▶ Pool B particiona → 3 workers
                       └──▶ worker-x.inbox ──▶ Solo miner (sin pool)
```

Esto permite que múltiples pools compitan entre sí. Cada pool divide el trabajo internamente según sus propios workers. El NCT no sabe quién resolvió el bloque — solo recibe un `ResultMessage` válido por `mining_results`.

### Nuevo componente: PoolCoordinator

Archivo: `pilar2/pool/pool.py`

| Responsabilidad | Detalle |
|---|---|
| Consumir tareas | Cola `pool.{POOL_ID}.inbox` bindeada a `task.mining` |
| Particionar | Divide el rango completo en `POOL_WORKER_COUNT` subrangos |
| Distribuir | Publica a `pool.{POOL_ID}.tasks` (cola de sus workers) |
| Recolectar | Consume de `pool.{POOL_ID}.results` |
| Verificar | Comprueba PoW localmente antes de reenviar al NCT |
| Reenviar | Publica resultado válido a `mining_results` con routing key `result.{pool_id}` |
| Abortar | Broadcast a `pool.{POOL_ID}.control` cuando un worker encuentra solución |

### Worker en modo pool

Cuando el worker tiene `POOL_ID=pumpkin`, adapta su comportamiento:

| Aspecto | Solo | Pool |
|---|---|---|
| Consume de | `worker.{id}.inbox` (bind a `task.mining`) | `pool.pumpkin.tasks` |
| Publica resultado a | `result.{id}` | `pool.pumpkin.result.{id}` |
| Heartbeat | `worker.heartbeat` | `worker.pumpkin.heartbeat` |
| Abort escucha | `control` | `control` + `pool.pumpkin.control` |

### Docker Compose

```
nct (:8080)      — coordinator, publica 1 tarea por bloque
pool-a (:8090)   — pool coordinator, particiona para 2 workers
worker-a1 (:8081) — minero del pool-a
worker-a2 (:8082) — minero del pool-a
```

### Tests

No se agregan tests nuevos en este paso (el pool es un componente de integración que requiere RabbitMQ real). Los tests existentes (61) siguen pasando — los cambios en broker, NCT y worker fueron adaptados en los tests correspondientes.
