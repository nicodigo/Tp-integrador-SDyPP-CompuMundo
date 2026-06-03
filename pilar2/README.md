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
