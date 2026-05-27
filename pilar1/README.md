## Hit 2 — Hello World en CUDA

### Entorno de desarrollo

El desarrollo del Pilar 1 se realiza sobre Google Colaboratory, que provee acceso gratuito a GPUs NVIDIA con CUDA preinstalado. Se eligió esta plataforma porque la GPU local disponible (NVIDIA GTX 1060) opera con una arquitectura Pascal (sm_61) incompatible con las versiones modernas del toolkit CUDA.

La sesión de Colab asignó una GPU Tesla T4 (arquitectura Turing, sm_75) con 15360 MiB de memoria de video y un TDP de 70W. El driver instalado es la versión 580.82.07, que soporta hasta CUDA 13.0. El compilador nvcc corresponde al toolkit CUDA 12.8 (build cuda_12.8.r12.8, compilado el 21/02/2025). No existe contradicción entre ambos números: el valor que muestra nvidia-smi es la versión máxima de CUDA soportada por el driver, mientras que nvcc indica la versión del toolkit efectivamente instalado.

El flujo de trabajo adoptado consiste en escribir y versionar el código localmente con asistencia de DeepSeek como herramienta de IA, y luego ejecutarlo en Colab copiando los archivos a la sesión activa. No se presentaron problemas de configuración: el entorno de Colab tiene CUDA disponible sin pasos adicionales de instalación.

### Programa Hello World

El programa implementado lanza un kernel con N threads, donde cada thread imprime su identificador global. Esto verifica que el compilador nvcc funciona correctamente, que el runtime de CUDA puede lanzar kernels, y que la comunicación entre host (CPU) y device (GPU) opera sin errores.

La compilación se realiza con:

```bash
nvcc -arch=sm_75 hello_cuda.cu -o hello_cuda
```

La flag `-arch=sm_75` especifica la arquitectura Turing de la T4. Omitirla produce un binario funcional pero con advertencias de compatibilidad.

[Enlace a notebook de ejemplo](https://colab.research.google.com/drive/1vjTVZpT4vtE9pklU-zulxxQkkooTIhJF?usp=sharing)

---

### Hit 3 — NVIDIA CCCL y Thrust

**CCCL**

NVIDIA CCCL (CUDA Core Compute Libraries) es el repositorio unificado que consolida tres librerías previamente independientes: Thrust, CUB y libcu++. El repositorio se encuentra en desarrollo activo: al momento de redactar este informe, el último commit data de hace 2 horas. El repositorio original de Thrust (`github.com/nvidia/thrust`) fue archivado en marzo de 2024 y toda su actividad migró a CCCL.

**Thrust**

Thrust es una librería de algoritmos paralelos para CUDA con una API modelada sobre la STL de C++. Provee operaciones como ordenamiento, reducción, transformación y búsqueda que se ejecutan en GPU sin requerir que el programador escriba kernels, gestione memoria manualmente ni calcule dimensiones de grillas.

La diferencia práctica con CUDA puro es significativa. En CUDA sin Thrust, ordenar un vector implica escribir o integrar un algoritmo de sort paralelo, gestionar la memoria del device con `cudaMalloc` y `cudaMemcpy`, definir la cantidad de bloques y threads, y sincronizar la ejecución. Con Thrust, la misma operación es `thrust::sort(d_vec.begin(), d_vec.end())`. La librería resuelve todos esos detalles internamente.

La contrapartida es menor control sobre el comportamiento de bajo nivel: distribución de work entre threads, uso de shared memory, y estrategias de scheduling quedan ocultos detrás de la abstracción.

Thrust no requiere instalación adicional: forma parte del toolkit CUDA y está disponible en el entorno de Colab sin ningún paso extra.

**Ejemplo ejecutado**

El programa `pilar1/thrust/thrust_vectors.cu` genera 32 millones de enteros aleatorios en CPU, los transfiere a la GPU, los ordena con `thrust::sort`, y los copia de vuelta al host. Los primeros 5 valores del vector ordenado resultaron:

```
First 5 sorted values: 23 88 106 108 110
```

El orden ascendente confirma que el sort operó correctamente. La compilación se realizó con el Makefile ubicado en `pilar1/thrust/`.

[Enlace a notebook de ejemplo](https://colab.research.google.com/drive/11zPMZA-e8fDbuRSopGlWePngFjcQjt5K?usp=sharing)

---

### Hit 4 — Cálculo de MD5 con CUDA

El programa `pilar1/md5/md5_cuda.cu` recibe un string por argumento, calcula su MD5 en GPU y devuelve el hash en hexadecimal por consola.

La implementación se divide en dos archivos. `md5.cuh` contiene las funciones del algoritmo MD5 marcadas `__device__`, lo que permite que sean llamadas desde kernels. Implementa las cuatro funciones de ronda (F, G, H, I), la rotación de bits, y `md5_transform` que procesa un bloque de 64 bytes mutando el estado interno de cuatro palabras de 32 bits. `md5_cuda.cu` contiene el kernel y el main.

El flujo de ejecución es el siguiente. El host aplica el padding definido por RFC 1321: se agrega el byte 0x80 al final del mensaje, se rellena con ceros hasta que la longitud sea congruente a 56 mod 64, y se agregan 8 bytes con la longitud original del mensaje en bits en formato little-endian. El mensaje paddeado se copia a memoria del device. El kernel lanza un único thread que itera sobre todos los bloques de 64 bytes llamando a `md5_transform` en cada uno, y escribe el digest de 16 bytes en memoria del device. El host copia el digest de vuelta e imprime los 16 bytes en hex.

La verificación con el string "hello" produjo `5d41402abc4b2a76b9719d911017c592`, que coincide con el valor de referencia conocido del algoritmo MD5.

En este hit el kernel usa un único thread porque el objetivo es verificar la corrección de la implementación antes de paralelizarla. La ventaja de GPU no aparece hasta el Hit 5, donde miles de threads calculan hashes distintos en paralelo.

[Enlace a notebook de ejemplo](https://colab.research.google.com/drive/1r7fDcrWiaH0iF8MpN1g6A26LuKb1CnVF?usp=sharing)

---

### Hit 5 — Búsqueda de nonce por fuerza bruta con CUDA

El programa `pilar1/md5_bruteforce/md5_bruteforce.cu` recibe un string base y un prefijo hexadecimal target, y encuentra por fuerza bruta un nonce entero tal que `MD5(base + nonce)` comience con ese prefijo.

**Estrategia de paralelización**

El kernel lanza 1280 bloques de 256 threads, totalizando 327.680 threads activos. Cada thread recibe como nonce de partida su índice global y avanza de a 327.680 en cada iteración (grid-stride loop). De esta forma el espacio de búsqueda se cubre en franjas paralelas sin superposición: el thread 0 prueba los nonces 0, 327680, 655360, ...; el thread 1 prueba 1, 327681, 655361, y así sucesivamente.

Dentro de cada iteración el thread construye el string `base + nonce` en un buffer local, aplica el padding RFC 1321, calcula el MD5 llamando a `md5_transform` de `md5.cuh`, y compara los nibbles iniciales del digest contra el prefijo target.

**Mecanismo de terminación**

Se usa una flag entera en memoria global del device inicializada en 0. El primer thread que encuentra una solución válida ejecuta `atomicExch(found_flag, 1)`. Si el valor anterior era 0, ese thread escribe el nonce y el hash en memoria global y termina. Los demás threads leen la flag al inicio de cada iteración con `atomicAdd(found_flag, 0)` y retornan si ya vale 1. Esto garantiza que exactamente un resultado se escribe aunque múltiples threads encuentren soluciones simultáneamente.

**Verificación**

```
MD5(blockchain17354)      = 007b5665...   prefijo "00"       ✓
MD5(blockchain10941)      = 00009e1c...   prefijo "0000"     ✓
MD5(blockchain2144346197) = 00000000...   prefijo "00000000" ✓
```

Los tres resultados son verificables independientemente con cualquier implementación estándar de MD5.

[Enlace a notebook de ejemplo](https://colab.research.google.com/drive/1B3qJQMFga3Wey6bhy-Vn3YzIgv_7Mbkv?usp=sharing)

---

### Hit 6 — Longitudes de prefijo en CUDA Hash

Se ejecutó el programa `pilar1/md5_bruteforce/md5_bruteforce.cu` con el string base "blockchain" y prefijos de longitud creciente, midiendo el tiempo real de cada ejecución.

| Prefijo | Longitud | Nonce encontrado | Tiempo real |
|---------|----------|-----------------|-------------|
| 00 | 2 | 17.354 | 0.484s |
| 0000 | 4 | 10.941 | 0.404s |
| 00000000 | 8 | 2.144.346.197 | 2.211s |
| 000000000 | 9 | 146.403.858.385 | 127.828s |
| 0000000000 | 10 | 12.890.126.772.603 | 11274s |

Los tiempos de longitud 2 y 4 no reflejan el costo computacional real: ambos están dominados por el overhead de inicialización del contexto CUDA, que en esta plataforma ronda los 0.4 segundos y se paga una vez por proceso independientemente del trabajo realizado. El tiempo de búsqueda efectivo para esas longitudes es menor al overhead de inicialización y por lo tanto no es medible con este método.

Los datos significativos comienzan en longitud 8. La razón observada entre 8 y 9 ceros es 127.828 / 2.211 ≈ 57.8x. La razón entre 9 y 10 ceros es 11.274 / 127.828 ≈ 88.2x. El factor teórico esperado entre longitudes consecutivas es 16, dado que cada carácter hexadecimal adicional en el prefijo reduce la probabilidad de éxito de un hash arbitrario por un factor de 16, multiplicando el número esperado de intentos por el mismo valor.

Los ratios observados (57.8x y 88.2x) superan consistentemente el factor teórico. La búsqueda de nonce es un proceso probabilístico: el tiempo real depende de la posición del primer nonce válido dentro del espacio de búsqueda, que es aleatoria. El tiempo teórico es el valor esperado de esa distribución geométrica, pero la varianza es alta. Que los tres runs medibles muestren ratios superiores al teórico indica que los nonces válidos para esta combinación de base string y prefijos cayeron en posiciones desfavorables respecto al promedio.

El prefijo más largo encontrado fue de 10 ceros, en 3 horas 7 minutos y 54 segundos, con el nonce 12.890.126.772.603. La búsqueda con 16 ceros fue interrumpida luego de más de 4 minutos sin resultado: el espacio esperado es del orden de 16^16 ≈ 1.8×10^19 intentos, lo que a la tasa de hash de la T4 representa cientos de años de cómputo.

La relación entre longitud del prefijo y tiempo requerido es exponencial en base 16. Esta propiedad es la que hace al prefijo útil como parámetro de dificultad en Proof of Work: un incremento de un carácter en el prefijo produce un incremento de un orden de magnitud en el costo computacional, permitiendo ajustar la dificultad de minado con granularidad controlada.

[Enlace a notebook de ejemplo](https://colab.research.google.com/drive/1B3qJQMFga3Wey6bhy-Vn3YzIgv_7Mbkv?usp=sharing)

---

### Hit 7 — Búsqueda de nonce con rango acotado

El programa `pilar1/md5_range/md5_range.cu` extiende el bruteforce del Hit 5 con dos parámetros adicionales: los límites inferior y superior del rango de nonces a explorar. Si no existe ningún nonce válido en ese rango, el programa lo reporta explícitamente.

El cambio respecto al Hit 5 es mínimo. Cada thread calcula su nonce inicial como `min + índice_global` en lugar de solo su índice global. El grid-stride loop agrega una condición de corte: si el nonce actual supera `max`, el thread termina sin escribir resultado. El mecanismo de flag atómica es idéntico al Hit 5.

Las ejecuciones de verificación cubren los tres casos relevantes:

El rango `[0, 100]` sobre "blockchain" con prefijo "0000" no encuentra solución, ya que el nonce conocido 10941 queda fuera. El rango `[0, 999999]` encuentra correctamente el nonce 10941. El rango `[20000, 999999]` no puede encontrar el nonce 10941 porque queda por debajo del límite inferior, y encuentra en su lugar el nonce 22041, que es la siguiente solución válida en ese espacio.

La prueba con una transacción real como base string demuestra que el programa opera correctamente con el formato de datos del Pilar 2. El nonce 44003680 para el prefijo "000000" no se encontró en `[0, 999999]` pero sí en `[1000000, 100000000]`, verificando que el particionado del espacio de búsqueda funciona sin perder soluciones entre rangos contiguos.

Esta capacidad de búsqueda por rangos es el mecanismo central del Pool de Transacciones del Pilar 2: el coordinador divide el espacio completo de nonces en segmentos y asigna uno distinto a cada worker, eliminando el trabajo redundante del Hit 5 donde todos los workers compiten sobre el mismo espacio.

[Enlace a notebook de ejemplo](https://colab.research.google.com/drive/1iSs0Lfaa7qFa1lhG1oPGHyNhX7Asp37V?usp=sharing)

---
