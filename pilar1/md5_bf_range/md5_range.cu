#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cstdint>
#include <cuda_runtime.h>
#include "md5.cuh"

// ---- Grid: tuned for Tesla T4 (sm_75, 40 SMs, 1024 max threads/SM) ----
#define THREADS_PER_BLOCK 256
#define BLOCKS            1280   // 40 SMs × 32 ≈ high occupancy + pipeline depth

// Padded-message buffer per thread (handles base strings up to ~240 chars)
#define MAX_PADDED 320

// ---------------------------------------------------------------------------
// Device helpers
// ---------------------------------------------------------------------------

// Convert uint64_t → decimal string (no null terminator). Returns length.
__device__ int uint64_to_dec(uint64_t n, char* buf) {
    if (n == 0) {
        buf[0] = '0';
        return 1;
    }
    int len = 0;
    char tmp[21];                     // max 20 digits for 2^64-1
    while (n > 0) {
        tmp[len++] = '0' + (char)(n % 10);
        n /= 10;
    }
    // Reverse into buf
    for (int i = 0; i < len; i++) {
        buf[i] = tmp[len - 1 - i];
    }
    return len;
}

// ---------------------------------------------------------------------------
// Range-limited brute-force kernel
// ---------------------------------------------------------------------------
__global__ void bruteForceRange(
    const char*  base,               // base string (device global memory)
    int          base_len,           // strlen(base)
    const char*  target,             // target hex prefix, e.g. "0000"
    int          target_len,         // strlen(target)
    uint64_t     range_min,          // inclusive lower bound
    uint64_t     range_max,          // inclusive upper bound
    int*         found_flag,         // atomic: 0 = searching, 1 = done
    uint64_t*    found_nonce,        // result nonce
    uint8_t*     found_hash          // result MD5 digest (16 bytes)
) {
    uint64_t idx    = (uint64_t)blockIdx.x * blockDim.x + threadIdx.x;
    uint64_t stride = (uint64_t)gridDim.x   * blockDim.x;

    // Each thread starts at range_min + its global index
    for (uint64_t nonce = range_min + idx; nonce <= range_max; nonce += stride) {

        // ---- Early exit if another thread already won ----
        if (atomicAdd(found_flag, 0) != 0) return;

        // ---- Build input: base + decimal_nonce ----
        uint8_t msg[MAX_PADDED];
        int msg_len = 0;

        for (int i = 0; i < base_len; i++) {
            msg[msg_len++] = (uint8_t)base[i];
        }

        char nonce_str[21];
        int nonce_len = uint64_to_dec(nonce, nonce_str);
        for (int i = 0; i < nonce_len; i++) {
            msg[msg_len++] = (uint8_t)nonce_str[i];
        }

        int unpadded_len = msg_len;     // save for bit-length field

        // ---- MD5 padding (RFC 1321 §3.1) ----
        msg[msg_len++] = 0x80;          // append bit '1'

        int rem = msg_len % 64;
        if (rem > 56) {
            int zeros = (64 - rem) + 56;
            for (int i = 0; i < zeros; i++) msg[msg_len++] = 0;
        } else {
            int zeros = 56 - rem;
            for (int i = 0; i < zeros; i++) msg[msg_len++] = 0;
        }

        // Append original message bit-length as little-endian uint64
        uint64_t bit_len = (uint64_t)unpadded_len * 8;
        for (int i = 0; i < 8; i++) {
            msg[msg_len++] = (uint8_t)((bit_len >> (i * 8)) & 0xFF);
        }

        int num_blocks = msg_len / 64;

        // ---- MD5 digest ----
        uint32_t state[4] = {
            0x67452301,   // A
            0xEFCDAB89,   // B
            0x98BADCFE,   // C
            0x10325476    // D
        };

        for (int i = 0; i < num_blocks; i++) {
            md5_transform(state, msg + i * 64);
        }

        // Unpack state → digest (little-endian byte order per word)
        uint8_t digest[16];
        for (int i = 0; i < 4; i++) {
            digest[i*4]     = (uint8_t)( state[i]        & 0xFF);
            digest[i*4 + 1] = (uint8_t)((state[i] >>  8) & 0xFF);
            digest[i*4 + 2] = (uint8_t)((state[i] >> 16) & 0xFF);
            digest[i*4 + 3] = (uint8_t)((state[i] >> 24) & 0xFF);
        }

        // ---- Hex-prefix check ----
        bool match = true;
        for (int i = 0; i < target_len; i++) {
            uint8_t byte_val = digest[i / 2];
            uint8_t nibble   = (i % 2 == 0) ? (byte_val >> 4)
                                            : (byte_val & 0x0F);
            char hex_char    = (char)(nibble < 10 ? '0' + nibble
                                                  : 'a' + nibble - 10);
            if (hex_char != target[i]) {
                match = false;
                break;
            }
        }

        if (match) {
            // Claim the solution atomically
            if (atomicExch(found_flag, 1) == 0) {
                *found_nonce = nonce;
                for (int i = 0; i < 16; i++) {
                    found_hash[i] = digest[i];
                }
            }
            return;   // winner or not — stop this thread
        }
    }
}

// ---------------------------------------------------------------------------
// Host
// ---------------------------------------------------------------------------
int main(int argc, char** argv) {
    if (argc != 5) {
        fprintf(stderr, "Usage: %s <base_string> <target_prefix> <min> <max>\n", argv[0]);
        return 1;
    }

    const char* base   = argv[1];
    const char* target = argv[2];
    int base_len       = (int)strlen(base);
    int target_len     = (int)strlen(target);

    // Parse range
    char* end;
    uint64_t range_min = strtoull(argv[3], &end, 10);
    if (*end != '\0') {
        fprintf(stderr, "Invalid min value: %s\n", argv[3]);
        return 1;
    }
    uint64_t range_max = strtoull(argv[4], &end, 10);
    if (*end != '\0') {
        fprintf(stderr, "Invalid max value: %s\n", argv[4]);
        return 1;
    }
    if (range_min > range_max) {
        fprintf(stderr, "Error: min (%lu) > max (%lu)\n",
                (unsigned long)range_min, (unsigned long)range_max);
        return 1;
    }

    // Validate target prefix (lowercase hex only)
    for (int i = 0; i < target_len; i++) {
        char c = target[i];
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
            fprintf(stderr, "Target must be lowercase hex [0-9a-f]\n");
            return 1;
        }
    }

    // Safety: padded message must fit in kernel buffer
    if (base_len > MAX_PADDED - 100) {
        fprintf(stderr, "Base string too long (max %d chars)\n", MAX_PADDED - 100);
        return 1;
    }

    // ---- GPU allocations ----
    char     *d_base, *d_target;
    int      *d_found_flag;
    uint64_t *d_found_nonce;
    uint8_t  *d_found_hash;

    cudaMalloc(&d_base,        base_len   + 1);
    cudaMalloc(&d_target,      target_len + 1);
    cudaMalloc(&d_found_flag,  sizeof(int));
    cudaMalloc(&d_found_nonce, sizeof(uint64_t));
    cudaMalloc(&d_found_hash,  16);

    cudaMemcpy(d_base,   base,   base_len   + 1, cudaMemcpyHostToDevice);
    cudaMemcpy(d_target, target, target_len + 1, cudaMemcpyHostToDevice);
    cudaMemset(d_found_flag, 0, sizeof(int));

    // ---- Launch ----
    printf("Base:   \"%s\"\n", base);
    printf("Target: \"%s\"\n", target);
    printf("Range:  [%lu, %lu]  (%lu nonces)\n",
           (unsigned long)range_min, (unsigned long)range_max,
           (unsigned long)(range_max - range_min + 1));
    printf("Grid:   %d blocks × %d threads  (stride = %lu)\n",
           BLOCKS, THREADS_PER_BLOCK,
           (unsigned long)BLOCKS * THREADS_PER_BLOCK);

    bruteForceRange<<<BLOCKS, THREADS_PER_BLOCK>>>(
        d_base, base_len,
        d_target, target_len,
        range_min, range_max,
        d_found_flag, d_found_nonce, d_found_hash
    );
    cudaDeviceSynchronize();

    // ---- Retrieve result ----
    int      found_flag;
    uint64_t found_nonce;
    uint8_t  found_hash[16];

    cudaMemcpy(&found_flag,  d_found_flag,  sizeof(int),      cudaMemcpyDeviceToHost);
    cudaMemcpy(&found_nonce, d_found_nonce, sizeof(uint64_t), cudaMemcpyDeviceToHost);
    cudaMemcpy(found_hash,   d_found_hash,  16,               cudaMemcpyDeviceToHost);

    if (found_flag) {
        printf("\nFound!\n");
        printf("  nonce = %lu\n", (unsigned long)found_nonce);
        printf("  MD5(%s%lu) = ", base, (unsigned long)found_nonce);
        for (int i = 0; i < 16; i++) printf("%02x", found_hash[i]);
        printf("\n");
    } else {
        printf("\nNo solution found in range [%lu, %lu]\n",
               (unsigned long)range_min, (unsigned long)range_max);
    }

    // ---- Cleanup ----
    cudaFree(d_base);
    cudaFree(d_target);
    cudaFree(d_found_flag);
    cudaFree(d_found_nonce);
    cudaFree(d_found_hash);

    return found_flag ? 0 : 1;
}
