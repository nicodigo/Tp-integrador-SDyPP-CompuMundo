#pragma once
#include <cstdint>

// MD5 round functions
__device__ inline uint32_t md5_F(uint32_t x, uint32_t y, uint32_t z) {
    return (x & y) | (~x & z);
}
__device__ inline uint32_t md5_G(uint32_t x, uint32_t y, uint32_t z) {
    return (x & z) | (y & ~z);
}
__device__ inline uint32_t md5_H(uint32_t x, uint32_t y, uint32_t z) {
    return x ^ y ^ z;
}
__device__ inline uint32_t md5_I(uint32_t x, uint32_t y, uint32_t z) {
    return y ^ (x | ~z);
}

__device__ inline uint32_t md5_rotl(uint32_t x, uint32_t n) {
    return (x << n) | (x >> (32 - n));
}

// Process one 64-byte block. state is mutated in-place.
__device__ void md5_transform(uint32_t state[4], const uint8_t block[64]) {
    // Per-round additive constants: K[i] = floor(2^32 * |sin(i+1)|)
    const uint32_t K[64] = {
        0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee,
        0xf57c0faf, 0x4787c62a, 0xa8304613, 0xfd469501,
        0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be,
        0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821,
        0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa,
        0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8,
        0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
        0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a,
        0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c,
        0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70,
        0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x04881d05,
        0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
        0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039,
        0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
        0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1,
        0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391
    };

    // Per-step rotation amounts (4 unique values per round, cycled)
    const uint32_t S[64] = {
        7, 12, 17, 22,  7, 12, 17, 22,  7, 12, 17, 22,  7, 12, 17, 22,
        5,  9, 14, 20,  5,  9, 14, 20,  5,  9, 14, 20,  5,  9, 14, 20,
        4, 11, 16, 23,  4, 11, 16, 23,  4, 11, 16, 23,  4, 11, 16, 23,
        6, 10, 15, 21,  6, 10, 15, 21,  6, 10, 15, 21,  6, 10, 15, 21
    };

    uint32_t a = state[0], b = state[1], c = state[2], d = state[3];

    // Decode block into 16 little-endian 32-bit words
    uint32_t M[16];
    for (int i = 0; i < 16; i++) {
        M[i] = (uint32_t)block[i*4]
             | ((uint32_t)block[i*4 + 1] << 8)
             | ((uint32_t)block[i*4 + 2] << 16)
             | ((uint32_t)block[i*4 + 3] << 24);
    }

    for (int i = 0; i < 64; i++) {
        uint32_t f, g;
        if (i < 16) {
            f = md5_F(b, c, d);
            g = i;
        } else if (i < 32) {
            f = md5_G(b, c, d);
            g = (5 * i + 1) % 16;
        } else if (i < 48) {
            f = md5_H(b, c, d);
            g = (3 * i + 5) % 16;
        } else {
            f = md5_I(b, c, d);
            g = (7 * i) % 16;
        }

        uint32_t tmp = d;
        d = c;
        c = b;
        b = b + md5_rotl(a + f + K[i] + M[g], S[i]);
        a = tmp;
    }

    state[0] += a;
    state[1] += b;
    state[2] += c;
    state[3] += d;
}
