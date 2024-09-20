// added by Arman -> 20 September 2024

#ifndef FLOOR_LG2_H_
#define FLOOR_LG2_H_

#include <stddef.h>
#include <stdint.h>

inline size_t floor_lg2(uint64_t val) { //https://cboard.cprogramming.com/cplusplus-programming/92048-efficient-way-calc-log-base-2-64-bit-unsigned-int.html
    uint64_t result = 0;
    if ( val >= 0x100000000 ) {
        result += 32;
        val >>= 32;
    }
    if ( val >= 0x10000 ) {
        result += 16;
        val >>= 16;
    }
    if ( val >= 0x100 ) {
        result += 8;
        val >>= 8;
    }
    if ( val >= 0x10 ) {
        result += 4;
        val >>= 4;
    }
    if ( val >= 0x4 ) {
        result += 2;
        val >>= 2;
    }
    if ( val >= 0x2 ) {
        result += 1;
        val >>= 1;
    }
    return result + val;
}

#endif