#include "splinterdb-js.h"

#ifdef _WIN32
#define ntohl _byteswap_ulong
#define htonl _byteswap_ulong
#else
#include <arpa/inet.h>
#endif

// compare items by 32-bit comparison, a is user provided and assumed to be zero terminated/padded
// which allows us to do the full 32-bit comparisons safely
int compareFast(const slice *a, const slice *b) {
    uint32_t* dataA = (uint32_t*) a->data;
    uint32_t* dataB = (uint32_t*) b->data;
    size_t remaining = b->length;
    uint32_t aVal, bVal;
    while(remaining >= 4) {
        aVal = ntohl(*dataA);
        bVal = ntohl(*dataB);
        if (aVal > bVal)
            return 1;
        if (aVal < bVal)
            return -1;
        /*diff = (int64_t) ntohl(*dataA) - (int64_t) ntohl(*dataB);
        if (diff)
            return diff;*/
        dataA++;
        dataB++;
        remaining -= 4;
    }
    if (remaining) {
        if (remaining == 1) {
            aVal = *((uint8_t*) dataA);
            bVal = *((uint8_t*) dataB);
        } else {
            aVal = ntohl(*dataA);
            bVal = remaining == 2 ? (*((uint8_t*) dataB) << 24) + (*((uint8_t*) dataB + 1) << 16) :
                ntohl(*dataB & 0x00ffffff);
        }
        if (aVal > bVal)
            return 1;
        if (aVal < bVal)
            return -1;
    }
    return a->length - b->length;
}
