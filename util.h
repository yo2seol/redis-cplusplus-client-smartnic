#include <boost/cstdint.hpp>
#ifndef UTIL_H
#define UTIL_H

/**
* C++ version 0.4 char* style "itoa":
* Written by Lukás Chmela
* Released under GPLv3.
*/
char* itoa_custom(int value, char* result, int base);

/**
* Modified version of C++ version 0.4 char* style "itoa":
* Takes 64-bit integer instead.
* Written by Lukás Chmela
* Released under GPLv3.
*/
char* ulltoa_custom(uint64_t value, char* result, int base); 

/**
 * Encode unsigned 64-bit integer with 64-base ASCII encoding.
 */
int ulltoa64_custom(char* dst, size_t dstlen, long long svalue); 

#endif
