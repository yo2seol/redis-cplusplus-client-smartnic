#include "util.h"

char* itoa_custom(int value, char* result, int base) {
   // check that the base if valid
   if (base < 2 || base > 36) {
       *result = '\0';
       return result;
   }

   char* ptr = result, *ptr1 = result, tmp_char;
   int tmp_value;

   do {
       tmp_value = value;
       value /= base;
       *ptr++ = "zyxwvutsrqponmlkjihgfedcba9876543210123456789abcdefghijklmnopqrstuvwxyz" [35 + (tmp_value - value * base)];
   } while (value);

   // Apply negative sign
   if (tmp_value < 0) *ptr++ = '-';
   *ptr-- = '\0';
   while (ptr1 < ptr) {
       tmp_char = *ptr;
       *ptr-- = *ptr1;
       *ptr1++ = tmp_char;
   }
   return result;
}

char* ulltoa_custom(uint64_t value, char* result, int base) {
   // check that the base if valid
   if (base < 2 || base > 36) {
       *result = '\0';
       return result;
   }

   char* ptr = result, *ptr1 = result, tmp_char;
   uint64_t tmp_value;

   do {
       tmp_value = value;
       value /= base;
       *ptr++ = "zyxwvutsrqponmlkjihgfedcba9876543210123456789abcdefghijklmnopqrstuvwxyz" [35 + (tmp_value - value * base)];
   } while (value);

   // Apply negative sign
//           if (tmp_value < 0) *ptr++ = '-';
   *ptr-- = '\0';
   while (ptr1 < ptr) {
       tmp_char = *ptr;
       *ptr-- = *ptr1;
       *ptr1++ = tmp_char;
   }
   return result;
}

int ulltoa64_custom(char* dst, size_t dstlen, long long svalue) {
    char* ptr = dst, *ptr1 = dst, tmp_char;
    uint64_t tmp_value;
    size_t plen = 0;

    do {
        if (plen >= dstlen) {
            return 0; // Error. Not enough space.
        }
        tmp_value = svalue & 63;
        svalue >>= 6;
        *ptr++ = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=" [tmp_value];
        plen++;
    } while (svalue);

    *ptr-- = '\0';
    while (ptr1 < ptr) {
        tmp_char = *ptr;
        *ptr-- = *ptr1;
        *ptr1++ = tmp_char;
    }
    return (int)plen;
}
