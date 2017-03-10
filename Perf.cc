/* Copyright (c) 2011-2016 Stanford University
 * Copyright (c) 2011 Facebook
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

// This program contains a collection of low-level performance measurements
// for RAMCloud, which can be run either individually or altogether.  These
// tests measure performance in a single stand-alone process, not in a cluster
// with multiple servers.  Invoke the program like this:
//
//     Perf test1 test2 ...
//
// test1 and test2 are the names of individual performance measurements to
// run.  If no test names are provided then all of the performance tests
// are run.
//
// To add a new test:
// * Write a function that implements the test.  Use existing test functions
//   as a guideline, and be sure to generate output in the same form as
//   other tests.
// * Create a new entry for the test in the #tests table.
#if (__GNUC__ == 4 && __GNUC_MINOR__ >= 5) || (__GNUC__ > 4)
#include <atomic>
#else
#include <cstdatomic>
#endif
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>
#include <unistd.h>
#include <sys/syscall.h>
#include <stdlib.h>

#include "Cycles.h"
#include "TimeTrace.h"
#include "redisclient.h"

using namespace redis;
using RAMCloud::Cycles;

/**
 * Ask the operating system to pin the current thread to a given CPU.
 *
 * \param cpu
 *      Indicates the desired CPU and hyperthread; low order 2 bits
 *      specify CPU, next bit specifies hyperthread.
 */
void bindThreadToCpu(int cpu)
{
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    sched_setaffinity((pid_t)syscall(SYS_gettid), sizeof(set), &set);
}

/*
 * This function just discards its argument. It's used to make it
 * appear that data is used,  so that the compiler won't optimize
 * away the code we're trying to measure.
 *
 * \param value
 *      Pointer to arbitrary value; it's discarded.
 */
void discard(void* value) {
    int x = *reinterpret_cast<int*>(value);
    if (x == 0x43924776) {
        printf("Value was 0x%x\n", x);
    }
}

// The following struct and table define each performance test in terms of
// a string name and a function that implements the test.
struct TestInfo {
    const char* name;             // Name of the performance test; this is
                                  // what gets typed on the command line to
                                  // run the test.
    double (*func)();             // Function that implements the test;
                                  // returns the time (in seconds) for each
                                  // iteration of that test.
    const char *description;      // Short description of this test (not more
                                  // than about 40 characters, so the entire
                                  // test output fits on a single line).
};

/**
 * C++ version 0.4 char* style "itoa":
 * Written by Lukás Chmela
 * Released under GPLv3.
 */
char* itoa(int value, char* result, int base) {
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

/**
 * Modified version of C++ version 0.4 char* style "itoa":
 * Takes 64-bit integer instead.
 * Written by Lukás Chmela
 * Released under GPLv3.
 */
char* lltoa(long long value, char* result, int base) {
    // check that the base if valid
    if (base < 2 || base > 36) {
        *result = '\0';
        return result;
    }

    char* ptr = result, *ptr1 = result, tmp_char;
    long long tmp_value;

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

double stringlength() {
    std::string value = "7SaDL5M5gm9MnLNpWUqdlU0LMlLvyZ5cUFBEdwm5RbwvqXBEOyCD7Q5p9e229ro3bfzEulm6kwkr3HhwWTqWrY0P2D7FnIwwDN0y";
    int count = 1000000;
    uint64_t start = Cycles::rdtsc();
    char buf[200];
    size_t len = 0;
    for (int i = 0; i < count; i++) {
        len += value.length();
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

double memcpy100() {
    std::string value = "7SaDL5M5gm9MnLNpWUqdlU0LMlLvyZ5cUFBEdwm5RbwvqXBEOyCD7Q5p9e229ro3bfzEulm6kwkr3HhwWTqWrY0P2D7FnIwwDN0y";
    int count = 1000000;
    uint64_t start = Cycles::rdtsc();
    char buf[200];
    for (int i = 0; i < count; i++) {
        memcpy(buf, value.data(), value.length());
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

double itoaTest() {
    int count = 1000000;
    uint64_t start = Cycles::rdtsc();
    uint64_t lastRequestId = 99997;
    char buf[100];
    int length = 0;
    for (int i = 0; i < count; i++) {
        lltoa(++lastRequestId, buf, 10);
        length = strlen(buf);
    }
    printf("lltoa converted value %s, %" PRId64 "\n", buf, lastRequestId);
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

double sprintfItoaTest() {
    int count = 1000000;
    uint64_t start = Cycles::rdtsc();
    uint64_t lastRequestId = 99997;
    char buf[100];
    for (int i = 0; i < count; i++) {
        sprintf(buf, "%" PRId64, ++lastRequestId);
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

double lltostr() {
    int count = 1000000;
    uint64_t start = Cycles::rdtsc();
    uint64_t lastRequestId = 99997;
    std::string str;
    for (int i = 0; i < count; i++) {
        str = std::to_string(++lastRequestId);
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

double requestConst() {
    int count = 1000000;
    uint64_t start = Cycles::rdtsc();
    std::string key = "628282xxxxxxxxxxxxxxxxxxxxxxxx";
    std::string value = "7SaDL5M5gm9MnLNpWUqdlU0LMlLvyZ5cUFBEdwm5RbwvqXBEOyCD7Q5p9e229ro3bfzEulm6kwkr3HhwWTqWrY0P2D7FnIwwDN0y";
    uint64_t clientId = 581405568;
    uint64_t lastRequestId = 99997;
    std::string str;
    for (int i = 0; i < count; i++) {
        makecmd request("SET");
        request << key << value << std::to_string(clientId) << std::to_string(++lastRequestId);
        //request << key;
        str = request;
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

double requestConstFastcmd() {
    int count = 1000000;
    uint64_t start = Cycles::rdtsc();
    std::string key = "628282xxxxxxxxxxxxxxxxxxxxxxxx";
    std::string value = "7SaDL5M5gm9MnLNpWUqdlU0LMlLvyZ5cUFBEdwm5RbwvqXBEOyCD7Q5p9e229ro3bfzEulm6kwkr3HhwWTqWrY0P2D7FnIwwDN0y";
    uint64_t clientId = 581405568;
    uint64_t lastRequestId = 99997;
    char strbuf[1000];
    char* str;
    for (int i = 0; i < count; i++) {
        fastcmd request(5, "SET");
        request << key << value << clientId << ++lastRequestId;
        str = request.c_str();
    }
    uint64_t stop = Cycles::rdtsc();

    fastcmd fastreq(5, "SET");
    fastreq << key << value << clientId << ++lastRequestId;
    makecmd request("SET");
    request << key << value << std::to_string(clientId) << std::to_string(lastRequestId);
    if (strcmp(fastreq.c_str(), ((std::string)request).c_str()) != 0) {
        printf("fastcmd and makecmd output are different!\n%s\n\n%s",
                ((std::string)request).c_str(), fastreq.c_str());
    }

    return Cycles::toSeconds(stop - start)/count;
}

double requestFastConst() {
    int count = 1000000;
    uint64_t start = Cycles::rdtsc();
    std::string key = "628282xxxxxxxxxxxxxxxxxxxxxxxx";
    std::string value = "7SaDL5M5gm9MnLNpWUqdlU0LMlLvyZ5cUFBEdwm5RbwvqXBEOyCD7Q5p9e229ro3bfzEulm6kwkr3HhwWTqWrY0P2D7FnIwwDN0y";
    uint64_t clientId = 581405568;
    std::string clientIdStr = std::to_string(clientId);
    int clientIdStrLen = clientIdStr.length();
    uint64_t lastRequestId = 99997;
    for (int i = 0; i < count; i++) {
        char strbuf[1000];
        std::string requestIdStr = std::to_string(++lastRequestId);
//        sprintf(strbuf, "*5\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n"
//                "$%d\r\n%s\r\n", key.length(), key.c_str(),
//                value.length(), value.c_str(),
//                clientIdStrLen, clientIdStr.c_str());
        sprintf(strbuf, "*5\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n"
                "$%d\r\n%s\r\n$%d\r\n%s\r\n", key.length(), key.c_str(),
                value.length(), value.c_str(),
                clientIdStrLen, clientIdStr.c_str(),
                requestIdStr.length(), requestIdStr.c_str());
        //request << key;
//        str = std::string(strbuf, strlen(strbuf));
    }
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

double requestSuperFastConst() {
    int count = 1000000;
    uint64_t start = Cycles::rdtsc();
    std::string key = "628282xxxxxxxxxxxxxxxxxxxxxxxx";
    std::string value = "7SaDL5M5gm9MnLNpWUqdlU0LMlLvyZ5cUFBEdwm5RbwvqXBEOyCD7Q5p9e229ro3bfzEulm6kwkr3HhwWTqWrY0P2D7FnIwwDN0y";
    uint64_t clientId = 581405568;
    std::string clientIdStr = std::to_string(clientId);
    int clientIdStrLen = clientIdStr.length();
    uint64_t lastRequestId = 99997;
    const char* cmd = "*5\r\n$3\r\nSET\r\n$";
    const char* newline = "\r\n";
    const char* newlineDollar = "\r\n$";
    int cmdLen = strlen(cmd);
    char strbuf[1000];
    for (int i = 0; i < count; i++) {
        char* buf = strbuf;
        memcpy(buf, cmd, cmdLen);
        buf += cmdLen;

        itoa(key.length(), buf, 10);
        buf += strlen(buf);
        memcpy(buf, newline, 2);
        buf += 2;
        memcpy(buf, key.data(), key.length());
        buf += key.length();
        memcpy(buf, newlineDollar, 3);
        buf += 3;

        itoa(value.length(), buf, 10);
        buf += strlen(buf);
        memcpy(buf, newline, 2);
        buf += 2;
        memcpy(buf, value.data(), value.length());
        buf += value.length();
        memcpy(buf, newlineDollar, 3);
        buf += 3;

        itoa(clientIdStrLen, buf, 10);
        buf += strlen(buf);
        memcpy(buf, newline, 2);
        buf += 2;
        memcpy(buf, clientIdStr.data(), clientIdStrLen);
        buf += clientIdStrLen;
        memcpy(buf, newlineDollar, 3);
        buf += 3;

        ++lastRequestId;
        char* bufPosForRequesIdLen = buf;
        if (lastRequestId >= 1000000000) {
            buf += 2;
        } else {
            ++buf;
        }

        buf += 2;
        lltoa(lastRequestId, buf, 10);
        int requestIdLen = strlen(buf);
        buf += requestIdLen;
        memcpy(buf, newline, 2);
        buf[2] = 0;

        itoa(requestIdLen, bufPosForRequesIdLen, 10);
        bufPosForRequesIdLen += strlen(bufPosForRequesIdLen);
        memcpy(bufPosForRequesIdLen, newline, 2);

//        sprintf(strbuf, "*5\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n"
//                "$%d\r\n%s\r\n$%d\r\n%s\r\n", key.length(), key.c_str(),
//                value.length(), value.c_str(),
//                clientIdStrLen, clientIdStr.c_str(),
//                requestIdStr.length(), requestIdStr.c_str());
    }
    printf("SuperFast CMD constructor:\n%s", strbuf);
    uint64_t stop = Cycles::rdtsc();
    return Cycles::toSeconds(stop - start)/count;
}

TestInfo tests[] = {
    {"stringlength", stringlength,
     "Getting length from std::string::length()"},
    {"memcpy100", memcpy100,
     "100 bytes memcpy"},
    {"itoaTest", itoaTest,
     "Converting 64_bit value to string using custom lltoa"},
    {"sprintfItoaTest", sprintfItoaTest,
     "Converting 64_bit value to string using sprintf"},
    {"lltostr", lltostr,
     "Converting 64_bit value to string using to_string"},
    {"requestConst", requestConst,
     "makecmd"},
    {"fastcmd", requestConstFastcmd,
     "fastcmd"},
    {"requestFastConst", requestFastConst,
     "sprintf SET cmd"},
    {"requestSuperFastConst", requestSuperFastConst,
     "custom gen SET cmd using itoa and memcpy"},
};

/**
 * Runs a particular test and prints a one-line result message.
 *
 * \param info
 *      Describes the test to run.
 */
void runTest(TestInfo& info)
{
    double secs = info.func();
    int width = printf("%-23s ", info.name);
    if (secs < 1.0e-06) {
        width += printf("%8.2fns", 1e09*secs);
    } else if (secs < 1.0e-03) {
        width += printf("%8.2fus", 1e06*secs);
    } else if (secs < 1.0) {
        width += printf("%8.2fms", 1e03*secs);
    } else {
        width += printf("%8.2fs", secs);
    }
    printf("%*s %s\n", 26-width, "", info.description);
}

int
main(int argc, char *argv[])
{
    bindThreadToCpu(3);
    Cycles::init();
    if (argc == 1) {
        // No test names specified; run all tests.
        for (TestInfo& info : tests) {
            runTest(info);
        }
    } else {
        // Run only the tests that were specified on the command line.
        for (int i = 1; i < argc; i++) {
            bool foundTest = false;
            for (TestInfo& info : tests) {
                if (strcmp(argv[i], info.name) == 0) {
                    foundTest = true;
                    runTest(info);
                    break;
                }
            }
            if (!foundTest) {
                int width = printf("%-18s ??", argv[i]);
                printf("%*s No such test\n", 26-width, "");
            }
        }
    }
}
