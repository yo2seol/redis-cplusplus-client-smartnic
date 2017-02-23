// The MIT License (MIT)
//
// Copyright (c) 2015-2017 Simon Ninon <simon.ninon@gmail.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#include <fcntl.h>    /* For O_RDWR */
#include <unistd.h>   /* For open(), creat() */

#include <cstdint>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <sys/time.h>
#include <xmmintrin.h>
#include <cinttypes>
#include <typeinfo>

#include "redisclient.h"
#include "Cycles.h"
#include <iostream>
using RAMCloud::Cycles;

// Globals.
const char* hostIp = "192.168.1.164";
int objectSize = 100;   // Number of bytes for value payload.
int count = 1000000;    // How many repeat
int clientIndex = 0;    // ClientIndex as in RAMCloud clusterPerf.
int threads = 1;        // How many client threads per machine to run benchmark.

redis::client* client;

// Common helper functions.
uint64_t
generateRandom()
{
    // Internal scratch state used by random_r 128 is the same size as
    // initstate() uses for regular random(), see manpages for details.
    // statebuf is malloc'ed and this memory is leaked, it could be a __thread
    // buffer, but after running into linker issues with large thread local
    // storage buffers, we thought better.
    enum { STATE_BYTES = 128 };
    static __thread char* statebuf;
    // random_r's state, must be handed to each call, and seems to refer to
    // statebuf in some undocumented way.
    static __thread random_data buf;

    if (statebuf == NULL) {
        int fd = open("/dev/urandom", 00);
        if (fd < 0) {
            fprintf(stderr, "Couldn't open /dev/urandom");
            exit(1);
        }
        unsigned int seed;
        ssize_t bytesRead = read(fd, &seed, sizeof(seed));
        close(fd);
        assert(bytesRead == sizeof(seed));
        statebuf = static_cast<char*>(malloc(STATE_BYTES));
        initstate_r(seed, statebuf, STATE_BYTES, &buf);
    }

    // Each call to random returns 31 bits of randomness,
    // so we need three to get 64 bits of randomness.
    static_assert(RAND_MAX >= (1 << 31), "RAND_MAX too small");
    int32_t lo, mid, hi;
    random_r(&buf, &lo);
    random_r(&buf, &mid);
    random_r(&buf, &hi);
    uint64_t r = (((uint64_t(hi) & 0x7FFFFFFF) << 33) | // NOLINT
                  ((uint64_t(mid) & 0x7FFFFFFF) << 2)  | // NOLINT
                  (uint64_t(lo) & 0x00000003)); // NOLINT
    return r;
}

/**
 * Generate a random string.
 *
 * \param str
 *      Pointer to location where the string generated will be stored.
 * \param length
 *      Length of the string to be generated in bytes.
 */
void
genRandomString(char* str, const int length) {
    static const char alphanum[] =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    for (int i = 0; i < length; ++i) {
        str[i] = alphanum[generateRandom() % (sizeof(alphanum) - 1)];
    }
    str[length] = 0;
}

/**
 * Given an integer value, generate a key of a given length
 * that corresponds to that value.
 *
 * \param value
 *      Unique value to encapsulate in the key.
 * \param length
 *      Total number of bytes in the resulting key. Must be at least 4.
 * \param dest
 *      Memory block in which to write the key; must contain at
 *      least length bytes.
 */
void makeKey(int value, uint32_t length, char* dest)
{
    memset(dest, 'x', length);
    *(reinterpret_cast<int*>(dest)) = value;
}

// Write or overwrite randomly-chosen objects from a large table (so that there
// will be cache misses on the hash table and the object) and compute a
// cumulative distribution of write times.
void
writeDistRandom()
{
    usleep(500);
    int numKeys = 2000000;
//    int numKeys = 1000; // For development only.
    if (clientIndex != 0)
        return;

    const uint16_t keyLength = 30;

    char* key = new char[keyLength + 1];
    char* value = new char[objectSize + 1];

    // fill table first.
//    for (int i = 0; i < numKeys; ++i) {
    for (int i = 0; i < 1000; ++i) {
        makeKey(static_cast<int>(i), keyLength, key);
        genRandomString(value, objectSize);

        client->set(std::string(key, keyLength), std::string(value, objectSize));
        // TODO: use pipelining.
    }
    Cycles::init();

    Cycles::sleep(10000);

    // The following variable is used to stop the test after 10 seconds
    // if we haven't read count keys by then.
    uint64_t stop = Cycles::rdtsc() + Cycles::fromSeconds(300.0);

    // Issue the writes back-to-back, and save the times.
    std::vector<uint64_t> ticks;
    ticks.resize(count);
    for (int i = 0; i < count; i++) {
        Cycles::sleep(3); // to give master time for syncing to backups.
        // We generate the random number separately to avoid timing potential
        // cache misses on the client side.
        makeKey(static_cast<int>(generateRandom() % numKeys), keyLength, key);
        genRandomString(value, objectSize);
        // Do the benchmark
        uint64_t start = Cycles::rdtsc();
        client->set(std::string(key, keyLength), std::string(value, objectSize));
        uint64_t now = Cycles::rdtsc();
        ticks.at(i) = now - start;
        if (now >= stop) {
            count = i+1;
            break;
        }
    }

    // Output the times (several comma-separated values on each line).
    int valuesInLine = 0;
    for (int i = 0; i < count; i++) {
        if (valuesInLine >= 10) {
            valuesInLine = 0;
            printf("\n");
        }
        if (valuesInLine != 0) {
            printf(",");
        }
        double micros = Cycles::toSeconds(ticks.at(i))*1.0e06;
        printf("%.2f", micros);
        valuesInLine++;
    }
    printf("\n");

    delete(key);
    delete(value);
}

/**
  * This function parses out the arguments intended for the thread library from
  * a command line, and adjusts the values of argc and argv to eliminate the
  * arguments that the thread library consumed.
  */
void
parseOptions(int argc, char* argv[]) {
    if (argc == 0) return;

    struct OptionSpecifier {
        // The string that the user uses after `--`.
        const char* optionName;
        // The id for the option that is returned when it is recognized.
        int id;
        // Does the option take an argument?
        bool takesArgument;
    } optionSpecifiers[] = {
        {"count", 'c', true},
        {"clientIndex", 'i', true},
        {"size", 's', true},
        {"threads", 't', true}
    };
    const int UNRECOGNIZED = ~0;

    int i = 0;
    while (i < argc) {
        if (argv[i][0] != '-' || argv[i][1] != '-') {
            i++;
            continue;
        }
        const char* optionName = argv[i] + 2;
        int optionId = UNRECOGNIZED;
        const char* optionArgument = NULL;

        for (size_t k = 0;
                k < sizeof(optionSpecifiers) / sizeof(OptionSpecifier); k++) {
            const char* candidateName = optionSpecifiers[k].optionName;
            bool needsArg = optionSpecifiers[k].takesArgument;
            if (strncmp(candidateName,
                        optionName, strlen(candidateName)) == 0) {
                if (needsArg) {
                    if (i + 1 >= argc) {
                        fprintf(stderr,
                                "Missing argument to option %s!\n",
                                candidateName);
                        break;
                    }
                    optionArgument = argv[i+1];
                    optionId = optionSpecifiers[k].id;
                    argc -= 2;
                } else {
                    optionId = optionSpecifiers[k].id;
                    argc -= 1;
                }
                break;
            }
        }
        switch (optionId) {
            case 'c':
                count = atoi(optionArgument);
                break;
            case 'i':
                clientIndex = atoi(optionArgument);
                break;
            case 's':
                objectSize = atoi(optionArgument);
                break;
            case 't':
                threads = atoi(optionArgument);
                break;
            case UNRECOGNIZED:
                i++;
        }
    }
}

int
main(int argc, char *argv[]) {
    parseOptions(argc, argv);

    redis::client realClient(hostIp);
    client = &realClient;

    client->select(14);
    //client->flushdb();

    if (strncmp("writeDistRandom", argv[1], 20) == 0) {
        writeDistRandom();
    } else {
        printf("no test was selected. (Provided argv[0]: %s\n", argv[0]);
    }
}
