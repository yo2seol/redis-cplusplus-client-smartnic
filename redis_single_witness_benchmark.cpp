/* Copyright (c) 2017 Stanford University
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

#include <fcntl.h>    /* For O_RDWR */
#include <unistd.h>   /* For open(), creat() */
#include <signal.h>

#include <stdlib.h>
#include <cstdint>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include <thread>
#include "Atomic.h"

#include <sys/time.h>
#include <xmmintrin.h>
#include <cinttypes>
#include <typeinfo>

#include "rediswitnessclient.h"
#include "Cycles.h"
#include <iostream>
#include <atomic>
using RAMCloud::Cycles;

// Globals.
const char* hostIp = "10.10.102.101";
int objectSize = 100;   // Number of bytes for value payload.
int count = 1000000;    // How many repeat
int clientIndex = 0;    // ClientIndex as in RAMCloud clusterPerf.
int threads = 50;        // How many client threads per machine to run benchmark.
                        // used for throughput benchmark only.
int numWitness = 0;// send requests to witness as well as master.

bool gc = false;

redis::witness_client* client;

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
    std::string str = std::to_string(value);
    memcpy(dest, str.c_str(), str.size());
}

PerfUtils::Atomic<int64_t> writeThroughputTotalWrites(0);

void
writeThroughputRunner(int tid, bool gc) {
    int numKeys = 2000000;
    const uint16_t keyLength = 30;
    char* key = new char[keyLength + 1];
    char* value = new char[objectSize + 1];

    redis::witness_client* clientPtr = client;
    if (tid != 0) {
        clientPtr = new redis::witness_client(hostIp);
    }

    //printf("New thread created! tid: %d clienId Assigned: %" PRIu64 ",
    //rpcId: %" PRIu64 "\n", tid, multiClient[tid]->clientId, multiClient[tid]->lastRequestId);
    uint64_t writeCount = 0;
    std::vector<std::string> gcKeys;
    std::vector<uint64_t> gcReqIds;
    while(true) {
        makeKey(static_cast<int>(generateRandom() % numKeys), keyLength, key);
        genRandomString(value, objectSize);
        std::string keyStr = std::string(key, keyLength);
        clientPtr->witnessset(keyStr, std::string(value, objectSize));
        writeCount++;
        gcKeys.push_back(keyStr);
        gcReqIds.push_back(clientPtr->lastRequestId);
        if (writeCount % 1000 == 0) {
            writeThroughputTotalWrites.add(1000);
            if (gc) {
                for (uint32_t ridx = 0; ridx < gcKeys.size(); ridx++) {
                    clientPtr->witnessgc(gcKeys.at(ridx), gcReqIds.at(ridx));
                }
                gcKeys.clear();
                gcReqIds.clear();
            }
        }
    }
}

void
writeThroughput(bool gc)
{
    Cycles::init();
    // Add startup delay.
    int delayInSec = 3;
    std::vector<std::thread> stdthreads;
    int lastWriteTotal = 0;
    uint64_t lastPrintTime = Cycles::rdtsc();
    for (int tid = 0; tid < threads; ++tid) {
        stdthreads.push_back(std::thread(&writeThroughputRunner, tid, gc));
        Cycles::sleep(delayInSec*1000000);
        uint64_t currentTime = Cycles::rdtsc();
        printf("Started thread %d. Throughput: %7.2f kops/sec\n", tid,
                (writeThroughputTotalWrites - lastWriteTotal) * 1e3 /
                Cycles::toMicroseconds(currentTime - lastPrintTime));
        lastPrintTime = currentTime;
        lastWriteTotal = writeThroughputTotalWrites;
    }
    printf("All threads were started.\n");
    while(true) {
        Cycles::sleep(delayInSec*1000000);

        uint64_t currentTime = Cycles::rdtsc();
        printf("Total threads: %d. Throughput: %7.2f kops/sec\n", threads,
                (writeThroughputTotalWrites - lastWriteTotal) * 1e3 /
                Cycles::toMicroseconds(currentTime - lastPrintTime));
        lastPrintTime = currentTime;
        lastWriteTotal = writeThroughputTotalWrites;
    }
}

// Write or overwrite randomly-chosen objects from a large table (so that there
// will be cache misses on the hash table and the object) and compute a
// cumulative distribution of write times.
void
writeDistRandom()
{
    usleep(500);
    int numKeys = 2000000;
    if (clientIndex != 0)
        return;

    const uint16_t keyLength = 30;

    char* key = new char[keyLength + 1];
    char* value = new char[objectSize + 1];

    for (int i = 0; i < 10000; ++i) {
        makeKey(static_cast<int>(i), keyLength, key);
        genRandomString(value, objectSize);
        client->witnessset(std::string(key, keyLength), std::string(value, objectSize));
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
        client->witnessset(std::string(key, keyLength), std::string(value, objectSize));
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

    PerfUtils::TimeTrace::print();

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
        {"threads", 't', true},
        {"witness", 'w', true}
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
                    //argc -= 2;
                    i += 2;
                } else {
                    optionId = optionSpecifiers[k].id;
                    //argc -= 1;
                    i++;
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
            case 'w':
                numWitness = atoi(optionArgument);
                break;
            case UNRECOGNIZED:
                i++;
        }
    }
}

/* Catch Signal Handler functio */
void signal_callback_handler(int signum) {
    ((void) signum);
}

int
main(int argc, char *argv[]) {
    srand(std::time(NULL));
    /* Catch Signal Handler SIGPIPE */
    signal(SIGPIPE, signal_callback_handler);

    parseOptions(argc, argv);

    redis::witness_client realClient(hostIp);
    client = &realClient;

    if (strncmp("writeDistRandom", argv[1], 20) == 0) {
        writeDistRandom();
    } else if (strncmp("writeThroughput", argv[1], 20) == 0) {
        writeThroughput(gc);
    } else {
        printf("no test was selected. (Provided argv[0]: %s\n", argv[0]);
    }
}
