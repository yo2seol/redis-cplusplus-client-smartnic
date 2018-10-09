# Redis C++ Client Library Makefile

VPATH = tests

CFLAGS?= -std=c++11 -pedantic -O3 -Wall -DNEBUG -W -Wno-unused-parameter -I/home/p4/Desktop/witnesscmd -L/home/p4/Desktop/witnesscmd -lwitnesscmd
#CFLAGS?= -std=c++11 -pedantic -O3 -W -DDEBUG -g
CC = g++

CLIENTOBJS = anet.o util.o udp.o
LIBNAME = libredisclient.a

#TESTAPP = test_client
TESTAPP = redis_benchmark
#TESTAPPOBJS = Cycles.o redis_benchmark.o UnsyncedRpcTracker.o MurmurHash3.o TimeTrace.o test_lists.o test_sets.o test_zsets.o test_hashes.o test_cluster.o test_distributed_strings.o test_distributed_ints.o test_distributed_mutexes.o test_generic.o benchmark.o functions.o
TESTAPPOBJS = Cycles.o redis_benchmark.o UnsyncedRpcTracker.o MurmurHash3.o TimeTrace.o functions.o
TESTAPPLIBS = $(LIBNAME) -lstdc++ -lboost_system -lboost_thread -lpthread -lwitnesscmd

all: $(LIBNAME) $(TESTAPP)

$(LIBNAME): $(CLIENTOBJS)
	ar rcs $(LIBNAME) $(CLIENTOBJS)

.c.o:
	$(CC) -c $(CFLAGS) $<

.cc.o:
	$(CC) -c $(CFLAGS) $<

.cpp.o:
	$(CC) -c $(CFLAGS) $<

$(TESTAPP): $(LIBNAME) $(TESTAPPOBJS)
	$(CC) -o $(TESTAPP) $(TESTAPPOBJS) $(TESTAPPLIBS) -I/home/p4/Desktop/witnesscmd -L/home/p4/Desktop/witnesscmd

test: $(TESTAPP)
	@./test_client

Perf: Perf.cc Cycles.o
	$(CC) -o $@ $(CFLAGS) $^ $(TESTAPPLIBS)

check: test

clean:
	rm -rf $(LIBNAME) *.o $(TESTAPP)

dep:
	$(CC) -MM *.c *.cpp

log:
	git log '--pretty=format:%ad %s' --date=short > Changelog

anet.o:                     anet.c fmacros.h anet.h
util.o:						util.c util.h
udp.o:						udp.c udp.h
redis_benchmark.o:	    redisclient.h redis_benchmark.cpp Cycles.h UnsyncedRpcTracker.h MurmurHash3.h
TimeTrace.o:		    TimeTrace.h Atomic.h
Cycles.o:		    Cycles.h
MurmurHash3.o:		    MurmurHash3.h
UnsyncedRpcTracker.o:	    UnsyncedRpcTracker.h
Cycles.o:		    Cycles.h
test_client.o:              redisclient.h test_client.cpp tests/functions.h
test_lists.o:               redisclient.h tests/test_lists.cpp tests/functions.h
test_sets.o:                redisclient.h tests/test_sets.cpp tests/functions.h
test_zsets.o:               redisclient.h tests/test_zsets.cpp tests/functions.h
test_hashes.o:              redisclient.h tests/test_hashes.cpp tests/functions.h
test_cluster.o:             redisclient.h tests/test_cluster.cpp tests/functions.h
test_distributed_strings.o: redisclient.h tests/test_distributed_strings.cpp tests/functions.h
test_distributed_ints.o:    redisclient.h tests/test_distributed_ints.cpp tests/functions.h
test_distributed_mutexes.o: redisclient.h tests/test_distributed_mutexes.cpp tests/functions.h
test_generic.o:             redisclient.h tests/test_generic.cpp
benchmark.o:                redisclient.h tests/benchmark.cpp tests/functions.h
