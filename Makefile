# Redis C++ Client Library Makefile

VPATH = tests

#CFLAGS?= -pedantic -O2 -Wall -DNEBUG -W
CFLAGS?= -std=c++11 -pedantic -O0 -W -DDEBUG -g
CC = g++

CLIENTOBJS = anet.o
LIBNAME = libredisclient.a

#TESTAPP = test_client
TESTAPP = redis_benchmark
TESTAPPOBJS = Cycles.o redis_benchmark.o test_lists.o test_sets.o test_zsets.o test_hashes.o test_cluster.o test_distributed_strings.o test_distributed_ints.o test_distributed_mutexes.o test_generic.o benchmark.o functions.o
TESTAPPLIBS = $(LIBNAME) -lstdc++ -lboost_system -lboost_thread -lpthread

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
	$(CC) -o $(TESTAPP) $(TESTAPPOBJS) $(TESTAPPLIBS)

test: $(TESTAPP)
	@./test_client

check: test

clean:
	rm -rf $(LIBNAME) *.o $(TESTAPP)

dep:
	$(CC) -MM *.c *.cpp

log:
	git log '--pretty=format:%ad %s' --date=short > Changelog

anet.o:                     anet.c fmacros.h anet.h
redis_benchmark.o:	    redisclient.h redis_benchmark.cpp Cycles.h
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
