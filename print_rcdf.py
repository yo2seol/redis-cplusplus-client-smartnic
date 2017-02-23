#!/usr/bin/env python

# Copyright (c) 2011-2017 Stanford University
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

import glob
import re
import sys
import numpy as np

def print_basic_from_file(filename):
    # Read the log file into an array of numbers.
    numbers = []
    globResult = glob.glob(filename)
    if len(globResult) == 0:
        raise Exception("couldn't find raw data file at %s" % (filename))
    result = "";
    leader = '>>> '
    for line in open(globResult[0], 'r'):
        if re.match(leader, line):
            continue
        if not re.match('([0-9]+\.[0-9]+) ', line):
            for value in line.split(","):
                try:
                    numbers.append(float(value))
                except ValueError, e:
                    print("Skipping, couldn't parse %s" % line)

    # Generate a CDF from the array.
    numbers.sort()
    print("count: %8d" % (len(numbers)))
    print("avg:       %8.3f" % (np.mean(numbers)))
    print("median:    %8.3f" % (numbers[int(len(numbers)/2)]))
    print("min:       %8.3f" % (numbers[0]))
    print("max:       %8.3f" % (numbers[len(numbers)-1]))


def print_cdf_from_file(filename):
    # Read the log file into an array of numbers.
    numbers = []
    globResult = glob.glob(filename)
    if len(globResult) == 0:
        raise Exception("couldn't find raw data file at %s" % (filename))
    result = "";
    leader = '>>> '
    for line in open(globResult[0], 'r'):
        if re.match(leader, line):
            continue
        if not re.match('([0-9]+\.[0-9]+) ', line):
            for value in line.split(","):
                try:
                    numbers.append(float(value))
                except ValueError, e:
                    print("Skipping, couldn't parse %s" % line)

    # Generate a CDF from the array.
    numbers.sort()
    result = []
    print("%8.2f    %8.3f" % (0.0, 0.0))
    print("%8.2f    %8.3f" % (numbers[0], float(1)/len(numbers)))
    for i in range(1, 100):
        print("%8.2f    %8.3f" % (numbers[int(len(numbers)*float(i)/100)], float(i)/100))
    print("%8.2f    %8.3f" % (numbers[int(len(numbers)*999/1000)], .999))
    print("%8.2f    %9.4f" % (numbers[int(len(numbers)*9999/10000)], .9999))
    print("%8.2f    %8.3f" % (numbers[-1], 1.0))

def print_rcdf_from_file(filename):
    """
    Given the index of a client, print in gnuplot format a reverse cumulative
    distribution of the data in the client's log file (where "data" consists
    of comma-separated numbers stored in all of the lines of the log file
    that are not RAMCloud log messages). Each line in the printed output
    will contain a fraction and a number, such that the given fraction of all
    numbers in the log file have values less than or equal to the given number.
    """

    # Read the log file into an array of numbers.
    numbers = []
    globResult = glob.glob(filename)
    if len(globResult) == 0:
        raise Exception("couldn't find raw data file at %s" % (filename))
    result = "";
    leader = '>>> '
    for line in open(globResult[0], 'r'):
        if re.match(leader, line):
            continue
        if not re.match('([0-9]+\.[0-9]+) ', line):
            for value in line.split(","):
                try:
                    numbers.append(float(value))
                except ValueError, e:
                    print("Skipping, couldn't parse %s" % line)

    # Generate a RCDF from the array.
    numbers.sort()
    print("%8.2f    %11.6f" % (numbers[0], 1.0))
    for i in range(1, len(numbers)-1):
        if (numbers[i] != numbers[i-1] or numbers[i] != numbers[i+1]):
            print("%8.2f    %11.6f" % (numbers[i], 1-(float(i)/len(numbers))))
    print("%8.2f    %11.6f" % (numbers[-1], float(1)/len(numbers)))

def main(argv):
    if (len(argv) > 2 and argv[2] == "cdf"):
        print_cdf_from_file(argv[1])
    elif (len(argv) > 2 and argv[2] == "basic"):
        print_basic_from_file(argv[1])
    else:
        print_rcdf_from_file(argv[1])
    pass

if __name__ == "__main__":
    main(sys.argv)
