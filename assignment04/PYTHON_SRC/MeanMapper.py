#!/usr/bin/env python
import sys

def read_input(file):
    # destructure each line and yield line-by-line, as a generator
    for line in file:
        (station, date, temp) = line.strip().split("\t")
        yield (station, date, temp)

def main(separator='\t'):
    data = read_input(sys.stdin)
    for line in data:
        (station, date, temp) = line
        # pull out only month/day from date
        print "%s\t%s\t%s" % (station, date[4:8], temp)

if __name__ == "__main__":
    main()
