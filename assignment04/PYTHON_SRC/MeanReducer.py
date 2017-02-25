#!/usr/bin/env python
import sys

def read_mapper_output(file, separator='\t'):
    for line in file:
        # destructure data, and yield as generator
        (station, month_day, temp) = line.strip().split(separator)
        yield (station, month_day, temp)

def main(separator='\t'):
    # seed our data values
    (last_key, count, total_sum) = (None, 0, 0)

    # read in from the generator
    data = read_mapper_output(sys.stdin, separator=separator)
    for line in data:
        # ensure structure is correct
        (station, month_day, temp) = line
        # and grab the key
        key = "%s\t%s" % (station, month_day)

        # keep count and determine mean
        if last_key and last_key != key:
            print "%s\t%s" % (last_key, total_sum / count)
            (last_key, count, total_sum) = (key, 1, int(temp))
        else:
            (last_key, count, total_sum) = (key, count + 1, total_sum + int(temp))

    # handle last key
    if last_key:
        print "%s\t%s" % (last_key, total_sum / count)

if __name__ == "__main__":
    main()
