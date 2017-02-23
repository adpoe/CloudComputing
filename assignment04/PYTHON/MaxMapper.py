""" MaxMapper
    CS1699 - Cloud Computing
    Anthony Poerio
    adp59@pitt.edu """

import sys
import re

def read_input(file):
    # parse input and yield it, line by line
    for line in file:
        val = line.strip()
        (usaf, wban, date, temp, q) = (val[4:10], val[10:15], val[15:23],
                                       int(val[87:92]), val[92:93])
        yield (usaf, wban, date, temp, q)

def main():
    # send stdin to a reader function
    data = read_input(sys.stdin)
    for line in data:
        # get each line in the structure we want, and print to stdout
        (usaf, wban, date, temp, q) = line
        if (temp != 9999 and re.match("[01459]", q)):
                print "%s-%s\t%s\t%s" % (usaf, wban, date, temp)

if __name__ == "__main__":
    main()
