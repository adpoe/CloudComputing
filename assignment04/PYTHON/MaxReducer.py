""" MaxReducer
    CS1699 - Cloud Computing
    Anthony Poerio
    adp59@pitt.edu """

import sys

def read_mapper_output(file, separator='\t'):
    # split stdin, line by line
    for line in file:
        yield line.strip().split("\t")

def main(separator='\t'):
    # grab a generator for stdin
    data = read_mapper_output(sys.stdin, separator='separator')

    # get base values
    (last_key, max_val) = (None, float('-inf'))

    # go through each line and check for max
    for line in data:
        (station, date, temp) = line      # destructure line
        key = "%s\t%s" % (station, date)  # store a key that combines station and date

        # we are sorted, so print after key changes, we know we're at max
        if last_key and last_key != key:
            print "%s\t%s" % (last_key, max_val)
            (last_key, max_val) = (key, int(temp))
        else:
            (last_key, max_val) = (key, max(max_val, int(temp)))

    # ensure last key is seen
    if last_key:
        print "%s\t%s" % (last_key, max_val)


####################
# MAIN ENTRY POINT #
####################
if __name__ == "__main__":
    main()
