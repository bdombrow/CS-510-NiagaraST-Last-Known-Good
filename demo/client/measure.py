#!/usr/bin/python

import sys
import re
import biggles

def main():
    if len(sys.argv) != 3:
        print "Usage: measure.py <generator file> <client file>"
        return 1
    
    generator_trace = parse_file(sys.argv[1])
    client_trace = parse_file(sys.argv[2])
    
    regexp = re.compile("([^,]+),(.*)")
    sum_delay = count = 0
    max_delay = 0
    min_delay = sys.maxint
    generator_timestamps = []
    delays = []
    min_gen_ts = 0
    while 1:
        gen_line = generator_trace.readline()
        client_line = client_trace.readline()
        if (gen_line == "" or client_line == ""): break
        
        (gen_id, gen_ts) = regexp.search(gen_line).group(1,2)
        (client_id, client_ts) = regexp.search(client_line).group(1,2)

        if (gen_id != client_id):
            print "Tuple ids don't match: " + gen_id + client_id
            return 3
	client_ts = long(client_ts)
	gen_ts = long(gen_ts)
	delay = client_ts - gen_ts
        if (count == 0):
            min_gen_ts = gen_ts
            
        
        generator_timestamps.append(gen_ts - min_gen_ts)
        delays.append(delay)
	max_delay = max(max_delay, delay)
	min_delay = min(min_delay, delay)
	sum_delay += delay
	count += 1

    print "Avg delay = " + str(sum_delay/count) + "ms"
    print "Max delay = " + str(max_delay) + "ms"
    print "Min delay = " + str(min_delay) + "ms"
    p = biggles.FramedPlot()
    p.xrange = generator_timestamps[0] - 10, generator_timestamps[count-1]
    p.yrange = min_delay - 100, max_delay + 100
    a = biggles.Curve(generator_timestamps, delays,symboltype="plus")
    p.add(a)

    p.write_img(800, 400, "plot.png")
	



def parse_file(filename):
    try:
        return file(filename)
    except IOError, msg:
        print msg
        sys.exit(-1)

if __name__ == '__main__':
    sys.exit(main() or 0)
