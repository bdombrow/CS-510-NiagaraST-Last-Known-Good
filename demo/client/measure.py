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

    # Draw tuple latency plot
    p = biggles.FramedPlot()
    p.title = "Tuple latency at client"
    p.xlabel = "tuple generation time (s)"
    p.ylabel = "tuple latency (ms)"
    p.add(biggles.PlotLabel(.75, .89, "Avg. " + str(sum_delay/count) + "ms", halign="left"))
    p.add(biggles.PlotLabel(.75, .83, "Min. " + str(min_delay) + "ms", texthalign="left"))
    p.add(biggles.PlotLabel(.75, .77, "Max. " + str(max_delay) + "ms", texthalign="left"))
    p.add(biggles.PlotBox((.74, .73), (.94, .93)))

    start_x = generator_timestamps[0] 
    end_x = generator_timestamps[count-1]
    
    p.xrange = start_x, end_x
    p.x1.ticks = int((end_x - start_x)/10000) + 1
    p.x1.ticklabels = [str(x) for x in range(0, (end_x - start_x)/1000, 10)]
    p.yrange = 0, max_delay + 100
    a = biggles.Curve(generator_timestamps, delays,symboltype="plus")
    p.add(a)

    p.write_img(800, 400, "client-latency.png")
	
    # Tuple latency detail
    p = biggles.FramedPlot()
    p.title = "Tuple latency at client (detail)"
    p.xlabel = "tuple generation time (s)"
    p.ylabel = "tuple latency (ms)"

    # Show 10000 tuples before and after center
    begin = count/2 - 10000
    end = count/2 + 10000

    (min_delay, max_delay) = (max_delay, min_delay)
    sum = 0
    for i in range(begin, end):
        if (delays[i] < min_delay): min_delay = delays[i]
        if (delays[i] > max_delay): max_delay = delays[i]
        sum += delays[i]

    p.add(biggles.PlotLabel(.75, .89, "Avg. " + str(sum/20000) + "ms", halign="left"))
    p.add(biggles.PlotLabel(.75, .83, "Min. " + str(min_delay) + "ms", texthalign="left"))
    p.add(biggles.PlotLabel(.75, .77, "Max. " + str(max_delay) + "ms", texthalign="left"))
    p.add(biggles.PlotBox((.74, .73), (.94, .93)))
    
    start_x = generator_timestamps[begin] 
    end_x = generator_timestamps[end]
    
    p.xrange = start_x, end_x
    p.yrange = 0, max_delay + 100
    p.x1.ticks = int((end_x - start_x)/2000) + 1
    p.x1.ticklabels = [str(x) for x in range(start_x/1000, end_x/1000, 2)]
    a = biggles.Points(generator_timestamps[begin:end], delays[begin:end],symboltype="plus")
    p.add(a)

    p.write_img(800, 400, "detail-client.png")


    # Delay between tuples at generator
    p = biggles.FramedPlot()
    p.title = "Inter-tuple delay at generator"
    p.xlabel = "tuple generation time (s)"
    p.ylabel = "tuple delay (ms)"

    begin = 1
    end = count - 1

    gen_delay = []
    (min_delay, max_delay) = (sys.maxint, 0)
    sum = 0
    for i in range(begin, end):
        gen_del = generator_timestamps[i] - generator_timestamps[i - 1]
        if (gen_del > max_delay): max_delay = gen_del
        gen_delay.append(int(gen_del))

    start_x = generator_timestamps[begin] 
    end_x = generator_timestamps[end]
    
    p.xrange = start_x - 5, end_x + 5
    p.x1.ticks = int((end_x - start_x)/10000) + 1
    p.x1.ticklabels = [str(x) for x in range(0, (end_x - start_x)/1000, 10)]
    p.yrange = 0, max_delay + 100
    a = biggles.Points(generator_timestamps[begin:end], gen_delay,symboltype="plus")
    p.add(a)

    p.write_img(800, 400, "generator.png")
def parse_file(filename):
    try:
        return file(filename)
    except IOError, msg:
        print msg
        sys.exit(-1)

if __name__ == '__main__':
    sys.exit(main() or 0)
