JDK_HOME = /disk/hopper/projects/disc/niagara/install/j2sdk1.4.0_01

INCLUDES = -I. -I$(JDK_HOME)/include -I$(JDK_HOME)/include/linux

CC = g++ -c $(INCLUDES)

all: libprofni.so

niag_profiler.o: niag_profiler.cc np_funcs.h np_consts.h \
        thread_list.h  
	$(CC) niag_profiler.cc -o $@

method_list.o: method_list.h method_list.cc
	$(CC) method_list.cc -o $@

thread_info.o: thread_info.h thread_info.cc trace_list.h \
               np_consts.h 
	$(CC) thread_info.cc -o $@

thread_list.o: thread_list.h thread_list.cc thread_info.h \
               np_funcs.h 
	$(CC) thread_list.cc -o $@

trace_list.o: trace_list.h trace_list.cc np_consts.h 
	$(CC) trace_list.cc -o $@

libprofni.so: niag_profiler.o method_list.o thread_info.o \
        thread_list.o trace_list.o
	g++ -shared -o $@ niag_profiler.o method_list.o thread_info.o thread_list.o trace_list.o

install:
	mv libprofni.so ../../../lib

clean:
	rm -f *.so *.o

# NB - can't find library means link problem, try removing
# -shared from compilation and see if you get an error
