JDK_HOME = /disk/hopper/projects/disc/niagara/install/IBMJava2-14
JDK_HOME = /disk/hopper/projects/disc/niagara/install/j2sdk1.4.1_02

INCLUDES = -I. -I$(JDK_HOME)/include -I$(JDK_HOME)/include/linux

HFILES = alloc_obj_list.h freed_obj_list.h method_list.h \
         niag_profiler.h np_consts.h np_funcs.h thread_info.h \
	 thread_list.h trace_list.h

CC = g++ -c $(INCLUDES)

all: libprofni.so

niag_profiler.o: niag_profiler.cc $(HFILES)
	$(CC) niag_profiler.cc -o $@

alloc_obj_list.o: alloc_obj_list.cc $(HFILES)
	$(CC) alloc_obj_list.cc -o $@

freed_obj_list.o: freed_obj_list.cc $(HFILES)
	$(CC) freed_obj_list.cc -o $@

method_list.o: method_list.cc $(HFILES)
	$(CC) method_list.cc -o $@

profiler_main.o: profiler_main.cc $(HFILES)
	$(CC) profiler_main.cc -o $@

thread_info.o: thread_info.cc $(HFILES)
	$(CC) thread_info.cc -o $@

thread_list.o: thread_list.cc $(HFILES)
	$(CC) thread_list.cc -o $@

trace_list.o: trace_list.cc $(HFILES) 
	$(CC) trace_list.cc -o $@

libprofni.so: niag_profiler.o alloc_obj_list.o freed_obj_list.o \
        method_list.o thread_info.o \
        thread_list.o trace_list.o profiler_main.o
	g++ -shared -o $@ niag_profiler.o alloc_obj_list.o freed_obj_list.o method_list.o thread_info.o thread_list.o trace_list.o profiler_main.o

install:
	mv libprofni.so ../../../lib

clean:
	rm -f *.so *.o

# NB - can't find library means link problem, try removing
# -shared from compilation and see if you get an error
