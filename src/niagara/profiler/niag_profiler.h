#ifndef NIAG_PROFILER_H
#define NIAG_PROFILER_H

#include <jvmpi.h>
#include <thread_list.h>
#include <method_list.h>

class Niag_Profiler {
public:
  JVMPI_Interface* jvmpi_interface;
  Thread_List* thread_list;
  Method_List* method_list;
  jint processOptions(char* options);
  void usage();
  bool profiling_on;
};

#endif
