#ifndef THREAD_INFO_H
#define THREAD_INFO_H

#include <iostream.h>
#include <trace_list.h>

const int NLEN = 50;

class Thread_Info {
 public:
  Thread_Info(int _thread_num, char* _thread_name, 
	      const JVMPI_Interface* const _jvmpi_interface,
	      const Method_List* const method_list);  
  ~Thread_Info();
  void addAlloc(JVMPI_Event *event);
  void print(ostream& os);
  void resetData();
  void setName(const char* newName);

 private:
  int thread_num;
  char thread_name[NLEN];
  long memory_allocd;
  Trace_List* trace_list;
  JVMPI_CallTrace trace; // used as arg to GetCallTrace
  const JVMPI_Interface *jvmpi_interface;
  const Method_List* method_list;
};

#endif // THREAD_INFO_H
