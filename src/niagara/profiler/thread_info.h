#ifndef THREAD_INFO_H
#define THREAD_INFO_H

#include <iostream.h>
#include <trace_list.h>

const int NLEN = 50;

class Thread_Info {
 public:
  Thread_Info(int _thread_num, char* _thread_name, 
	      const Method_List* const method_list);  
  ~Thread_Info();
  void addAlloc(JVMPI_Event *event);
  void print(ostream& os);
  void resetData();
  void setName(const char* newName);
  int  getThreadNum();
  int  mostRecentTrace();
  int  numTraces();

 private:
  int threadNum;
  char threadName[NLEN];
  long memoryAllocd;
  Trace_List* traceList;
  JVMPI_CallTrace trace; // used as arg to GetCallTrace
  const JVMPI_Interface *jvmpiInterface;
  const Method_List* methodList;
};

#endif // THREAD_INFO_H
