#ifndef THREAD_LIST_H
#define THREAD_LIST_H

#include <iostream.h>
#include <fstream.h>
#include <jvmpi.h>

#include <thread_info.h>

class Thread_List {
 public:
  Thread_List(const JVMPI_Interface* const _jvmpi_interface, 
	      const Method_List* const _method_list);
  ~Thread_List();
  Thread_Info* add(JNIEnv* thread, char* name);
  void remove(JNIEnv* thread);
  void dumpData();
  void resetData();
  int getThreadNum(JNIEnv* env_id);
  void setName(JNIEnv* thread, const char* threadName);

 private:
  int numAlloc;
  int numThreads;
  JNIEnv** threads;
  ofstream os;
  int outputCount;
  const JVMPI_Interface* jvmpiInterface;
  const Method_List* methodList;
  int threadNum;
  int outCnt;
  void checkSpace();
  void openOutput();
  int getNextThreadNum();
};

#endif
