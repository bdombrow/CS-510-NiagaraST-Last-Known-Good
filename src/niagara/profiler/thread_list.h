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
  int get_thread_num(JNIEnv* env_id);
  void setName(JNIEnv* thread, const char* threadName);

 private:
  int numAlloc;
  int numThreads;
  JNIEnv** threads;
  ofstream os;
  int output_count;
  const JVMPI_Interface* jvmpi_interface;
  const Method_List* method_list;
  int thread_num;
  int out_cnt;
  void checkSpace();
  void openOutput();
  int get_new_thread_num();
};

#endif
