#ifndef NIAG_PROFILER_H
#define NIAG_PROFILER_H

#include <jvmpi.h>
#include <thread_list.h>
#include <method_list.h>
#include <freed_obj_list.h>
#include <alloc_obj_list.h>

class Niag_Profiler {
 private:
  Thread_List* threadList;
  JVMPI_RawMonitor freedLatch;
  JVMPI_RawMonitor allocLatch;
  Freed_Obj_List freedObjList;
  Alloc_Obj_List allocObjList;
  JVMPI_Interface* jvmpiInterface;
  Method_List methodList;
  int traceDepth;
  bool profilingOn;

  jint processOptions(char* _options);
  bool isWhitespace(char c);
  char* goToNextOption(char* _options);
  void usage();

public:

  int getTraceDepth() {return traceDepth;};

  void getFreedObjListLatch();
  void getAllocObjListLatch();
  void releaseFreedObjListLatch();
  void releaseAllocObjListLatch();

  void dumpData();
  void resetData();

  jint initialize(JavaVM* jvm, char* options);
  jint registerThreadName(JNIEnv* enf, jclass cls, jstring threadName);

  // don't latch, assume called within gc and latch already obtained
  void addFreedObject(jobjectID objId) {
    freedObjList.add(objId);
  }

  void addAlloc(JVMPI_Event* event);
  void threadStart(JVMPI_Event* event);
  void threadEnd(JVMPI_Event* event);
  void addClassMethods(JVMPI_Event* event);
  jlong getCurrentThreadCpuTime();
  void getCallTrace(JVMPI_CallTrace* tracePtr);
  jobjectID getFreedObjId(int idx);
  Obj_Info* getObjInfo(jobjectID objId);
  void removeFreedObj(int idx);
  void removeAllocdObj(jobjectID objId);
  int getFreedListSize() {
    return freedObjList.getListSize();
  }
};

#endif
