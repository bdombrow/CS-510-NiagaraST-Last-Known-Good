#include <thread_info.h>
#include <np_consts.h>
#include <np_funcs.h>
#include <string.h>
#include <niag_profiler.h>
#include <iostream.h>

extern Niag_Profiler profiler;

Thread_Info::Thread_Info(int _threadNum, char* _threadName, 
			 const Method_List* const _methodList) {
  threadNum = _threadNum;
  setName(_threadName);
  memoryAllocd = 0;
  methodList = _methodList;
  traceList = new Trace_List(methodList, threadNum);
  trace.num_frames = profiler.getTraceDepth();
  trace.frames = new JVMPI_CallFrame[profiler.getTraceDepth()];
}

Thread_Info::~Thread_Info() {
  delete []threadName;
  delete traceList;
}
  
void Thread_Info::addAlloc(JVMPI_Event *event) {
  // size is in bytes
  memoryAllocd+= event->u.obj_alloc.size; // u is union of event structs
  // look into traces
  trace.env_id = event->env_id;
  profiler.getCallTrace(&trace);
  traceList->addAlloc(&trace, event->u.obj_alloc.size, threadName);
}

void Thread_Info::print(ostream& os) {
  if(threadName == NULL)
    barf("Thread_Info::print thread name is null");
  os << "Thread: " << threadName;
  os << "-----Total Memory Allocd " << memoryAllocd << "(bytes)";
  traceList->processFreedObjList();
  long live_mem = traceList->getLiveMem();
  os << "  Live Memory " << live_mem << "(bytes)" << endl;

  if(traceList->getTotalMem() != memoryAllocd) {
    cerr << "WARNING: Trace List Mem (" << traceList->getTotalMem() << ") ";
    cerr << "not equal to allocd Mem (" << memoryAllocd << ")" << endl;
  }
  traceList->print(os, threadName);
  os << endl;
}

void Thread_Info::resetData() {
  memoryAllocd = 0;
  traceList->resetData();
}

void Thread_Info::setName(const char* newName) {
  strncpy(threadName, newName, NLEN); // threadName is 50 chars
  threadName[NLEN-1]='\0';
}

int Thread_Info::getThreadNum() {
  if(threadNum != traceList->threadId)
    barf("KT bad thread numbers");
  return threadNum;
}

int Thread_Info::mostRecentTrace() {
  return traceList->getMostRecentTrace();
}

int Thread_Info::numTraces() {
  return traceList->getNumTraces();
}

// ----------------- PRIVATE FUNCTIONS ------------------------------


