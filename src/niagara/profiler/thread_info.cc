#include <thread_info.h>
#include <np_consts.h>
#include <np_funcs.h>
#include <string.h>
#include <iostream.h>

Thread_Info::Thread_Info(int _thread_num, char* _thread_name, 
			 const JVMPI_Interface* const _jvmpi_interface,
			 const Method_List* const _method_list) {
  thread_num = _thread_num;
  setName(_thread_name);
  memory_allocd = 0;
  method_list = _method_list;
  trace_list = new Trace_List(method_list, thread_num);
  trace.num_frames = TRACE_DEPTH;
  trace.frames = new JVMPI_CallFrame[TRACE_DEPTH];
  jvmpi_interface = _jvmpi_interface;
}

Thread_Info::~Thread_Info() {
  delete []thread_name;
  delete trace_list;
}
  
void Thread_Info::addAlloc(JVMPI_Event *event) {
  // size is in bytes
  memory_allocd+= event->u.obj_alloc.size; // u is union of event structs
  // look into traces
  trace.env_id = event->env_id;
  jvmpi_interface->GetCallTrace(&trace, TRACE_DEPTH);
  trace_list->addAlloc(&trace, event->u.obj_alloc.size, thread_name);
}

void Thread_Info::print(ostream& os) {
  if(thread_name == NULL)
    barf("Thread_Info::print thread name is null");
  os << "Thread   " << thread_name; 
  os << "-----Total Memory Allocd " << memory_allocd << "(bytes)" << endl;
  if(trace_list->getTotalMem() != memory_allocd) {
    cerr << "WARNING: Trace List Mem (" << trace_list->getTotalMem() << ") ";
    cerr << "not equal to allocd Mem (" << memory_allocd << ")" << endl;
  }
  trace_list->print(os, thread_name);
  os << endl;
}

void Thread_Info::resetData() {
  memory_allocd = 0;
  trace_list->resetData();
}

void Thread_Info::setName(const char* newName) {
  strncpy(thread_name, newName, NLEN); // thread_name is 50 chars
  thread_name[NLEN-1]='\0';
}

// ----------------- PRIVATE FUNCTIONS ------------------------------


