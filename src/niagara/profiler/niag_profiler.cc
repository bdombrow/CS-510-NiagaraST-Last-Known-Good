#include <string.h>
#include <iostream.h>
#include <fstream.h>
#include <stdio.h>

#include <niag_profiler.h>
#include <np_consts.h>
#include <np_funcs.h>

extern void notifyEvent(JVMPI_Event* event);

jint Niag_Profiler::initialize(JavaVM* jvm, char* options) {
  
  // get jvmpi interface pointer
  if ((jvm->GetEnv((void **)&(jvmpiInterface), JVMPI_VERSION_1)) < 0) {
    fprintf(stderr, "myprofiler> error in obtaining jvmpi interface pointer\n");
    return JNI_ERR;
  } 
  
  if(processOptions(options) != JNI_OK) {
    barf(NULL);
  }
  
  if(profilingOn) {
    cout << "KT: PROFILING ON" << endl;
    // initialize jvmpi interface
    jvmpiInterface->NotifyEvent = notifyEvent;
    
    threadList = new Thread_List(jvmpiInterface, &methodList);
    
    // enabling class load event notification
    int ok = jvmpiInterface->EnableEvent(JVMPI_EVENT_CLASS_LOAD, NULL);
    ok = jvmpiInterface->EnableEvent(JVMPI_EVENT_CLASS_UNLOAD, NULL);
    ok = jvmpiInterface->EnableEvent(JVMPI_EVENT_THREAD_START, NULL);
    ok = jvmpiInterface->EnableEvent(JVMPI_EVENT_THREAD_END, NULL);
    ok = jvmpiInterface->EnableEvent(JVMPI_EVENT_DUMP_DATA_REQUEST, NULL);
    ok = jvmpiInterface->EnableEvent(JVMPI_EVENT_RESET_DATA_REQUEST, NULL);
    ok = jvmpiInterface->EnableEvent(JVMPI_EVENT_JVM_INIT_DONE, NULL);
    ok = jvmpiInterface->EnableEvent(JVMPI_EVENT_JVM_SHUT_DOWN, NULL);
    ok = jvmpiInterface->EnableEvent(JVMPI_EVENT_METHOD_ENTRY, NULL);
    ok = jvmpiInterface->EnableEvent(JVMPI_EVENT_METHOD_EXIT, NULL);
    ok =  jvmpiInterface->EnableEvent(JVMPI_EVENT_RAW_MONITOR_CONTENDED_ENTER, 
				      NULL);
    ok =  jvmpiInterface->EnableEvent(JVMPI_EVENT_RAW_MONITOR_CONTENDED_ENTERED,
				      NULL);
    ok =  jvmpiInterface->EnableEvent(JVMPI_EVENT_RAW_MONITOR_CONTENDED_EXIT, 
				      NULL);
    ok =  jvmpiInterface->EnableEvent(JVMPI_EVENT_MONITOR_CONTENDED_ENTER, 
				      NULL);
    ok =  jvmpiInterface->EnableEvent(JVMPI_EVENT_MONITOR_CONTENDED_ENTERED, 
				      NULL);
    ok =  jvmpiInterface->EnableEvent(JVMPI_EVENT_MONITOR_CONTENDED_EXIT, 
				      NULL);
    ok =  jvmpiInterface->EnableEvent(JVMPI_EVENT_MONITOR_WAIT, NULL);
    ok =  jvmpiInterface->EnableEvent(JVMPI_EVENT_MONITOR_WAITED, NULL);
    ok =  jvmpiInterface->EnableEvent(JVMPI_EVENT_GC_START, NULL);
    ok =  jvmpiInterface->EnableEvent(JVMPI_EVENT_GC_FINISH, NULL);
    ok =  jvmpiInterface->EnableEvent(JVMPI_EVENT_OBJ_ALLOC, NULL);
    ok =  jvmpiInterface->EnableEvent(JVMPI_EVENT_OBJ_MOVE, NULL);
    ok =  jvmpiInterface->EnableEvent(JVMPI_EVENT_OBJ_FREE, NULL);
    ok =  jvmpiInterface->EnableEvent(JVMPI_EVENT_NEW_ARENA, NULL);
    ok =  jvmpiInterface->EnableEvent(JVMPI_EVENT_DELETE_ARENA, NULL);
    ok =  jvmpiInterface->EnableEvent(JVMPI_EVENT_JNI_GLOBALREF_ALLOC,
				      NULL);
    ok =  jvmpiInterface->EnableEvent(JVMPI_EVENT_JNI_GLOBALREF_FREE, 
				      NULL);

    if(ok != JVMPI_SUCCESS)
      barf("BAD EVENT ENABLE");
  }
  
  freedLatch = 
    jvmpiInterface->RawMonitorCreate("FreedObjListLatch");
  allocLatch = 
    jvmpiInterface->RawMonitorCreate("AllocObjListLatch");
  
  return JNI_OK;
}

jint Niag_Profiler::registerThreadName(JNIEnv* env, jclass cls, jstring threadName) {

  const char *name = env->GetStringUTFChars(threadName, 0);
  threadList->setName(env, name);
  env->ReleaseStringUTFChars(threadName, name);
  return JNI_OK;
}

void Niag_Profiler::addAlloc(JVMPI_Event* event) {
  Thread_Info* localStore;
  localStore = 
    (Thread_Info*)jvmpiInterface->GetThreadLocalStorage(event->env_id);
  if(localStore == NULL) {
    localStore = threadList->add(event->env_id, "unknown");
  }
  localStore->addAlloc(event);
  getAllocObjListLatch();
  allocObjList.add(event->u.obj_alloc.obj_id, 
		   event->u.obj_alloc.size,
		   localStore->getThreadNum(),
		   localStore->mostRecentTrace());
  releaseAllocObjListLatch();
}

void Niag_Profiler::threadStart(JVMPI_Event* event) {
  //cout << "Thread start received: " << event->u.thread_start.thread_name << "...";
  Thread_Info* localStore;
  localStore = (Thread_Info*)jvmpiInterface->GetThreadLocalStorage(event->env_id);
  if(localStore == NULL) {
    threadList->add(event->u.thread_start.thread_env_id,
		    event->u.thread_start.thread_name);
  }
  //cout << "thread start finished " << endl;
}

void Niag_Profiler::threadEnd(JVMPI_Event* event) {
  // cout << "Thread end received:..." 
  threadList->remove(event->env_id);
  //cout << "thread end finished" << endl;
}

void Niag_Profiler::addClassMethods(JVMPI_Event* event) {
  methodList.addClassMethods(event);
}

void Niag_Profiler::dumpData() {
  threadList->dumpData();
}

void Niag_Profiler::resetData() {
  threadList->resetData();
  freedObjList.reset();
  allocObjList.reset(); 
}

jint Niag_Profiler::processOptions(char* _options) {
  char* options = _options;

  if(options == NULL) {
    profilingOn = true;
    return JNI_OK;
  }
 
  bool endOfOptions = false;
  bool found = true;

  while(options != NULL && found) {
    found = false;
    if(strncmp(options, "profile", 6)==0) {
      if(strncmp(options+8, "yes", 3)==0) {
	profilingOn = true;
	found = true;
      } else if(strncmp(options+8, "no", 2)==0) {
	profilingOn = false;
	found = true;
      }
    } else if(strncmp(options, "depth", 4)==0) {
      traceDepth = atoi(options+6);
      cout << "KT: TRACE_DEPTH is " << traceDepth << endl;
      found = true;
    }
    if(found) {
      options = goToNextOption(options);
    }
  }

  if(!found) {
    usage();
    return JNI_ERR;
  }
  return JNI_OK;
}

char* Niag_Profiler::goToNextOption(char* _options) {
  char* options = _options;
  while(*options != ',' && !isWhitespace(*options))
    options++;
  if(*options == ',')
    return options+1;
  else
    return NULL;
}

bool Niag_Profiler::isWhitespace(char c) {
  if(c == '\n' || c == ' ' || c == '\t')
    return true;
  return false;
}

void Niag_Profiler::usage() {
  cout << "Usage: -Xrunprofni:[profile:yes|no]" << endl;
}

void Niag_Profiler::getFreedObjListLatch() {
  jvmpiInterface->RawMonitorEnter(freedLatch);
}

void Niag_Profiler::getAllocObjListLatch() {
  jvmpiInterface->RawMonitorEnter(allocLatch);
}

void Niag_Profiler::releaseFreedObjListLatch() {
  jvmpiInterface->RawMonitorExit(freedLatch);
}

void Niag_Profiler::releaseAllocObjListLatch() {
  jvmpiInterface->RawMonitorExit(allocLatch);
}

jlong Niag_Profiler::getCurrentThreadCpuTime() {
  return jvmpiInterface == NULL ? 0 : jvmpiInterface->GetCurrentThreadCpuTime();
}

void Niag_Profiler::getCallTrace(JVMPI_CallTrace* tracePtr) {
  jvmpiInterface->GetCallTrace(tracePtr, traceDepth);
}

jobjectID Niag_Profiler::getFreedObjId(int idx){
  return freedObjList.getId(idx);
}

Obj_Info* Niag_Profiler::getObjInfo(jobjectID objId) {
  return allocObjList.getInfo(objId);
}

void Niag_Profiler::removeFreedObj(int idx) {
  freedObjList.remove(idx);
}

void Niag_Profiler::removeAllocdObj(jobjectID objId) {
  allocObjList.remove(objId);
}
