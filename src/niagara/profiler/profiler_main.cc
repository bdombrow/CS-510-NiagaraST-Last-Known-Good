#include <niag_profiler.h>

Niag_Profiler profiler;
void notifyEvent(JVMPI_Event *event);

// profiler agent entry point
extern "C" { 
  JNIEXPORT jint JNICALL JVM_OnLoad(JavaVM *jvm, char *options, void *reserved) {
    return profiler.initialize(jvm, options);
  }
  
  JNIEXPORT jlong JNICALL Java_niagara_utils_JProf_getCurrentThreadCpuTime
  (JNIEnv*, jclass) {
    return profiler.getCurrentThreadCpuTime();
  }

  JNIEXPORT jint JNICALL Java_niagara_utils_JProf_registerThreadName(
				                JNIEnv* env, jclass cls, 
						jstring threadName) {

    return profiler.registerThreadName(env, cls, threadName);
  }

  JNIEXPORT jint JNICALL Java_niagara_utils_JProf_requestDataDump(JNIEnv*, jclass) {
    cout << "KT Dumping Data" << endl;
    profiler.dumpData();
    profiler.resetData();
    return JNI_OK;
  }
}

// function for handling event notification
void notifyEvent(JVMPI_Event *event) {
  bool found;

  //cout << "KT received event " << event->event_type << endl;
  
  //switch(event->event_type & !JVMPI_REQUESTED_EVENT) {
  switch(event->event_type) {
    
  case JVMPI_EVENT_OBJECT_ALLOC:    
    profiler.addAlloc(event);
    break;
    
  case JVMPI_EVENT_OBJECT_FREE:
    profiler.addFreedObject(event->u.obj_free.obj_id);
    break;

  case JVMPI_EVENT_GC_START:
    // start timer!
    profiler.getFreedObjListLatch();
    break;
    
  case JVMPI_EVENT_GC_FINISH:
    // end timer and sum!
    profiler.releaseFreedObjListLatch();
    break;

  case JVMPI_EVENT_THREAD_START:
    profiler.threadStart(event);
    break;

  case JVMPI_EVENT_THREAD_END:
    profiler.threadEnd(event);
    break;
    
  case JVMPI_EVENT_CLASS_LOAD:
  case JVMPI_EVENT_CLASS_LOAD | JVMPI_REQUESTED_EVENT:
    profiler.addClassMethods(event);
    break;

    /*
  case JVMPI_EVENT_CLASS_UNLOAD:
    cout << "Class unload received...";
    profiler.methodList->removeClassMethods(event->u.class_unload.class_id);
    cout << "class unload finished" << endl;
    break;
    */

  case JVMPI_EVENT_DATA_DUMP_REQUEST:
  case JVMPI_EVENT_DATA_DUMP_REQUEST | JVMPI_REQUESTED_EVENT:
    cout << "Data dump received...";
    profiler.dumpData();
    cout << "data dump finished" << endl;
    break;
    
  case JVMPI_EVENT_DATA_RESET_REQUEST:
    cout << "Data reset received...";
    profiler.resetData();
    cout << "data reset finished" << endl;
    break;
  
  case JVMPI_EVENT_JVM_SHUT_DOWN:
    cout << "jvm shutdown received...";
    profiler.dumpData();
    profiler.resetData();
    cout << "jvm shutdown finished" << endl;
    break;
  }
  return;
}
