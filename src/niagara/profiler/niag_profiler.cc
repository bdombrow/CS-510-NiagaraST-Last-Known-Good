#include <string.h>
#include <iostream.h>
#include <fstream.h>
#include <stdio.h>

#include <niag_profiler.h>
#include <np_consts.h>
#include <np_funcs.h>

static Niag_Profiler profiler;
void notifyEvent(JVMPI_Event *event);

// profiler agent entry point
extern "C" { 
  JNIEXPORT jint JNICALL JVM_OnLoad(JavaVM *jvm, char *options, void *reserved) {
    //fprintf(stderr, "niag_profiler> initializing ..... \n");
    
    // get jvmpi interface pointer
    if ((jvm->GetEnv((void **)&(profiler.jvmpi_interface), JVMPI_VERSION_1)) < 0) {
      fprintf(stderr, "myprofiler> error in obtaining jvmpi interface pointer\n");
      return JNI_ERR;
    } 

    if(profiler.processOptions(options) != JNI_OK) {
      barf(NULL);
    }

    if(profiler.profiling_on) {
      cout << "KT: PROFILING ON" << endl;
      // initialize jvmpi interface
      profiler.jvmpi_interface->NotifyEvent = notifyEvent;
      
      profiler.method_list = new Method_List();
      profiler.thread_list = new Thread_List(profiler.jvmpi_interface, 
					     profiler.method_list);
      
      // enabling class load event notification
      int ok = profiler.jvmpi_interface->EnableEvent(JVMPI_EVENT_CLASS_LOAD, NULL);
      ok = profiler.jvmpi_interface->EnableEvent(JVMPI_EVENT_CLASS_UNLOAD, NULL);
      ok = profiler.jvmpi_interface->EnableEvent(JVMPI_EVENT_THREAD_START, NULL);
      ok = profiler.jvmpi_interface->EnableEvent(JVMPI_EVENT_THREAD_END, NULL);
      ok = profiler.jvmpi_interface->EnableEvent(JVMPI_EVENT_DUMP_DATA_REQUEST, 
						 NULL);
      ok = profiler.jvmpi_interface->EnableEvent(JVMPI_EVENT_RESET_DATA_REQUEST, 
						 NULL);
      ok = profiler.jvmpi_interface->EnableEvent(JVMPI_EVENT_JVM_INIT_DONE, NULL);
      ok = profiler.jvmpi_interface->EnableEvent(JVMPI_EVENT_JVM_SHUT_DOWN, NULL);
      
      ok = profiler.jvmpi_interface->EnableEvent(JVMPI_EVENT_METHOD_ENTRY, NULL);
      ok = profiler.jvmpi_interface->EnableEvent(JVMPI_EVENT_METHOD_EXIT, NULL);

      ok =  profiler.jvmpi_interface->EnableEvent(
					JVMPI_EVENT_RAW_MONITOR_CONTENDED_ENTER, 
					NULL);
      ok =  profiler.jvmpi_interface->EnableEvent(
					JVMPI_EVENT_RAW_MONITOR_CONTENDED_ENTERED,
					NULL);
      ok =  profiler.jvmpi_interface->EnableEvent(
					JVMPI_EVENT_RAW_MONITOR_CONTENDED_EXIT, 
					NULL);
      ok =  profiler.jvmpi_interface->EnableEvent(
					JVMPI_EVENT_MONITOR_CONTENDED_ENTER, 
					NULL);
      ok =  profiler.jvmpi_interface->EnableEvent(
                                        JVMPI_EVENT_MONITOR_CONTENDED_ENTERED, 
				        NULL);
      ok =  profiler.jvmpi_interface->EnableEvent(
				        JVMPI_EVENT_MONITOR_CONTENDED_EXIT, 
				        NULL);
      ok =  profiler.jvmpi_interface->EnableEvent(JVMPI_EVENT_MONITOR_WAIT, NULL);
      ok =  profiler.jvmpi_interface->EnableEvent(JVMPI_EVENT_MONITOR_WAITED, 
						  NULL);
      ok =  profiler.jvmpi_interface->EnableEvent(JVMPI_EVENT_GC_START, NULL);
      ok =  profiler.jvmpi_interface->EnableEvent(JVMPI_EVENT_GC_FINISH, NULL);
      ok =  profiler.jvmpi_interface->EnableEvent(JVMPI_EVENT_OBJ_ALLOC, NULL);
      ok =  profiler.jvmpi_interface->EnableEvent(JVMPI_EVENT_OBJ_MOVE, NULL);
      ok =  profiler.jvmpi_interface->EnableEvent(JVMPI_EVENT_OBJ_FREE, NULL);
      ok =  profiler.jvmpi_interface->EnableEvent(JVMPI_EVENT_NEW_ARENA, NULL);
      ok =  profiler.jvmpi_interface->EnableEvent(JVMPI_EVENT_DELETE_ARENA, NULL);
      ok =  profiler.jvmpi_interface->EnableEvent(JVMPI_EVENT_JNI_GLOBALREF_ALLOC,
						  NULL);
      ok =  profiler.jvmpi_interface->EnableEvent(JVMPI_EVENT_JNI_GLOBALREF_FREE, 
						  NULL);

      if(ok != JVMPI_SUCCESS)
	barf("BAD EVENT ENABLE");
    }



    return JNI_OK;
  }

  

  JNIEXPORT jlong JNICALL Java_niagara_utils_JProf_getCurrentThreadCpuTime
          (JNIEnv*, jclass) {
            return profiler.jvmpi_interface == 0 ? 0 : profiler.jvmpi_interface->GetCurrentThreadCpuTime();
  }

  JNIEXPORT jint JNICALL Java_niagara_utils_JProf_registerThreadName(
				                JNIEnv* env, jclass cls, 
						jstring threadName) {
    const char *name = env->GetStringUTFChars(threadName, 0);
    profiler.thread_list->setName(env, name);
    env->ReleaseStringUTFChars(threadName, name);
    return JNI_OK;
  }

  JNIEXPORT jint JNICALL Java_niagara_utils_JProf_requestDataDump(JNIEnv*, jclass) {
    cout << "KT Dumping Data" << endl;
    profiler.thread_list->dumpData();
    profiler.thread_list->resetData();
    return JNI_OK;
  }
}

// function for handling event notification
void notifyEvent(JVMPI_Event *event) {
  
  Thread_Info* local_store;

  //switch(event->event_type & !JVMPI_REQUESTED_EVENT) {
  switch(event->event_type) {
    
  case JVMPI_EVENT_OBJECT_ALLOC:
    //cout << "Object Alloc received... ";
    local_store = 
      (Thread_Info*)profiler.jvmpi_interface->GetThreadLocalStorage(event->env_id);
    if(local_store == NULL) {
      local_store = profiler.thread_list->add(event->env_id, "unknown");
    }
    local_store->addAlloc(event);
    //cout << "object alloc finished " << endl;
    break;
    
  case JVMPI_EVENT_THREAD_START:
    //cout << "Thread start received: " << event->u.thread_start.thread_name << "...";
    local_store = (Thread_Info*)profiler.jvmpi_interface->GetThreadLocalStorage(event->env_id);
    if(local_store == NULL) {
      profiler.thread_list->add(event->u.thread_start.thread_env_id,
				event->u.thread_start.thread_name);
    }
    //cout << "thread start finished " << endl;
    break;

  case JVMPI_EVENT_THREAD_END:
    //cout << "Thread end received:...";
    profiler.thread_list->remove(event->env_id);
    //cout << "thread end finished" << endl;
    break;
    
  case JVMPI_EVENT_CLASS_LOAD:
  case JVMPI_EVENT_CLASS_LOAD | JVMPI_REQUESTED_EVENT:
    //cout << "Class load received...";
    profiler.method_list->addClassMethods(event);
    //cout << "class load finished" << endl;
    break;
    /*
  case JVMPI_EVENT_CLASS_UNLOAD:
    cout << "Class unload received...";
    profiler.method_list->removeClassMethods(event->u.class_unload.class_id);
    cout << "class unload finished" << endl;
    break;
    */
  case JVMPI_EVENT_DATA_DUMP_REQUEST:
  case JVMPI_EVENT_DATA_DUMP_REQUEST | JVMPI_REQUESTED_EVENT:
    cout << "Data dump received...";
    profiler.thread_list->dumpData();
    cout << "data dump finished" << endl;
    break;
    
  case JVMPI_EVENT_DATA_RESET_REQUEST:
    cout << "Data reset received...";
    profiler.thread_list->resetData();
    cout << "data reset finished" << endl;
    break;
  
  case JVMPI_EVENT_JVM_SHUT_DOWN:
    cout << "jvm shutdown received...";
    profiler.thread_list->dumpData();
    profiler.thread_list->resetData();
    cout << "jvm shutdonw finished" << endl;
    break;
  }
  return;
}

jint Niag_Profiler::processOptions(char* options) {

  if(options == NULL) {
    profiling_on = true;
    return JNI_OK;
  }
    
  if(strncmp(options, "profile", 3)==0) {
    if(strncmp(options+8, "yes", 3)==0) {
      profiling_on = true;
      return JNI_OK;
    } else if(strncmp(options+8, "no", 2)==0) {
      profiling_on = false;
      return JNI_OK;
    }
  }

  usage();
  return JNI_ERR;
}

void Niag_Profiler::usage() {
  cout << "Usage: -Xrunprofni:[profile:yes|no]" << endl;
}

