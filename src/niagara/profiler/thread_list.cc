#include <thread_list.h>
#include <np_funcs.h>

Thread_List::Thread_List(const JVMPI_Interface* const _jvmpi_interface,
			 const Method_List* const _method_list) {
  numAlloc = 40;
  numThreads = 0;
  threads = new JNIEnv*[numAlloc];
  thread_num = 0;
  out_cnt = 0;
  jvmpi_interface = _jvmpi_interface;
  method_list = _method_list;
}

Thread_List::~Thread_List() {
  delete []threads;
}

Thread_Info* Thread_List::add(JNIEnv* thread, char* name){
  checkSpace();
  Thread_Info* local_store = new Thread_Info(get_new_thread_num(), name,
					     jvmpi_interface, method_list);
  jvmpi_interface->SetThreadLocalStorage(thread, local_store);
  threads[numThreads] = thread;
  numThreads++;
  return local_store;
}

void Thread_List::remove(JNIEnv* thread) {
  for(int i = 0; i<numThreads; i++) {
    if(threads[i] == thread) {
      threads[i] = NULL;
      //cout << "Removing thread " << i;
      return;
    }
  }
  barf("WARNING: Unable to remove thread ");
}

void Thread_List::setName(JNIEnv* thread, const char* threadName) {
  Thread_Info* local_storage;
  local_storage = (Thread_Info*)jvmpi_interface->GetThreadLocalStorage(thread);
  local_storage->setName(threadName);
  return;
}

void Thread_List::dumpData() {
  openOutput();

  if(!os.is_open())
    barf("KT: failed to open output");

  Thread_Info* local_storage;
  cout << "Dumping data (" << numThreads << ")" << endl;
  for(int i = 0; i<numThreads; i++) {
    //cout << "Thread " << i << endl;
    if(threads[i] != NULL) {
      local_storage = (Thread_Info*)jvmpi_interface->GetThreadLocalStorage(threads[i]);
      if(local_storage == NULL)
	barf("local storage is null in Thread_List::dumpData");
      local_storage->print(os);
    }
  }
  
  os.close();
}

void Thread_List::resetData() {
  Thread_Info* local_storage;
  for(int i = 0; i<numThreads; i++) {
    if(threads[i] != NULL) {
      local_storage = (Thread_Info*)jvmpi_interface->GetThreadLocalStorage(threads[i]);
      local_storage->resetData();
    }
  }
}

// --------------------- PRIVATE FUNCTIONS ------------------------------
void Thread_List::checkSpace() {
  if(numThreads < numAlloc)
    return;
  
  int oldSize = numAlloc;
  numAlloc *= 2;
  JNIEnv** newThreads = new JNIEnv*[numAlloc];
  int iOld = 0;
  int iNew = 0;
  while(iOld<oldSize) {
    if(threads[iOld] == NULL) {
      iOld++;
    } else {
      newThreads[iNew] = threads[iOld];
      iOld++;
      iNew++;
    }
  }
  delete []threads;
  threads = newThreads;
  numThreads = iNew;
}

void Thread_List::openOutput() {
  char* outfile = new char[20];
  sprintf(outfile, "%s%d%s", "niag_prof", out_cnt, ".txt");
  out_cnt++;
  os.open(outfile);  
}

// returns a number between 0 and num_threads identifying this thread
// static function
int Thread_List::get_thread_num(JNIEnv* env_id) {

  Thread_Info* local_storage 
    = (Thread_Info*)jvmpi_interface->GetThreadLocalStorage(env_id);
  if(!local_storage) {
   int thread_num = get_new_thread_num();
   local_storage = new Thread_Info(thread_num, "Unknown", jvmpi_interface,
				   method_list);
   return thread_num;
  }
}

// static function
int Thread_List::get_new_thread_num() {
  int new_thread_num;
  //get_mutex();
  new_thread_num = thread_num;
  //cout << "TNum " << new_thread_num << "...";
  thread_num++;
  //release_mutex();
  return new_thread_num;
}

