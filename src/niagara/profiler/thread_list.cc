#include <thread_list.h>
#include <np_funcs.h>
#include <niag_profiler.h>

extern Niag_Profiler profiler;

Thread_List::Thread_List(const JVMPI_Interface* const _jvmpiInterface,
			 const Method_List* const _methodList) {
  numAlloc = 40;
  numThreads = 0;
  threads = new JNIEnv*[numAlloc];
  outCnt = 0;
  jvmpiInterface = _jvmpiInterface;
  methodList = _methodList;
}

Thread_List::~Thread_List() {
  delete []threads;
}

Thread_Info* Thread_List::add(JNIEnv* thread, char* name){
  checkSpace();
  int tnum = getNextThreadNum();
  Thread_Info* localStore = new Thread_Info(tnum, name, methodList);
  jvmpiInterface->SetThreadLocalStorage(thread, localStore);
  threads[tnum] = thread;
  return localStore;
}

void Thread_List::remove(JNIEnv* thread) {
  if(!os.is_open()) {
    openOutput();
  }

  Thread_Info* localStorage;
  for(int i = 0; i<numThreads; i++) {
    if(threads[i] == thread) {
      localStorage = (Thread_Info*)jvmpiInterface->GetThreadLocalStorage(threads[i]);
      if(localStorage == NULL)
	barf("local storage is null in Thread_List::dumpData");
      localStorage->print(os);
      threads[i] = NULL;
      return;
    }
  }
  barf("WARNING: Unable to remove thread ");
}

void Thread_List::setName(JNIEnv* thread, const char* threadName) {
  Thread_Info* localStorage;
  localStorage = (Thread_Info*)jvmpiInterface->GetThreadLocalStorage(thread);
  localStorage->setName(threadName);
  return;
}

void Thread_List::dumpData() {
  if(!os.is_open())
    openOutput();

  if(!os.is_open())
    barf("KT: failed to open output");

  Thread_Info* localStorage;
  cout << "Dumping data (" << numThreads << ")" << endl;
  for(int i = 0; i<numThreads; i++) {
    //cout << "Thread " << i << endl;
    if(threads[i] != NULL) {
      localStorage = (Thread_Info*)jvmpiInterface->GetThreadLocalStorage(threads[i]);
      if(localStorage == NULL)
	barf("local storage is null in Thread_List::dumpData");
      localStorage->print(os);
    }
  }
  
  os.close();
}

void Thread_List::resetData() {
  Thread_Info* localStorage;
  for(int i = 0; i<numThreads; i++) {
    if(threads[i] != NULL) {
      localStorage = (Thread_Info*)jvmpiInterface->GetThreadLocalStorage(threads[i]);
      localStorage->resetData();
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
  sprintf(outfile, "%s%d%s", "niag_prof", outCnt, ".txt");
  outCnt++;
  os.open(outfile);  
}

// returns a number between 0 and numThreads identifying this thread
// static function
int Thread_List::getThreadNum(JNIEnv* envId) {

  Thread_Info* localStorage 
    = (Thread_Info*)jvmpiInterface->GetThreadLocalStorage(envId);
  if(!localStorage) {
   int tnum = getNextThreadNum();
   localStorage = new Thread_Info(threadNum, "Unknown", methodList);
   return tnum;
  }
}

// static function
int Thread_List::getNextThreadNum() {
  int newThreadNum;
  //get_mutex();
  newThreadNum = numThreads;
  numThreads++;
  //release_mutex();
  return newThreadNum;
}

