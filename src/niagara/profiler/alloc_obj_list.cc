#include <alloc_obj_list.h>
#include <np_funcs.h>
#include <niag_profiler.h>

extern Niag_Profiler profiler;


Obj_Info::Obj_Info(jobjectID _objId, int _objSize, int _threadNum, int _traceNum) {
  objId = _objId;
  objSize = _objSize;
  threadNum = _threadNum;
  traceNum = _traceNum;
  next = NULL;
}
		   

Alloc_Obj_List::Alloc_Obj_List() {
  allocList = new Obj_Info*[HASH_TABLE_SIZE];
}

void Alloc_Obj_List::add(jobjectID id, int size, int threadNum, int traceNum) {
  // needs to be hash table
  int idx = hash(id);
  Obj_Info* old_list = allocList[idx];
  allocList[idx] = new Obj_Info(id, size, threadNum, traceNum);
  allocList[idx]->next = old_list;
}

Obj_Info* Alloc_Obj_List::getInfo(jobjectID id) {
  // needs to be hash table
  int idx = hash(id);
  Obj_Info* list_ptr = allocList[idx];
  while(list_ptr != NULL) {
    if(list_ptr->objId == id) { 
      //cout << "KT found obj id in Alloc_Obj_List::getInfo" << endl;
      return list_ptr;
    }
    list_ptr = list_ptr->next;
  }
  /*
  cout << "KT: WARNING: obj id not found in Alloc_Obj_List::getInfo ... checking check list" << endl;
  // check check_alloc list
  for(int i = 0; i<profiler.check_allocList.getListSize(); i++) {
    if(id == profiler.check_allocList.getId(i)) {
      cout << "KT found obj id in check list " << endl;
    }
  }
  cout << "KT did not find obj id in check list " << endl;
  */
  return NULL;
}

void Alloc_Obj_List::remove(jobjectID id) {
  // needs to be hash table
  int idx = hash(id);
  if(allocList[idx] == NULL) {
    barf("null allocList - id not found");
  }
  if(allocList[idx]->objId == id) { 
    allocList[idx] = allocList[idx]->next;
    return;
  }

  Obj_Info* prev_ptr = allocList[idx];
  Obj_Info* list_ptr = allocList[idx]->next;
  while(list_ptr != NULL) {
    if(list_ptr->objId == id) {
      Obj_Info* to_delete = list_ptr;
      prev_ptr->next = list_ptr->next;
      delete list_ptr;
      return;
    }
    prev_ptr = prev_ptr->next;
    list_ptr = list_ptr->next;
  }
  barf("KT: obj id not found in Alloc_Obj_List::remove");
}

void Alloc_Obj_List::reset() {
  for(int i = 0; i<HASH_TABLE_SIZE; i++) {
      Obj_Info* nextPtr = allocList[i];
      Obj_Info* delPtr;
      while(nextPtr != NULL) {
	delPtr = nextPtr;
	nextPtr = nextPtr->next;
	delete delPtr;
      }
      allocList[i] = NULL;
  }
}

int Alloc_Obj_List::hash(jobjectID id) {
  return (unsigned long)id % HASH_TABLE_SIZE;
}
