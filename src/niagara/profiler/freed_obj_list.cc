#include <freed_obj_list.h>
#include <np_funcs.h>

Freed_Obj_List::Freed_Obj_List() {
  listSize = 0;
  allocSize = 1000;
  freed_list = new jobjectID[allocSize];
  removed = new bool[allocSize];
}

void Freed_Obj_List::add(jobjectID id) {
  checkSpace();
  freed_list[listSize] = id;
  removed[listSize] = false;
  listSize++;
}

int Freed_Obj_List::getListSize() {
  return listSize;
}

jobjectID Freed_Obj_List::getId(int idx) {
  if(idx >= listSize)
    barf("bad id in Freed_Obj_List::getId");
  
  return freed_list[idx];
}

void Freed_Obj_List::remove(int idx) {
  if(idx >= listSize)
    barf("bad id in Freed_Obj_List::remove");
  removed[idx] = true;
}


void Freed_Obj_List::checkSpace() {
  if(listSize < allocSize) 
    return;
  
  allocSize *=2;
  jobjectID* new_freed_list = new jobjectID[allocSize];
  bool* new_removed = new bool[allocSize];
  
  int j = 0;
  for(int i = 0; i<listSize; i++) {
    if(!removed[i]) {
      new_freed_list[j] = freed_list[i];
      new_removed[j] = false;
      j++;
    }
    // else skip item
  }
  delete []removed;
  removed = new_removed;
  delete []freed_list;
  freed_list = new_freed_list;
}
