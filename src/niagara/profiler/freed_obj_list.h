#ifndef FREED_OBJ_LIST_H
#define FREED_OBJ_LIST_H

#include <jvmpi.h>

class Freed_Obj_List {
 private:
  jobjectID* freed_list;
  bool* removed;
  int listSize;
  int allocSize;
  
 public:

  Freed_Obj_List();
  void add(jobjectID id);
  int getListSize();
  jobjectID getId(int idx);
  void remove(int idx);
  void reset() {listSize = 0;};

 private:
  void checkSpace();
};



#endif
