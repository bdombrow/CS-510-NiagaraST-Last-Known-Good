#ifndef ALLOC_OBJ_LIST_H
#define ALLOC_OBJ_LIST_H

#include <jvmpi.h>

class Obj_Info {
 public:
  jobjectID objId;
  int objSize;
  int threadNum;
  int traceNum;
  Obj_Info* next;

  Obj_Info(jobjectID _objId, int _objSize, int _threadNum, int _traceNum);
};

class Alloc_Obj_List {
 private:
  Obj_Info** allocList;
  const static int HASH_TABLE_SIZE = 200003;
 public:

  Alloc_Obj_List();
  void add(jobjectID id, int size, int threadNum, int traceNum);
  Obj_Info* getInfo(jobjectID);
  void remove(jobjectID);
  void reset();

 private:
  int hash(jobjectID id);
};



#endif
