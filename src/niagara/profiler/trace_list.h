#ifndef TRACE_LIST_H
#define TRACE_LIST_H

#include <iostream.h>
#include <jvmpi.h>
#include <method_list.h>

class Trace_List {

  class List_Elem {
  public: 
    JVMPI_CallTrace* trace;
    long memAllocd;
    long freedMem;
    int traceNum;
    int numAllocs;
  };

 public:
  Trace_List(const Method_List* const _methodList, int _threadId);
  ~Trace_List();
  void addAlloc(JVMPI_CallTrace* trace, int bytes, char* threadName);
  void print(ostream& os, char* threadName);
  int  getTotalMem();
  int  getLiveMem();
  void resetData();
  void processFreedObjList();
  int  getMostRecentTrace();
  int getNumTraces() {return numTraces;}
  int threadId;

 private:
  int numTraces;
  int numAlloc;
  int mostRecentTrace;
  List_Elem** traceList;
  const Method_List* methodList;

  void printTrace(const JVMPI_CallTrace* const trace, ostream& os);
  int traceEquals(const JVMPI_CallTrace* const trace1,
			  const JVMPI_CallTrace* const trace2);
  JVMPI_CallTrace* copyTrace(const JVMPI_CallTrace* const trace);
  void checkSpace();
  void sortTraces();
  void quicksort(List_Elem** array, int start, int end);
  void insertionsort(List_Elem** array, int start, int end);
};

#endif
