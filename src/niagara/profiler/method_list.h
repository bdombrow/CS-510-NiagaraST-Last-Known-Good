#ifndef METHOD_LIST_H
#define METHOD_LIST_H

#include <jvmpi.h>

class Method_Info {
 public:
  char* name;
  char* sourceFile;
  long id;
};

class Method_List {
 public:
  Method_List();
  ~Method_List();
  void addClassMethods(JVMPI_Event* event);
  const Method_Info* getMethodInfo(jmethodID id) const;

 private:
  Method_Info* methods;
  int numMethods;
  int numMAlloc;
  void addMethod(const char* const _sourceFile, 
		 const JVMPI_Method* const method);
  void checkSpace();
};

#endif // METHOD_LIST_H
