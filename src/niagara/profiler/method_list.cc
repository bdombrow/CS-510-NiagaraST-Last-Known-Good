#include <method_list.h>
#include <np_funcs.h>

#include <string.h>
#include <iostream.h>

Method_List::Method_List() {
  numMAlloc = 1000;
  numMethods = 0;
  methods = new Method_Info[numMAlloc];
}

Method_List::~Method_List() {
  delete []methods;
}

void Method_List::addClassMethods(JVMPI_Event* event) {

  //cout << "KT: loading class " << event->u.class_load.class_name;
  //cout << " num methods " << event->u.class_load.num_methods << endl;

  for(int i = 0; i<event->u.class_load.num_methods; i++) {
    addMethod(event->u.class_load.class_name, &event->u.class_load.methods[i]);
  }
}


void Method_List::addMethod(const char* const _sourceFile, 
			    const JVMPI_Method* const method) {
  checkSpace();
  if(_sourceFile == NULL) {
    methods[numMethods].sourceFile = new char[8];
    strcpy(methods[numMethods].sourceFile, "Unknown");
  } else {
    methods[numMethods].sourceFile = new char[strlen(_sourceFile)+1];
    strcpy(methods[numMethods].sourceFile, _sourceFile);
  }

  if(method->method_name == NULL) {
    methods[numMethods].name = new char[8];
    strcpy(methods[numMethods].name, "Unknown");
  } else {
    methods[numMethods].name = new char[strlen(method->method_name)+1];
    strcpy(methods[numMethods].name, method->method_name);
  }
  methods[numMethods].id = (long)method->method_id;
  numMethods++;
  return;
}

const Method_Info* Method_List::getMethodInfo(jmethodID id) const {
  for(int i = 0; i<numMethods; i++) {
    if(methods[i].id == (long)id) // hprof cast to long and subtracted
      return &methods[i];
  }
  return NULL;
}


// --------------------- PRIVATE METHODS --------------------------
void Method_List::checkSpace() {
  if(numMethods < numMAlloc) {
    return;
  }
  int oldCount = numMAlloc;
  numMAlloc *= 2;
  Method_Info* newMethods = new Method_Info[numMAlloc];

  for(int i = 0; i<oldCount; i++) {
    newMethods[i].id = methods[i].id;
    newMethods[i].sourceFile = methods[i].sourceFile;
    newMethods[i].name = methods[i].name;
  }
  delete []methods;
  methods = newMethods;
}


/*
void Method_List::checkCSpace() {
  if(numClasses < numCAlloc) {
    return;
  }
  int oldCount = numCAlloc;
  numCAlloc *= 2;
  Class_Methods** newCM = new Class_Methods*[numCAlloc];

  int newNumClasses = 0;
  for(int i = 0; i<oldCount; i++) {
    if(classMethods[i] != NULL) {
      newCM[newNumClasses] = classMethods[i];
      newNumClasses++;
    }
  }
  //delete []classMethods;
  //classMethods = newCM;
  numClasses = newNumClasses;
}
*/
/*
void Method_List::removeClassMethods(jobjectID removeId) {
  // look in hprof_objmap_lookup (hprof_object.c in hprof) for hints handling jobjectIDs
  for(int i = 0; i<numClasses; i++) {
    if(classMethods[i] != NULL && classMethods[i]->classId == removeId) {
      // found the class, set associated methods to null
      int start = classMethods[i]->startIdx;
      for(int j = 0; j<classMethods[i]->numMethods; j++) {
	ids[start+j] = NULL;
	delete []methods[start+j].name;
	delete []methods[start+j].sourceFile;
	methods[start+j].name = NULL;
	methods[start+j].sourceFile = NULL;
      }
      classMethods[i] = NULL;
      return;
    }
  }
  barf("WARNING: Unable to remove class");
}
*/
