#ifndef NP_FUNCS_H
#define NP_FUNCS_H

#include <stdlib.h>
#include <iostream>

using namespace std;

static void barf(char* message) {
  if(message != NULL)
    cerr << message << endl;
  exit(-1);  // BAD - this uses the port number - should throw exception!
}

#endif // NP_FUNCS_H
