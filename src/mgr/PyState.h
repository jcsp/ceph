#ifndef PYSTATE_H_
#define PYSTATE_H_

#include "Python.h"

class Mgr;

extern Mgr *global_handle;
extern PyMethodDef CephStateMethods[];

#endif

