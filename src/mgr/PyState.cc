// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

/**
 * The interface we present to python code that runs within
 * ceph-mgr.
 */

#include "Mgr.h"

#include "mon/MonClient.h"

#include "PyState.h"

Mgr *global_handle = NULL;


class MonCommandCompletion : public Context
{
public:
  std::string outs;
  bufferlist outbl;
  PyObject *python_completion;

  MonCommandCompletion(PyObject* ev)  
    : python_completion(ev)
  {
    assert(python_completion != nullptr);
    Py_INCREF(python_completion);
  }

  ~MonCommandCompletion()
  {
    Py_DECREF(python_completion);
  }

  void finish(int r)
  {
    PyGILState_STATE gstate;
    gstate = PyGILState_Ensure();

    auto set_fn = PyObject_GetAttrString(python_completion, "complete");
    assert(set_fn != nullptr);

    auto pyR = PyInt_FromLong(r);
    auto pyOutBl = PyString_FromString(outbl.to_str().c_str());
    auto pyOutS = PyString_FromString(outs.c_str());
    auto args = PyTuple_Pack(3, pyR, pyOutBl, pyOutS);
    Py_DECREF(pyR);
    Py_DECREF(pyOutBl);
    Py_DECREF(pyOutS);

    auto rtn = PyObject_CallObject(set_fn, args);
    if (rtn != nullptr) {
      Py_DECREF(rtn);
    }
    Py_DECREF(args);

    PyGILState_Release(gstate);
  }
};


static PyObject*
ceph_send_command(PyObject *self, PyObject *args)
{
  char *cmd_json = nullptr;
  PyObject *completion = nullptr;
  if (!PyArg_ParseTuple(args, "Os:ceph_send_command",
        &completion, &cmd_json)) {
    return nullptr;
  }

  auto set_fn = PyObject_GetAttrString(completion, "complete");
  if (set_fn == nullptr) {
    assert(0);  // TODO raise python exception instead
  } else {
    assert(PyCallable_Check(set_fn));
  }
  Py_DECREF(set_fn);

  auto c = new MonCommandCompletion(completion);
  auto r = global_handle->monc->start_mon_command(
      {cmd_json},
      {},
      &c->outbl,
      &c->outs,
      c);
  assert(r == 0);  // start_mon_command is forbidden to fail

  Py_RETURN_NONE;
}

static PyObject*
ceph_state_get(PyObject *self, PyObject *args)
{
  char *what = NULL;
  if (!PyArg_ParseTuple(args, "s:ceph_state_get", &what)) {
    return NULL;
  }

  return global_handle->get_python(what);
}

PyMethodDef CephStateMethods[] = {
    {"get", ceph_state_get, METH_VARARGS,
     "Get a cluster object"},
    {"send_command", ceph_send_command, METH_VARARGS,
     "Send a mon command"},
    {NULL, NULL, 0, NULL}
};

// When I want to call into the python code to notify it,
// I guess I just have to take GIL first, that's okay.

// When the python code wants to do something synchronous,
// calling into C++, that's okay too, but the C++ code has
// to explicitly drop the GIL so that other python code
// can proceed.

// How should the python code handle doing e.g. a send_command?
//  The API calls want to hand this kind of thing off from the
//  HTTP handler to a Request worker.  We end up with essentially
//  an intra-process RPC from the HTTP request handler loop into
//  another loop servicing RPCs.
//
//  Should we let the Request worker block?  I don't see why not?
//  Other than that it is kind of uncomfortable for that code which
//  was written for salt jobs; all the existing calamari code expects
//  to emit a command and then receive a message later when it completes.
//
// If the python code is using gevent, then releasing the GIL isn't
// sufficient, right?  We need to let the python interpreter
// resume execution.  Do we need python-side wrapper functions?
//
// Hmm, in Calamari my remote stubs would do their command, then
// return the map epoch that I need in order to see the result
// of their action.  Now, I guess I can go ahead and do a
// wait_for_latest equivalent down in my MonClient, although
// that's not really the same thing.  Maybe I should make
// the MonCommand interface return cluster map epochs?
