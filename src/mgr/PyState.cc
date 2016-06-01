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
  PyObject *python_completion;
  const std::string tag;

public:
  std::string outs;
  bufferlist outbl;

  MonCommandCompletion(PyObject* ev, const std::string &tag_)
    : python_completion(ev), tag(tag_)
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

    global_handle->notify_all("command", tag);
  }
};


static PyObject*
ceph_send_command(PyObject *self, PyObject *args)
{
  char *cmd_json = nullptr;
  char *tag = nullptr;
  PyObject *completion = nullptr;
  if (!PyArg_ParseTuple(args, "Oss:ceph_send_command",
        &completion, &cmd_json, &tag)) {
    return nullptr;
  }

  auto set_fn = PyObject_GetAttrString(completion, "complete");
  if (set_fn == nullptr) {
    assert(0);  // TODO raise python exception instead
  } else {
    assert(PyCallable_Check(set_fn));
  }
  Py_DECREF(set_fn);

  auto c = new MonCommandCompletion(completion, tag);
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


static PyObject*
ceph_get_server(PyObject *self, PyObject *args)
{
  char *hostname = NULL;
  if (!PyArg_ParseTuple(args, "z:ceph_get_server", &hostname)) {
    return NULL;
  }

  if (hostname) {
    return global_handle->get_server_python(hostname);
  } else {
    return global_handle->list_servers_python();
  }
}

static PyObject*
ceph_config_get(PyObject *self, PyObject *args)
{
  char *what = nullptr;
  if (!PyArg_ParseTuple(args, "s:ceph_state_get", &what)) {
    return nullptr;
  }

  std::string value;
  bool found = global_handle->get_config(what, &value);
  if (found) {
    return PyString_FromString(value.c_str());
  } else {
    Py_RETURN_NONE;
  }
}

static PyObject*
ceph_config_set(PyObject *self, PyObject *args)
{
  char *key = nullptr;
  char *value = nullptr;
  if (!PyArg_ParseTuple(args, "ss:ceph_state_get", &key, &value)) {
    return nullptr;
  }

  global_handle->set_config(key, value);

  Py_RETURN_NONE;
}

PyMethodDef CephStateMethods[] = {
    {"get", ceph_state_get, METH_VARARGS,
     "Get a cluster object"},
    {"get_server", ceph_get_server, METH_VARARGS,
     "Get a server object"},
    {"send_command", ceph_send_command, METH_VARARGS,
     "Send a mon command"},
    {"get_config", ceph_config_get, METH_VARARGS,
     "Get a configuration value"},
    {"set_config", ceph_config_set, METH_VARARGS,
     "Set a configuration value"},
    {NULL, NULL, 0, NULL}
};

