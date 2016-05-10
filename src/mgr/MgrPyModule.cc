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


#include "MgrPyModule.h"

MgrPyModule::MgrPyModule(const std::string &module_name_)
  : module_name(module_name_), pModule(nullptr), pServe(nullptr),
  pNotify(nullptr)
{}

MgrPyModule::~MgrPyModule()
{
  Py_XDECREF(pModule);
  Py_XDECREF(pServe);
  Py_XDECREF(pNotify);
}

int MgrPyModule::load()
{
  PyObject *pName = PyString_FromString(module_name.c_str());
  pModule = PyImport_Import(pName);
  Py_DECREF(pName);

  if (pModule  == nullptr) {
    PyErr_Print();
    return -EINVAL;
  }

  pServe = PyObject_GetAttrString(pModule, "serve");
  if (pServe == nullptr || !PyCallable_Check(pServe)) {
    if (PyErr_Occurred())
      PyErr_Print();
    return -EINVAL;
  }

  pNotify = PyObject_GetAttrString(pModule, "notify");
  if (pNotify == nullptr || !PyCallable_Check(pNotify)) {
    if (PyErr_Occurred())
      PyErr_Print();
    return -EINVAL;
  }

  return 0;
}

int MgrPyModule::serve()
{
  assert(pServe != nullptr);
  PyObject *pArgs = nullptr;
  PyObject *pValue = nullptr;

  PyGILState_STATE gstate;
  gstate = PyGILState_Ensure();

  pArgs = PyTuple_New(0);
  pValue = PyObject_CallObject(pServe, pArgs);
  Py_DECREF(pArgs);
  if (pValue != NULL) {
    Py_DECREF(pValue);
  } else {
    PyErr_Print();
    return -EINVAL;
  }

  PyGILState_Release(gstate);

  return 0;
}

void MgrPyModule::notify(const std::string &notify_type, const std::string &notify_id)
{
  assert(pServe != nullptr);

  PyGILState_STATE gstate;
  gstate = PyGILState_Ensure();

  // Compose args
  auto pyType = PyString_FromString(notify_type.c_str());
  auto pyId = PyString_FromString(notify_id.c_str());
  auto pArgs = PyTuple_Pack(2, pyType, pyId);

  // Execute
  auto pValue = PyObject_CallObject(pNotify, pArgs);

  Py_DECREF(pArgs);
  if (pValue != NULL) {
    Py_DECREF(pValue);
  } else {
    PyErr_Print();
    // FIXME: callers can't be expected to handle a python module
    // that has spontaneously broken, but Mgr() should provide
    // a hook to unload misbehaving modules when they have an
    // error somewhere like this
  }

  PyGILState_Release(gstate);
}

