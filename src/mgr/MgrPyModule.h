
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


#ifndef MGR_PY_MODULE_H_
#define MGR_PY_MODULE_H_

// Python.h comes first because otherwise it clobbers ceph's assert
#include "Python.h"

#include <string>

class MgrPyModule
{
private:
  const std::string module_name;
  PyObject *pModule;
  PyObject *pServe;
  PyObject *pNotify;

public:
  MgrPyModule(const std::string &module_name);
  ~MgrPyModule();

  int load();
  int serve();
  void notify(const std::string &notify_type, const std::string &notify_id);
};

#endif

