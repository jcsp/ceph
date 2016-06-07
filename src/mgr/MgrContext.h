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

#ifndef MGR_CONTEXT_H_
#define MGR_CONTEXT_H_

#include <memory>
#include "include/Context.h"

class C_StdFunction : public Context
{
private:
  std::function<void()> fn;

public:
  C_StdFunction(std::function<void()> fn_)
    : fn(fn_)
  {}

  void finish(int r)
  {
    fn();
  }
};

#endif

