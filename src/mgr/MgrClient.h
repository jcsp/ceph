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

#ifndef MGR_CLIENT_H_
#define MGR_CLIENT_H_

#include "msg/Dispatcher.h"
#include "mon/MgrMap.h"

class MMgrMap;
class Messenger;

class MgrSessionState
{
};

class MgrClient : public Dispatcher
{
protected:
  MgrMap map;
  Messenger *msgr;

public:
  MgrClient(Messenger *msgr_);

  bool ms_dispatch(Message *m);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con) {}

  bool handle_mgr_map(MMgrMap *m);
};

#endif

