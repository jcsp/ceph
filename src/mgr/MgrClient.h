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

#include "msg/Connection.h"

#include "common/perf_counters.h"
#include "common/Timer.h"

class MMgrMap;
class MMgrConfigure;
class Messenger;

class MgrSessionState
{
  public:
  // Which performance counters have we already transmitted schema for?
  std::set<std::string> declared;

  // Our connection to the mgr
  ConnectionRef con;
};

class MgrClient : public Dispatcher
{
protected:
  MgrMap map;
  Messenger *msgr;

  MgrSessionState *session;

  Mutex lock;

  uint32_t stats_period;
  SafeTimer     timer;


public:
  MgrClient(Messenger *msgr_);

  void init();
  void shutdown();

  bool ms_dispatch(Message *m);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con) {}

  bool handle_mgr_map(MMgrMap *m);
  bool handle_mgr_configure(MMgrConfigure *m);

  void send_report();
};

#endif

