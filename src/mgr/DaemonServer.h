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

#ifndef DAEMON_SERVER_H_
#define DAEMON_SERVER_H_

#include <msg/Messenger.h>
#include <mon/MonClient.h>

#include "auth/AuthAuthorizeHandler.h"

// For PerfCounterType
#include "messages/MMgrReport.h"

// For DaemonKey (move it somewhere else?)
#include "DaemonMetadata.h"

class MMgrReport;

typedef std::map<std::string, PerfCounterType> PerfCounterTypes;

class PerfCounterInstance
{
  // TODO: store some short history or whatever
  uint64_t current;
  public:
  void push(uint64_t const &v) {current = v;}
};

class DaemonPerfCounters
{
  public:
  // The record of perf stat types, shared between daemons
  PerfCounterTypes &types;

  DaemonPerfCounters(PerfCounterTypes &types_)
    : types(types_)
  {}

  std::map<std::string, PerfCounterInstance> instances;
  std::set<std::string> declared_types;

  void update(MMgrReport *report);
};

/**
 * Server used in ceph-mgr to communicate with Ceph daemons like
 * MDSs and OSDs.
 */
class DaemonServer : public Dispatcher
{
  Messenger *msgr;
  MonClient *monc;

  AuthAuthorizeHandlerRegistry auth_registry;

public:
  int init(uint64_t gid, entity_addr_t client_addr);
  void shutdown();

  entity_addr_t get_myaddr() const;

  DaemonServer(MonClient *monc_);
  ~DaemonServer();

  bool ms_dispatch(Message *m);
  bool ms_handle_reset(Connection *con) { return false; }
  void ms_handle_remote_reset(Connection *con) {}
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
                         bool force_new);
  bool ms_verify_authorizer(Connection *con,
      int peer_type,
      int protocol,
      ceph::bufferlist& authorizer,
      ceph::bufferlist& authorizer_reply,
      bool& isvalid,
      CryptoKey& session_key);

  bool handle_report(MMgrReport *m);

  PerfCounterTypes types;
  std::map<DaemonKey, std::shared_ptr<DaemonPerfCounters> > perf_counters;
};

#endif

