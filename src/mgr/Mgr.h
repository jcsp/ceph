// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 John Spray <john.spray@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef CEPH_PYFOO_H_
#define CEPH_PYFOO_H_

// Python.h comes first because otherwise it clobbers ceph's assert
#include "Python.h"
// Python's pyconfig-64.h conflicts with ceph's acconfig.h
#undef HAVE_SYS_WAIT_H
#undef HAVE_UNISTD_H
#undef HAVE_UTIME_H
#undef _POSIX_C_SOURCE
#undef _XOPEN_SOURCE

#include "osdc/Objecter.h"
#include "mds/FSMap.h"
#include "messages/MFSMap.h"
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"
#include "auth/Auth.h"
#include "common/Finisher.h"
#include "common/Timer.h"
#include "DaemonMetadata.h"
#include "DaemonServer.h"

class MCommand;


class MgrPyModule;

class Mgr : public Dispatcher {
protected:
  Objecter *objecter;
  FSMap *fsmap;
  Messenger *client_messenger;

public:
  // FIXME just exposing this for the moment
  // for ceph_send_command
  MonClient *monc;

  // Public so that MonCommandCompletion can use it
  // FIXME: bit weird that we're sending command completions
  // to all modules (we rely on them to ignore anything that
  // they don't recognise), but when we get called from
  // python-land we don't actually know who we are.  Need
  // to give python-land a handle in initialisation.
  void notify_all(const std::string &notify_type,
                  const std::string &notify_id);

protected:

  Mutex lock;
  SafeTimer timer;
  Finisher finisher;

  Context *waiting_for_fs_map;

  std::list<MgrPyModule*> modules;

  DaemonMetadataIndex dmi;

  void load_all_metadata();

  DaemonServer server;

  std::map<std::string, std::string> config_cache;

  void load_config();

public:
  Mgr();
  ~Mgr();

  void handle_fs_map(MFSMap* m);
  void handle_osd_map();
  bool ms_dispatch(Message *m);
  bool ms_handle_reset(Connection *con) { return false; }
  void ms_handle_remote_reset(Connection *con) {}
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
                         bool force_new);
  int init();
  void shutdown();
  void usage() {}
  int main(vector<const char *> args);

  void dump_server(const std::string &hostname,
                   const DaemonMetadataCollection &dmc,
                   Formatter *f);
  PyObject *get_python(const std::string &what);
  PyObject *get_server_python(const std::string &hostname);
  PyObject *list_servers_python();

  bool get_config(const std::string &key, std::string *val) const;
  void set_config(const std::string &key, const std::string &val);

  void handle_command(MCommand *m);
};

#endif /* MDS_UTILITY_H_ */
