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

#ifndef PY_MODULES_H_
#define PY_MODULES_H_

#include "common/Finisher.h"
#include "common/Mutex.h"


#include "DaemonMetadata.h"
#include "ClusterState.h"

#include "MgrPyModule.h"

class PyModules
{
  protected:
  std::list<MgrPyModule*> modules;

  DaemonMetadataIndex &daemon_state;
  ClusterState &cluster_state;
  MonClient &monc;
  Finisher &finisher;

  Mutex lock;

public:
  PyModules(DaemonMetadataIndex &ds, ClusterState &cs, MonClient &mc,
            Finisher &f)
    : daemon_state(ds), cluster_state(cs), monc(mc), finisher(f),
      lock("PyModules")
  {
  }

  // FIXME: wrap for send_command?
  MonClient &get_monc() {return monc;}

  PyObject *get_python(const std::string &what);
  PyObject *get_server_python(const std::string &hostname);
  PyObject *list_servers_python();

  std::map<std::string, std::string> config_cache;

  void insert_config(const std::map<std::string, std::string> &new_config)
  {
    config_cache = new_config;
  }

  // Public so that MonCommandCompletion can use it
  // FIXME: bit weird that we're sending command completions
  // to all modules (we rely on them to ignore anything that
  // they don't recognise), but when we get called from
  // python-land we don't actually know who we are.  Need
  // to give python-land a handle in initialisation.
  void notify_all(const std::string &notify_type,
                  const std::string &notify_id);

  int main(std::vector<const char *> args);

  void dump_server(const std::string &hostname,
                   const DaemonMetadataCollection &dmc,
                   Formatter *f);

  bool get_config(const std::string &key, std::string *val) const;
  void set_config(const std::string &key, const std::string &val);
};

#endif

