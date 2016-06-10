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

#ifndef CLUSTER_STATE_H_
#define CLUSTER_STATE_H_

#include "mds/FSMap.h"
#include "common/Mutex.h"

#include "osdc/Objecter.h"
#include "mon/MonClient.h"


/**
 * Cluster-scope state (things like cluster maps) as opposed
 * to daemon-level state (things like perf counters and smart)
 */
class ClusterState
{
protected:
  MonClient *monc;
  Objecter *objecter;
  FSMap fsmap;
  Mutex lock;

public:

  ClusterState(MonClient *monc_, Objecter *objecter_);

  void set_objecter(Objecter *objecter_);
  void set_fsmap(FSMap const &new_fsmap);

  template<typename Callback, typename...Args>
  void with_fsmap(Callback&& cb, Args&&...args)
  {
  Mutex::Locker l(lock);
  std::forward<Callback>(cb)(const_cast<const FSMap&>(fsmap),
      std::forward<Args>(args)...);
  }

  template<typename Callback, typename...Args>
  void with_monmap(Callback&& cb, Args&&...args)
  {
    Mutex::Locker l(lock);
    assert(monc != nullptr);
    monc->with_monmap(cb);
  }

  template<typename Callback, typename...Args>
  void with_osdmap(Callback&& cb, Args&&...args)
  {
    Mutex::Locker l(lock);
    assert(objecter != nullptr);
    objecter->with_osdmap(cb);
  }
};

#endif

