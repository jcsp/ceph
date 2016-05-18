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

class MgrSessionState
{
};

class MgrClient : public Dispatcher
{
    /**
     * Pretty simple protocol, layered on top of a lossless pipe.
     *
     * Consume mgrmap-esque thing, that tells me who to talk to.
     * When I see the map change, my session ends and I start
     * a new session.
     *
     * When I start a session, I transmit a hello message with
     * my perf counter schema.
     *
     * I wait for a hello message from the mgr that will tell
     * me how frequently he wants me to send him stats.
     *
     * Then I just sit there sending him stats every N seconds.
     */

  MgrClient(Messenger *msgr_)
      : msgr(msgr_)
  {
    assert(msgr != nullptr);
  }


  bool ms_dispatch(Message *m);
  {
  }
};

#endif

