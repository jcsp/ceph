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


#include "MgrClient.h"

#include "messages/MMgrMap.h"

#define dout_subsys ceph_subsys_mgrc
#undef dout_prefix
#define dout_prefix *_dout << "mgrc " << __func__ << " "

MgrClient::MgrClient(Messenger *msgr_)
    : Dispatcher(g_ceph_context), msgr(msgr_)
{
  assert(msgr != nullptr);
}

bool MgrClient::ms_dispatch(Message *m)
{
  dout(20) << *m << dendl;
  switch(m->get_type()) {
  case MSG_MGR_MAP:
    return handle_mgr_map(static_cast<MMgrMap*>(m));
  case MSG_MGR_OPEN:
    m->put();
    return true;
  default:
    dout(10) << "Not handling " << *m << dendl; 
    return false;
  }
}

bool MgrClient::handle_mgr_map(MMgrMap *m)
{
  map = m->get_map();
  dout(4) << "Got map version " << map.epoch << dendl;
  m->put();

  dout(4) << "Active mgr is now " << map.get_active() << dendl;

  // TODO: is the map active addr different to the one
  // for our current session?  If so, nuke the session
  // and start again.

  return true;
}

bool MgrClient::ms_handle_reset(Connection *con)
{
#if 0
  Mutex::Locker lock(monc_lock);

  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
    if (cur_mon.empty() || con != cur_con) {
      ldout(cct, 10) << "ms_handle_reset stray mon " << con->get_peer_addr() << dendl;
      return true;
    } else {
      ldout(cct, 10) << "ms_handle_reset current mon " << con->get_peer_addr() << dendl;
      if (hunting)
	return true;
      
      ldout(cct, 0) << "hunting for new mon" << dendl;
      _reopen_session();
    }
  }
  return false;
#else
  return true;
#endif
}
