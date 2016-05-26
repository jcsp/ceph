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

#include "msg/Messenger.h"
#include "messages/MMgrMap.h"
#include "messages/MMgrReport.h"

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

  dout(4) << "Active mgr is now " << map.get_active_addr() << dendl;

  entity_inst_t inst;
  inst.addr = map.get_active_addr();
  inst.name = entity_name_t::MGR(map.get_active_gid());

  if (session == nullptr || 
      session->con->get_peer_addr() != map.get_active_addr()) {
    delete session;
    session = new MgrSessionState();
    session->con = msgr->get_connection(inst);
  }

  send_report();

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

void MgrClient::send_report()
{
  assert(session);

  auto report = new MMgrReport();
  auto pcc = g_ceph_context->get_perfcounters_collection();

  // TODO: keep a session flag that tells us we've sent schema
  // for all counters, so that we don't have to do N set::set lookups
  // when sending.
  
  // TODO: refactor perf counter collections and/or this
  // code so that this structure isn't needed.
  // Map type path to data
  std::map<std::string, PerfCounters::perf_counter_data_any_d *> all; 

  for (auto l : pcc->m_loggers) {
    // FIXME: dropping this lock while keeping ptrs
    // to the data isn't exactly safe.
    Mutex::Locker collection_lock(l->m_lock);
    for (int i = 0; i < l->m_data.size(); ++i) {
      PerfCounters::perf_counter_data_any_d &data = l->m_data[i];

      std::string path = l->get_name();
      path += ".";
      path += data.name;

      assert(all.count(path) == 0);
      all[path] = &data;

      if (session->declared.count(path) == 0) {
        PerfCounterType type;
        type.path = path;
        if (data.description) {
          type.description = data.description;
        }
        if (data.nick) {
          type.nick = data.nick;
        }
        type.type = data.type;
        report->declare_types.push_back(std::move(type));
        session->declared.insert(path);
      }
    }
  }

  dout(20) << all.size() << " counters, of which "
           << report->declare_types.size() << " new" << dendl;

  ENCODE_START(1, 1, report->packed);
  for (const auto &path : session->declared) {
    auto data = all.at(path);
    ::encode(static_cast<uint64_t>(data->u64.read()),
        report->packed);
    if (data->type & PERFCOUNTER_LONGRUNAVG) {
      ::encode(static_cast<uint64_t>(data->avgcount.read()),
          report->packed);
      ::encode(static_cast<uint64_t>(data->avgcount2.read()),
          report->packed);
    }
  }
  ENCODE_FINISH(report->packed);

  dout(20) << "encoded " << report->packed.length() << " bytes" << dendl;

  report->daemon_name = g_conf->name.get_id();

  session->con->send_message(report);
}

