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
#include "messages/MMgrOpen.h"
#include "messages/MMgrConfigure.h"

#define dout_subsys ceph_subsys_mgrc
#undef dout_prefix
#define dout_prefix *_dout << "mgrc " << __func__ << " "

MgrClient::MgrClient(Messenger *msgr_)
    : Dispatcher(g_ceph_context), msgr(msgr_), lock("mgrc"),
      timer(g_ceph_context, lock)
{
  assert(msgr != nullptr);
}

void MgrClient::init()
{
  timer.init();
}

void MgrClient::shutdown()
{
  timer.shutdown();
}

bool MgrClient::ms_dispatch(Message *m)
{
  Mutex::Locker l(lock);

  dout(20) << *m << dendl;
  switch(m->get_type()) {
  case MSG_MGR_MAP:
    return handle_mgr_map(static_cast<MMgrMap*>(m));
  case MSG_MGR_CONFIGURE:
    return handle_mgr_configure(static_cast<MMgrConfigure*>(m));
  default:
    dout(10) << "Not handling " << *m << dendl; 
    return false;
  }
}

bool MgrClient::handle_mgr_map(MMgrMap *m)
{
  assert(lock.is_locked_by_me());

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

  auto open = new MMgrOpen();
  open->daemon_name = g_conf->name.get_id();
  session->con->send_message(open);

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



void MgrClient::send_report()
{
  assert(lock.is_locked_by_me());
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
    for (unsigned int i = 0; i < l->m_data.size(); ++i) {
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

  if (stats_period != 0) {
    auto c = new C_StdFunction([this](){send_report();});
    timer.add_event_after(stats_period, c);
  }
}

bool MgrClient::handle_mgr_configure(MMgrConfigure *m)
{
  assert(lock.is_locked_by_me());

  dout(4) << "stats_period=" << m->stats_period << dendl;

  bool starting = (stats_period == 0) && (m->stats_period != 0);
  stats_period = m->stats_period;
  if (starting) {
    send_report();
  }

  m->put();
  return true;
}

