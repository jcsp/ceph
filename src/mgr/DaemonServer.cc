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

#include "DaemonServer.h"

#include "messages/MMgrOpen.h"
#include "messages/MMgrConfigure.h"

#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr.server " << __func__ << " "

DaemonServer::DaemonServer(MonClient *monc_)
    : Dispatcher(g_ceph_context), msgr(nullptr), monc(monc_),
      auth_registry(g_ceph_context,
                    g_conf->auth_supported.empty() ?
                      g_conf->auth_cluster_required :
                      g_conf->auth_supported),
      lock("DaemonServer")
{}

DaemonServer::~DaemonServer() {
  delete msgr;
}

int DaemonServer::init(uint64_t gid, entity_addr_t client_addr)
{
  // Initialize Messenger
  msgr = Messenger::create(g_ceph_context, g_conf->ms_type,
      entity_name_t::MGR(gid), "server", getpid());
  int r = msgr->bind(g_conf->public_addr);
  if (r < 0)
    return r;

  msgr->set_myname(entity_name_t::MGR(gid));
  msgr->set_addr_unknowns(client_addr);

  msgr->start();
  msgr->add_dispatcher_tail(this);

  return 0;
}

entity_addr_t DaemonServer::get_myaddr() const
{
  return msgr->get_myaddr();
}


bool DaemonServer::ms_verify_authorizer(Connection *con,
    int peer_type,
    int protocol,
    ceph::bufferlist& authorizer_data,
    ceph::bufferlist& authorizer_reply,
    bool& is_valid,
    CryptoKey& session_key)
{
  auto handler = auth_registry.get_handler(protocol);
  if (!handler) {
    dout(0) << "No AuthAuthorizeHandler found for protocol " << protocol << dendl;
    assert(0);
    is_valid = false;
    return true;
  }

  AuthCapsInfo caps_info;
  EntityName name;
  uint64_t global_id = 0;

  is_valid = handler->verify_authorizer(cct, monc->rotating_secrets,
						  authorizer_data,
                                                  authorizer_reply, name,
                                                  global_id, caps_info,
                                                  session_key);

  // TODO: invent some caps suitable for ceph-mgr

  return true;
}


bool DaemonServer::ms_get_authorizer(int dest_type,
    AuthAuthorizer **authorizer, bool force_new)
{
  dout(10) << "type=" << ceph_entity_type_name(dest_type) << dendl;

  if (dest_type == CEPH_ENTITY_TYPE_MON) {
    return true;
  }

  if (force_new) {
    if (monc->wait_auth_rotating(10) < 0)
      return false;
  }

  *authorizer = monc->auth->build_authorizer(dest_type);
  dout(20) << "got authorizer " << *authorizer << dendl;
  return *authorizer != NULL;
}


bool DaemonServer::ms_dispatch(Message *m)
{
  switch(m->get_type()) {
    case MSG_MGR_REPORT:
      return handle_report(static_cast<MMgrReport*>(m));
    case MSG_MGR_OPEN:
      return handle_open(static_cast<MMgrOpen*>(m));
    default:
      dout(1) << "Unhandled message type " << m->get_type() << dendl;
      return false;
  };
}

void DaemonServer::shutdown()
{
  msgr->shutdown();
  msgr->wait();
}



bool DaemonServer::handle_open(MMgrOpen *m)
{
  DaemonKey key(
      m->get_connection()->get_peer_type(),
      m->daemon_name);

  dout(4) << "from " << m->get_connection() << " name "
          << m->daemon_name << dendl;

  auto configure = new MMgrConfigure();
  configure->stats_period = 5;
  m->get_connection()->send_message(configure);

  m->put();
  return true;
}

bool DaemonServer::handle_report(MMgrReport *m)
{
  DaemonKey key(
      m->get_connection()->get_peer_type(),
      m->daemon_name);

  dout(4) << "from " << m->get_connection() << " name "
          << m->daemon_name << dendl;

  std::shared_ptr<DaemonPerfCounters> counters;
  if (perf_counters.count(key)) {
    counters = perf_counters.at(key);
  } else {
    counters = std::make_shared<DaemonPerfCounters>(types);
    perf_counters.insert(std::make_pair(key, counters));
  }

  auto daemon_counters = perf_counters[key];
  counters->update(m);
  
  m->put();
  return true;
}

void DaemonPerfCounters::update(MMgrReport *report)
{
  dout(20) << "loading " << report->declare_types.size() << " new types, "
           << report->packed.length() << " bytes of data" << dendl;

  // Load any newly declared types
  for (const auto &t : report->declare_types) {
    types.insert(std::make_pair(t.path, t));
    declared_types.insert(t.path);
  }

  // Parse packed data according to declared set of types
  bufferlist::iterator p = report->packed.begin();
  DECODE_START(1, p);
  for (const auto &t_path : declared_types) {
    const auto &t = types.at(t_path);
    uint64_t val = 0;
    uint64_t avgcount = 0;
    uint64_t avgcount2 = 0;

    ::decode(val, p);
    if (t.type & PERFCOUNTER_LONGRUNAVG) {
      ::decode(avgcount, p);
      ::decode(avgcount2, p);
    }
    // TODO: interface for insertion of avgs, add timestamp
    instances[t_path].push(val);
  }
  // TODO: handle badly encoded things without asserting out
  DECODE_FINISH(p);
}

void DaemonServer::cull(entity_type_t daemon_type,
                        std::set<std::string> names_exist)
{
  Mutex::Locker l(lock);

  std::set<DaemonKey> victims;

  for (const auto &i : perf_counters) {
    if (i.first.first != daemon_type) {
      continue;
    }

    if (names_exist.count(i.first.second) == 0) {
      victims.insert(i.first);
    }
  }

  for (const auto &i : victims) {
    dout(4) << "Removing data for " << i << dendl;
    perf_counters.erase(i);
  }
}

