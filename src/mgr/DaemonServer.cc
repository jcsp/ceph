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
DaemonServer::DaemonServer(MonClient *monc_)
    : Dispatcher(g_ceph_context), msgr(nullptr), monc(monc_)
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

bool DaemonServer::ms_get_authorizer(int dest_type,
    AuthAuthorizer **authorizer, bool force_new)
{
  if (dest_type == CEPH_ENTITY_TYPE_MON)
    return true;

  if (force_new) {
    if (monc->wait_auth_rotating(10) < 0)
      return false;
  }

  *authorizer = monc->auth->build_authorizer(dest_type);
  return *authorizer != NULL;
}


bool DaemonServer::ms_dispatch(Message *m)
{
  return false;
}

void DaemonServer::shutdown()
{
  msgr->shutdown();
  msgr->wait();
}

