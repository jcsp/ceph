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

#include "DaemonMetadata.h"

void DaemonMetadataIndex::insert(DaemonMetadataPtr dm)
{
  if (all.count(dm->key)) {
    erase(dm->key);
  }

  by_server[dm->hostname][dm->key] = dm;
  all[dm->key] = dm;
}

void DaemonMetadataIndex::erase(DaemonKey dmk)
{
  const auto dm = all.at(dmk);
  auto &server_collection = by_server[dm->hostname];
  server_collection.erase(dm->key);
  if (server_collection.empty()) {
    by_server.erase(dm->hostname);
  }

  all.erase(dmk);
}

DaemonMetadataCollection DaemonMetadataIndex::get_by_type(uint8_t type) const
{
  DaemonMetadataCollection result;

  for (const auto &i : all) {
    if (i.first.first == type) {
      result[i.first] = i.second;
    }
  }

  return result;
}

DaemonMetadataCollection DaemonMetadataIndex::get_by_server(const std::string &hostname) const
{
  if (by_server.count(hostname)) {
    return by_server.at(hostname);
  } else {
    return {};
  }
}

