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

#ifndef DAEMON_METADATA_H_
#define DAEMON_METADATA_H_

#include <map>
#include <string>
#include <memory>

typedef std::pair<uint8_t, std::string> DaemonKey;

class DaemonMetadata
{
  public:
  DaemonKey key;
  std::string hostname;
  std::map<std::string, std::string> metadata;
};

typedef std::shared_ptr<DaemonMetadata> DaemonMetadataPtr;
typedef std::map<DaemonKey, DaemonMetadataPtr> DaemonMetadataCollection;

/**
 * Fuse the collection of per-daemon metadata from Ceph into
 * a view that can be queried by service type, ID or also
 * by server (aka fqdn).
 */
class DaemonMetadataIndex
{
  private:
  std::map<std::string, DaemonMetadataCollection> by_server;
  DaemonMetadataCollection all;

  public:
  void insert(DaemonMetadataPtr dm);
  void erase(DaemonKey dmk);

  DaemonMetadataPtr get(const DaemonKey &key);
  DaemonMetadataCollection get_by_server(const std::string &hostname) const;
  DaemonMetadataCollection get_by_type(uint8_t type) const;

  const DaemonMetadataCollection &get_all() const {return all;}
  const std::map<std::string, DaemonMetadataCollection> &get_all_servers() const
  {
    return by_server;
  }
};

#endif

