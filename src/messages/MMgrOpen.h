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


#ifndef CEPH_MMGROPEN_H_
#define CEPH_MMGROPEN_H_

#include "msg/Message.h"

class MMgrOpen : public Message
{
  /**
   * Client is responsible for remembering whether it has introduced
   * each perf counter to the server.  When first sending a particular
   * counter, it must inline the counter's schema here.
   */
  std::vector<PerfCounterType> types;

  // For all counters present, sorted by idx, output
  // as many bytes as are needed to represent them

  // Decode: iterate over the types we know about, sorted by idx,
  // and use the current type's type to decide how to decode
  // the next bytes from the bufferlist.
  bufferlist packed;

  void decode_payload();
  {
    bufferlist::iterator p = payload.begin();
    ::decode(types, p);
    ::decode(packed, p);
  }

  void encode_payload(uint64_t features) {
    ::encode(types, payload);
    ::encode(packed, payload);
  }
};

#endif

