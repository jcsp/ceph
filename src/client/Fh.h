#ifndef CEPH_CLIENT_FH_H
#define CEPH_CLIENT_FH_H

#include "include/types.h"

class Inode;
class Cond;

// file handle for any open file state

struct Fh {
  Inode    *inode;
  loff_t     pos;
  int       mds;        // have to talk to mds we opened with (for now)
  int       mode;       // the mode i opened the file with

  int flags;
  bool pos_locked;           // pos is currently in use
  list<Cond*> pos_waiters;   // waiters for pos

  // readahead state
  loff_t last_pos;
  loff_t consec_read_bytes;
  int nr_consec_read;

  bool flock_locked;
  bool fcntl_locked;

  Fh() : inode(0), pos(0), mds(0), mode(0), flags(0), pos_locked(false),
	 last_pos(0), consec_read_bytes(0), nr_consec_read(0),
     flock_locked(false), fcntl_locked(false) {}
};


#endif
