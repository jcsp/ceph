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


#include "Mgr.h"
#include "PyState.h"
#include "MgrPyModule.h"

#include "common/ceph_json.h"
#include "mon/MonClient.h"
#include "PyFormatter.h"
#include "include/stringify.h"

#include "global/global_context.h"


#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "


Mgr::Mgr() :
  Dispatcher(g_ceph_context),
  objecter(NULL),
  lock("Mgr::lock"),
  timer(g_ceph_context, lock),
  finisher(g_ceph_context, "Mgr", "mgr-fin"),
  waiting_for_mds_map(NULL)
{
  monc = new MonClient(g_ceph_context);
  messenger = Messenger::create_client_messenger(g_ceph_context, "mds");
  mdsmap = new MDSMap();

  // FIXME: using objecter as convenience to handle incremental
  // OSD maps, but that's overkill.  We don't really need an objecter.
  objecter = new Objecter(g_ceph_context, messenger, monc, NULL, 0, 0);
}


Mgr::~Mgr()
{
  delete objecter;
  delete monc;
  delete messenger;
  delete mdsmap;
  assert(waiting_for_mds_map == NULL);
}


int Mgr::init()
{
  // Initialize Messenger
  int r = messenger->bind(g_conf->public_addr);
  if (r < 0)
    return r;

  messenger->start();

  objecter->set_client_incarnation(0);
  objecter->init();

  // Connect dispatchers before starting objecter
  messenger->add_dispatcher_tail(objecter);
  messenger->add_dispatcher_tail(this);

  // Initialize MonClient
  if (monc->build_initial_monmap() < 0) {
    objecter->shutdown();
    messenger->shutdown();
    messenger->wait();
    return -1;
  }

  monc->set_want_keys(CEPH_ENTITY_TYPE_MON|CEPH_ENTITY_TYPE_OSD|CEPH_ENTITY_TYPE_MDS);
  monc->set_messenger(messenger);
  monc->init();
  r = monc->authenticate();
  if (r < 0) {
    derr << "Authentication failed, did you specify an MDS ID with a valid keyring?" << dendl;
    monc->shutdown();
    objecter->shutdown();
    messenger->shutdown();
    messenger->wait();
    return r;
  }

  client_t whoami = monc->get_global_id();
  messenger->set_myname(entity_name_t::CLIENT(whoami.v));

  // Preload all daemon metadata (will subsequently keep this
  // up to date by watching maps, so do the initial load before
  // we subscribe to any maps)
  dout(4) << "Loading daemon metadata..." << dendl;
  load_all_metadata();

  // Start Objecter and wait for OSD map
  objecter->start();
  objecter->wait_for_osd_map();
  timer.init();

  // Prepare to receive MDS map and request it
  dout(4) << "requesting MDS map..." << dendl;
  assert(!mdsmap->get_epoch());
  C_SaferCond cond;
  lock.Lock();
  waiting_for_mds_map = &cond;
  lock.Unlock();
  monc->sub_want("mdsmap", 0, 0);
  monc->renew_subs();

  // Wait for MDS map
  dout(4) << "waiting for MDS map..." << dendl;
  cond.wait();
  lock.Lock();
  waiting_for_mds_map = nullptr;
  lock.Unlock();
  dout(4) << "Got MDS map " << mdsmap->get_epoch() << dendl;

  finisher.start();

  dout(4) << "Complete." << dendl;
  return 0;
}


void Mgr::load_all_metadata()
{
  Mutex::Locker l(lock);

  bufferlist outbl;
  std::string outs;
  std::string cmd = "{\"prefix\": \"osd metadata\"}";

  C_SaferCond cond;
  monc->start_mon_command({cmd}, {}, &outbl, &outs, &cond);
  int r = cond.wait();

  
  json_spirit::mValue metadata_list;
  bool read_ok = json_spirit::read(outbl.to_str(), metadata_list);

  // FIXME: error handling: this command is supposed to never fail but...
  assert(r == 0);
  assert(read_ok);

  for (auto &osd_metadata_val : metadata_list.get_array()) {
    json_spirit::mObject osd_metadata = osd_metadata_val.get_obj();
    dout(4) << osd_metadata.at("hostname").get_str() << dendl;

    DaemonMetadataPtr dm = std::make_shared<DaemonMetadata>();
    dm->key = DaemonKey(CEPH_ENTITY_TYPE_OSD,
                        stringify(osd_metadata.at("id").get_int()));
    dm->hostname = osd_metadata.at("hostname").get_str();

    osd_metadata.erase("id");
    osd_metadata.erase("hostname");

    for (const auto &i : osd_metadata) {
      dm->metadata[i.first] = i.second.get_str();
    }

    dmi.insert(dm);
  }

#if 0
  /**
  dout(4) << outbl.to_str() << dendl;
  dout(4) << outs << dendl;
  **/

  JSONObj obj;
  // FIXME: error handling?  check r and also catch exception from decode
  assert(r == 0);
  decode_json_obj(outbl, &obj);
  dout(4) << obj << dendl;

  assert(obj.is_array());
#endif
}


void Mgr::shutdown()
{
  finisher.stop();

  lock.Lock();
  timer.shutdown();
  objecter->shutdown();
  lock.Unlock();
  monc->shutdown();
  messenger->shutdown();
  messenger->wait();
}

void Mgr::notify_all(const std::string &notify_type,
                     const std::string &notify_id)
{
  dout(10) << __func__ << ": notify_all " << notify_type << dendl;
  for (auto i : modules) {
    i->notify(notify_type, notify_id);
  }
}

bool Mgr::ms_dispatch(Message *m)
{
   Mutex::Locker locker(lock);

   derr << *m << dendl;

   switch (m->get_type()) {
   case CEPH_MSG_MON_MAP:
     // FIXME: we probably never get called here because MonClient
     // has consumed the message.  For consuming OSDMap we need
     // to be the tail dispatcher, but to see MonMap we would
     // need to be at the head.
     notify_all("mon_map", "");
     break;
   case CEPH_MSG_MDS_MAP:
     notify_all("mds_map", "");
     handle_mds_map((MMDSMap*)m);
     break;
   case CEPH_MSG_OSD_MAP:
     notify_all("osd_map", "");

     // Continuous subscribe, so that we can generate notifications
     // for our MgrPyModules
     objecter->maybe_request_map();
     break;
   default:
     return false;
   }
   return true;
}


void Mgr::handle_mds_map(MMDSMap* m)
{
  mdsmap->decode(m->get_encoded());
  if (waiting_for_mds_map) {
    waiting_for_mds_map->complete(0);
    waiting_for_mds_map = NULL;
  }
}


bool Mgr::ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
                         bool force_new)
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

PyObject *Mgr::get_python(const std::string &what)
{
  Mutex::Locker l(lock);

  if (what == "mds_map") {
    PyFormatter f;
    mdsmap->dump(&f);
    return f.get();
  } else if (what == "osdmap_crush_map_text") {
    bufferlist rdata;
    objecter->with_osdmap([&rdata](const OSDMap &osd_map){
      osd_map.crush->encode(rdata);
    });
    std::string crush_text = rdata.to_str();
    return PyString_FromString(crush_text.c_str());
  } else if (what.substr(0, 7) == "osd_map") {
    PyFormatter f;
    objecter->with_osdmap([&f, &what](const OSDMap &osd_map){
      if (what == "osd_map") {
        osd_map.dump(&f);
      } else if (what == "osd_map_tree") {
        osd_map.print_tree(&f, nullptr);
      } else if (what == "osd_map_crush") {
        osd_map.crush->dump(&f);
      }
    });
    return f.get();
  } else if (what == "config") {
    PyFormatter f;
    g_conf->show_config(&f);
    return f.get();
  } else if (what == "mon_map") {
    PyFormatter f;
    monc->with_monmap(
      [&f](const MonMap &monmap) {
        monmap.dump(&f);
      }
    );
    return f.get();
  } else if (what == "osd_metadata") {
    PyFormatter f;
    auto dmc = dmi.get_by_type(CEPH_ENTITY_TYPE_OSD);
    for (const auto &i : dmc) {
      f.open_object_section(i.first.second.c_str());
      f.dump_string("hostname", i.second->hostname);
      for (const auto &j : i.second->metadata) {
        f.dump_string(j.first.c_str(), j.second);
      }
      f.close_section();
    }
    return f.get();
  } else {
    derr << "Python module requested unknown data '" << what << "'" << dendl;
    Py_RETURN_NONE;
  }
}

int Mgr::main(vector<const char *> args)
{
  global_handle = this;

  // Set up global python interpreter
  Py_Initialize();

  // Some python modules do not cope with an unpopulated argv, so lets
  // fake one.  This step also picks up site-packages into sys.path.
  const char *argv[] = {"ceph-mgr"};
  PySys_SetArgv(1, (char**)argv);
  
  // Populate python namespace with callable hooks
  Py_InitModule("ceph_state", CephStateMethods);

  // Configure sys.path to include mgr_module_path
  const std::string module_path = g_conf->mgr_module_path;
  dout(4) << "Loading modules from '" << module_path << "'" << dendl;
  std::string sys_path = Py_GetPath();

  // We need site-packages for flask et al, unless we choose to
  // embed them in the ceph package.  site-packages is an interpreter-specific
  // thing, so as an embedded interpreter we're responsible for picking
  // this.  FIXME: don't hardcode this.
  std::string site_packages = "/usr/lib/python2.7/site-packages:/usr/lib64/python2.7/site-packages:/usr/lib64/python2.7";
  sys_path += ":";
  sys_path += site_packages;

  sys_path += ":";
  sys_path += module_path;
  dout(10) << "Computed sys.path '" << sys_path << "'" << dendl;
  PySys_SetPath((char*)(sys_path.c_str()));

  // Let CPython know that we will be calling it back from other
  // threads in future.
  if (! PyEval_ThreadsInitialized()) {
    PyEval_InitThreads();
  }

  // Load python code
  // TODO load mgr_modules list, run them all in a thread each.
  auto mod = new MgrPyModule("rest");
  int r = mod->load();
  if (r != 0) {
    derr << "Error loading python module" << dendl;
    // FIXME: be tolerant of bad modules, log an error and continue
    // to load other, healthy modules.
    return -1;
  }
  {
    Mutex::Locker locker(lock);
    modules.push_back(mod);
  }

  // Execute python server
  mod->serve();

  {
    Mutex::Locker locker(lock);
    // Tear down modules
    for (auto i : modules) {
      delete i;
    }
    modules.clear();
  }

  Py_Finalize();
  return 0;
}

