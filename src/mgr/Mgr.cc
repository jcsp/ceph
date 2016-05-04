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

#include "mon/MonClient.h"
#include "PyFormatter.h"

#include "global/global_context.h"



#define dout_subsys ceph_subsys_mon


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

  // Start Objecter and wait for OSD map
  objecter->start();
  objecter->wait_for_osd_map();
  timer.init();

  // Prepare to receive MDS map and request it
  Mutex init_lock("Mgr:init");
  Cond cond;
  bool done = false;
  assert(!mdsmap->get_epoch());
  lock.Lock();
  waiting_for_mds_map = new C_SafeCond(&init_lock, &cond, &done, NULL);
  lock.Unlock();
  monc->sub_want("mdsmap", 0, CEPH_SUBSCRIBE_ONETIME);
  monc->renew_subs();

  // Wait for MDS map
  dout(4) << "waiting for MDS map..." << dendl;
  init_lock.Lock();
  while (!done)
    cond.Wait(init_lock);
  init_lock.Unlock();
  dout(4) << "Got MDS map " << mdsmap->get_epoch() << dendl;

  finisher.start();

  return 0;
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


bool Mgr::ms_dispatch(Message *m)
{
   Mutex::Locker locker(lock);
   switch (m->get_type()) {
   case CEPH_MSG_MON_MAP:
     break;
   case CEPH_MSG_MDS_MAP:
     handle_mds_map((MMDSMap*)m);
     break;
   case CEPH_MSG_OSD_MAP:
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
  if (what == "mdsmap") {
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
  } else if (what.substr(0, 6) == "osdmap") {
    PyFormatter f;
    objecter->with_osdmap([&f,&what](const OSDMap &osd_map){
      if (what == "osdmap") {
        osd_map.dump(&f);
      } else if (what == "osdmap_tree") {
        osd_map.print_tree(&f, nullptr);
      } else if (what == "osdmap_crush") {
        osd_map.crush->dump(&f);
      }
    });
    return f.get();
  } else {
    Py_RETURN_NONE;
  }
}

int Mgr::main(vector<const char *> args)
{
  global_handle = this;

  PyObject *pName, *pModule, *pFunc;
  PyObject *pArgs, *pValue;

  Py_Initialize();

  Py_InitModule("ceph_state", CephStateMethods);

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

  // Construct pModule
  // TODO load mgr_modules list, run them all in a thread each.
  pName = PyString_FromString("rest");
  pModule = PyImport_Import(pName);
  Py_DECREF(pName);

  if (pModule != NULL) {
      pFunc = PyObject_GetAttrString(pModule, "serve");
      //pNotify = PyObject_GetAttrString(pModule, "notify");
      if (pFunc && PyCallable_Check(pFunc)) {
          pArgs = PyTuple_New(0);
          pValue = PyObject_CallObject(pFunc, pArgs);
          Py_DECREF(pArgs);
          if (pValue != NULL) {
              Py_DECREF(pValue);
          } else {
              Py_DECREF(pFunc);
              Py_DECREF(pModule);
              PyErr_Print();
              return 1;
          }
      } else {
          if (PyErr_Occurred())
              PyErr_Print();
      }
      Py_XDECREF(pFunc);
      Py_DECREF(pModule);
  }
  else {
      PyErr_Print();
      return 1;
  }
  Py_Finalize();
  return 0;
}

