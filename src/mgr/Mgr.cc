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

#include "PyState.h"

#include "common/ceph_json.h"
#include "common/errno.h"
#include "mon/MonClient.h"
#include "include/stringify.h"
#include "global/global_context.h"

#include "mgr/MgrContext.h"

#include "MgrPyModule.h"
#include "DaemonServer.h"
#include "messages/MMgrBeacon.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "PyFormatter.h"

#include "Mgr.h"

#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "


Mgr::Mgr() :
  Dispatcher(g_ceph_context),
  objecter(NULL),
  monc(new MonClient(g_ceph_context)),
  lock("Mgr::lock"),
  timer(g_ceph_context, lock),
  finisher(g_ceph_context, "Mgr", "mgr-fin"),
  waiting_for_fs_map(NULL),
  server(monc)
{
  client_messenger = Messenger::create_client_messenger(g_ceph_context, "mds");
  fsmap = new FSMap();

  // FIXME: using objecter as convenience to handle incremental
  // OSD maps, but that's overkill.  We don't really need an objecter.
  objecter = new Objecter(g_ceph_context, client_messenger, monc, NULL, 0, 0);
}


Mgr::~Mgr()
{
  delete objecter;
  delete monc;
  delete client_messenger;
  delete fsmap;
  assert(waiting_for_fs_map == NULL);
}


/**
 * Context for completion of metadata mon commands: take
 * the result and stash it in DaemonMetadataIndex
 */
class MetadataUpdate : public Context
{
  DaemonMetadataIndex &dmi;
  DaemonKey key;

public:
  bufferlist outbl;
  std::string outs;

  MetadataUpdate(DaemonMetadataIndex &dmi_, const DaemonKey &key_)
    : dmi(dmi_), key(key_) {}

  void finish(int r)
  {
    dmi.clear_updating(key);
    if (r == 0) {
      if (key.first == CEPH_ENTITY_TYPE_MDS) {
        json_spirit::mValue json_result;
        bool read_ok = json_spirit::read(
            outbl.to_str(), json_result);
        if (!read_ok) {
          dout(1) << "mon returned invalid JSON for "
                  << ceph_entity_type_name(key.first)
                  << "." << key.second << dendl;
          return;
        }


        json_spirit::mObject daemon_meta = json_result.get_obj();
        DaemonMetadataPtr dm = std::make_shared<DaemonMetadata>();
        dm->key = key;
        dm->hostname = daemon_meta.at("hostname").get_str();

        daemon_meta.erase("name");
        daemon_meta.erase("hostname");

        for (const auto &i : daemon_meta) {
          dm->metadata[i.first] = i.second.get_str();
        }

        dmi.insert(dm);
      } else if (key.first == CEPH_ENTITY_TYPE_OSD) {
      } else {
        assert(0);
      }
    } else {
      dout(1) << "mon failed to return metadata for "
              << ceph_entity_type_name(key.first)
              << "." << key.second << ": " << cpp_strerror(r) << dendl;
    }
  }
};




int Mgr::init()
{
  // Initialize Messenger
  int r = client_messenger->bind(g_conf->public_addr);
  if (r < 0)
    return r;

  client_messenger->start();

  objecter->set_client_incarnation(0);
  objecter->init();

  // Connect dispatchers before starting objecter
  client_messenger->add_dispatcher_tail(objecter);
  client_messenger->add_dispatcher_tail(this);

  // Initialize MonClient
  if (monc->build_initial_monmap() < 0) {
    objecter->shutdown();
    client_messenger->shutdown();
    client_messenger->wait();
    return -1;
  }

  monc->set_want_keys(CEPH_ENTITY_TYPE_MON|CEPH_ENTITY_TYPE_OSD
      |CEPH_ENTITY_TYPE_MDS|CEPH_ENTITY_TYPE_MGR);
  monc->set_messenger(client_messenger);
  monc->init();
  r = monc->authenticate();
  if (r < 0) {
    derr << "Authentication failed, did you specify a mgr ID with a valid keyring?" << dendl;
    monc->shutdown();
    objecter->shutdown();
    client_messenger->shutdown();
    client_messenger->wait();
    return r;
  }

  client_t whoami = monc->get_global_id();
  client_messenger->set_myname(entity_name_t::CLIENT(whoami.v));

  // Start communicating with daemons to learn statistics etc
  server.init(monc->get_global_id(), client_messenger->get_myaddr());

  dout(4) << "Initialized server at " << server.get_myaddr() << dendl;
  // TODO: send the beacon periodically
  MMgrBeacon *m = new MMgrBeacon(monc->get_global_id(),
                                 server.get_myaddr());
  monc->send_mon_message(m);

  // Preload all daemon metadata (will subsequently keep this
  // up to date by watching maps, so do the initial load before
  // we subscribe to any maps)
  dout(4) << "Loading daemon metadata..." << dendl;
  load_all_metadata();

  // Preload config keys (`get` for plugins is to be a fast local
  // operation, we we don't have to synchronize these later because
  // all sets will come via mgr)
  load_config();

  // Start Objecter and wait for OSD map
  objecter->start();
  objecter->wait_for_osd_map();
  timer.init();

  // Prepare to receive FSMap and request it
  dout(4) << "requesting FSMap..." << dendl;
  assert(!fsmap->get_epoch());
  C_SaferCond cond;
  lock.Lock();
  waiting_for_fs_map = &cond;
  lock.Unlock();
  monc->sub_want("fsmap", 0, 0);
  monc->renew_subs();

  // Wait for FSMap
  dout(4) << "waiting for FSMap..." << dendl;
  cond.wait();
  lock.Lock();
  waiting_for_fs_map = nullptr;
  lock.Unlock();
  dout(4) << "Got FSMap " << fsmap->get_epoch() << dendl;

  finisher.start();

  dout(4) << "Complete." << dendl;
  return 0;
}


class Command
{
protected:
  C_SaferCond cond;
public:
  bufferlist outbl;
  std::string outs;
  int r;

  void run(MonClient *monc, const std::string &command)
  {
    monc->start_mon_command({command}, {},
        &outbl, &outs, &cond);
  }

  virtual void wait()
  {
    r = cond.wait();
  }

  virtual ~Command() {}
};


class JSONCommand : public Command
{
public:
  json_spirit::mValue json_result;

  void wait()
  {
    Command::wait();

    if (r == 0) {
      bool read_ok = json_spirit::read(
          outbl.to_str(), json_result);
      if (!read_ok) {
        r = -EINVAL;
      }
    }
  }
};


void Mgr::load_all_metadata()
{
  Mutex::Locker l(lock);

  JSONCommand mds_cmd;
  mds_cmd.run(monc, "{\"prefix\": \"mds metadata\"}");
  JSONCommand osd_cmd;
  osd_cmd.run(monc, "{\"prefix\": \"osd metadata\"}");
  JSONCommand mon_cmd;
  mon_cmd.run(monc, "{\"prefix\": \"mon metadata\"}");

  mds_cmd.wait();
  osd_cmd.wait();
  mon_cmd.wait();

  assert(mds_cmd.r == 0);
  assert(mon_cmd.r == 0);
  assert(osd_cmd.r == 0);

  for (auto &metadata_val : mds_cmd.json_result.get_array()) {
    json_spirit::mObject daemon_meta = metadata_val.get_obj();

    DaemonMetadataPtr dm = std::make_shared<DaemonMetadata>();
    dm->key = DaemonKey(CEPH_ENTITY_TYPE_MDS,
                        daemon_meta.at("name").get_str());
    dm->hostname = daemon_meta.at("hostname").get_str();

    daemon_meta.erase("name");
    daemon_meta.erase("hostname");

    for (const auto &i : daemon_meta) {
      dm->metadata[i.first] = i.second.get_str();
    }

    dmi.insert(dm);
  }

  for (auto &metadata_val : mon_cmd.json_result.get_array()) {
    json_spirit::mObject daemon_meta = metadata_val.get_obj();

    DaemonMetadataPtr dm = std::make_shared<DaemonMetadata>();
    dm->key = DaemonKey(CEPH_ENTITY_TYPE_MON,
                        daemon_meta.at("name").get_str());
    dm->hostname = daemon_meta.at("hostname").get_str();

    daemon_meta.erase("name");
    daemon_meta.erase("hostname");

    for (const auto &i : daemon_meta) {
      dm->metadata[i.first] = i.second.get_str();
    }

    dmi.insert(dm);
  }

  for (auto &osd_metadata_val : osd_cmd.json_result.get_array()) {
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
}

const std::string config_prefix = "mgr.";

void Mgr::load_config()
{
  dout(10) << "listing keys" << dendl;
  JSONCommand cmd;
  cmd.run(monc, "{\"prefix\": \"config-key list\"}");

  cmd.wait();
  assert(cmd.r == 0);
  
  for (auto &key_str : cmd.json_result.get_array()) {
    std::string const key = key_str.get_str();
    dout(20) << "saw key '" << key << "'" << dendl;

    if (key.substr(0, config_prefix.size()) == config_prefix) {
      dout(20) << "fetching '" << key << "'" << dendl;
      Command get_cmd;
      std::ostringstream cmd_json;
      cmd_json << "{\"prefix\": \"config-key get\", \"key\": \"" << key << "\"}";
      get_cmd.run(monc, cmd_json.str());
      get_cmd.wait();
      assert(get_cmd.r == 0);

      config_cache[key] = get_cmd.outbl.to_str();
    }
  }
}


void Mgr::shutdown()
{
  // First stop the server so that we're not taking any more incoming requests
  server.shutdown();

  // Then stop the finisher to ensure its enqueued contexts aren't going
  // to touch references to the things we're about to tear down
  finisher.stop();

  lock.Lock();
  timer.shutdown();
  objecter->shutdown();
  lock.Unlock();

  monc->shutdown();
  client_messenger->shutdown();
  client_messenger->wait();
}

void Mgr::notify_all(const std::string &notify_type,
                     const std::string &notify_id)
{
  // The python code might try and call back into
  // our C++->Python interface, so we must not be
  // holding this lock when we call into the python code.
  assert(lock.is_locked_by_me());

  dout(10) << __func__ << ": notify_all " << notify_type << dendl;
  for (auto i : modules) {
    // Send all python calls down a Finisher to avoid blocking
    // C++ code, and avoid any potential lock cycles.
    finisher.queue(new C_StdFunction([i, notify_type, notify_id](){
      i->notify(notify_type, notify_id);
    }));
  }
}

void Mgr::handle_osd_map()
{
  std::set<std::string> names_exist;

  /**
   * When we see a new OSD map, inspect the entity addrs to
   * see if they have changed (service restart), and if so
   * reload the metadata.
   */
  objecter->with_osdmap([this, &names_exist](const OSDMap &osd_map) {
    for (unsigned int osd_id = 0; osd_id < osd_map.get_num_osds(); ++osd_id) {
      if (!osd_map.exists(osd_id)) {
        continue;
      }

      // Remember which OSDs exist so that we can cull any that don't
      names_exist.insert(stringify(osd_id));

      // Consider whether to update the daemon metadata (new/restarted daemon)
      bool update_meta = false;
      const auto k = DaemonKey(CEPH_ENTITY_TYPE_OSD, stringify(osd_id));
      if (dmi.is_updating(k)) {
        continue;
      }

      if (dmi.exists(k)) {
        auto metadata = dmi.get(k);
        auto metadata_addr = metadata->metadata.at("front_addr");
        const auto &map_addr = osd_map.get_addr(osd_id);

        if (metadata_addr != stringify(map_addr)) {
          dout(4) << "OSD[" << osd_id << "] addr change " << metadata_addr
                  << " != " << stringify(map_addr) << dendl;
          update_meta = true;
        } else {
          dout(20) << "OSD[" << osd_id << "] addr unchanged: "
                   << metadata_addr << dendl;
        }
      } else {
        update_meta = true;
      }

      if (update_meta) {
        dmi.notify_updating(k);
        auto c = new MetadataUpdate(dmi, k);
        std::ostringstream cmd;
        cmd << "{\"prefix\": \"osd metadata\", \"id\": "
            << osd_id << "}";
        int r = monc->start_mon_command(
            {cmd.str()},
            {}, &c->outbl, &c->outs, c);
        assert(r == 0);  // start_mon_command defined to not fail
      }
    }
  });

  server.cull(CEPH_ENTITY_TYPE_OSD, names_exist);
  dmi.cull(CEPH_ENTITY_TYPE_OSD, names_exist);
}

bool Mgr::ms_dispatch(Message *m)
{
  derr << *m << dendl;
  Mutex::Locker l(lock);

  switch (m->get_type()) {
    case CEPH_MSG_MON_MAP:
      // FIXME: we probably never get called here because MonClient
      // has consumed the message.  For consuming OSDMap we need
      // to be the tail dispatcher, but to see MonMap we would
      // need to be at the head.
      assert(0);

      notify_all("mon_map", "");
      break;
    case CEPH_MSG_FS_MAP:
      notify_all("fs_map", "");
      handle_fs_map((MFSMap*)m);
      m->put();
      break;
    case CEPH_MSG_OSD_MAP:

      handle_osd_map();

      notify_all("osd_map", "");

      // Continuous subscribe, so that we can generate notifications
      // for our MgrPyModules
      objecter->maybe_request_map();
      m->put();
      break;

    case MSG_COMMAND:
      handle_command(static_cast<MCommand*>(m));
      m->put();
      break;

    default:
      return false;
  }
  return true;
}



void Mgr::handle_fs_map(MFSMap* m)
{

  *fsmap = m->get_fsmap();
  if (waiting_for_fs_map) {
    waiting_for_fs_map->complete(0);
    waiting_for_fs_map = NULL;
  }

  auto mds_info = fsmap->get_mds_info();
  for (const auto &i : mds_info) {
    const auto &info = i.second;

    const auto k = DaemonKey(CEPH_ENTITY_TYPE_MDS, info.name);
    if (dmi.is_updating(k)) {
      continue;
    }

    bool update = false;
    if (dmi.exists(k)) {
      auto metadata = dmi.get(k);
      // FIXME: nothing stopping old daemons being here, they won't have
      // addr :-/
      auto metadata_addr = metadata->metadata.at("addr");
      const auto map_addr = info.addr;

      if (metadata_addr != stringify(map_addr)) {
        dout(4) << "MDS[" << info.name << "] addr change " << metadata_addr
                << " != " << stringify(map_addr) << dendl;
        update = true;
      } else {
        dout(20) << "MDS[" << info.name << "] addr unchanged: "
                 << metadata_addr << dendl;
      }
    } else {
      update = true;
    }

    if (update) {
      dmi.notify_updating(k);
      auto c = new MetadataUpdate(dmi, k);
      std::ostringstream cmd;
      cmd << "{\"prefix\": \"mds metadata\", \"who\": \""
          << info.name << "\"}";
      int r = monc->start_mon_command(
          {cmd.str()},
          {}, &c->outbl, &c->outs, c);
      assert(r == 0);  // start_mon_command defined to not fail
    }
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

void Mgr::dump_server(const std::string &hostname,
                      const DaemonMetadataCollection &dmc,
                      Formatter *f)
{
  f->dump_string("hostname", hostname);
  f->open_array_section("services");
  std::string ceph_version;

  for (const auto &i : dmc) {
    const auto &key = i.first;
    const std::string str_type = ceph_entity_type_name(key.first);
    const std::string &svc_name = key.second;

    // TODO: pick the highest version, and make sure that
    // somewhere else (during health reporting?) we are
    // indicating to the user if we see mixed versions
    ceph_version = i.second->metadata.at("ceph_version");

    f->open_object_section("service");
    f->dump_string("type", str_type);
    f->dump_string("id", svc_name);
    f->close_section();
  }
  f->close_section();

  f->dump_string("ceph_version", ceph_version);
}

PyObject *Mgr::get_server_python(const std::string &hostname)
{
  PyThreadState *tstate = PyEval_SaveThread();
  Mutex::Locker l(lock);
  PyEval_RestoreThread(tstate);
  dout(10) << " (" << hostname << ")" << dendl;

  auto dmc = dmi.get_by_server(hostname);

  PyFormatter f;
  dump_server(hostname, dmc, &f);
  return f.get();
}


PyObject *Mgr::list_servers_python()
{
  PyThreadState *tstate = PyEval_SaveThread();
  Mutex::Locker l(lock);
  PyEval_RestoreThread(tstate);
  dout(10) << " >" << dendl;

  PyFormatter f(false, true);
  const auto &all = dmi.get_all_servers();
  for (const auto &i : all) {
    const auto &hostname = i.first;

    f.open_object_section("server");
    dump_server(hostname, i.second, &f);
    f.close_section();
  }

  return f.get();
}


PyObject *Mgr::get_python(const std::string &what)
{
  PyThreadState *tstate = PyEval_SaveThread();
  Mutex::Locker l(lock);
  PyEval_RestoreThread(tstate);

  if (what == "fs_map") {
    PyFormatter f;
    fsmap->dump(&f);
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
    return r;
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


bool Mgr::get_config(const std::string &key, std::string *val) const
{
  if (config_cache.count(key)) {
    *val = config_cache.at(key);
    return true;
  } else {
    return false;
  }
}

void Mgr::set_config(const std::string &key, const std::string &val)
{
  config_cache[key] = val;

  std::ostringstream cmd_json;
  Command set_cmd;
  cmd_json << "{\"prefix\": \"config-key put\","
              " \"key\": \"" << key << "\","
              "\"val\": \"" << val << "\"}";
  set_cmd.run(monc, cmd_json.str());
  set_cmd.wait();

  // FIXME: is config-key put ever allowed to fail?
  assert(set_cmd.r == 0);
}

struct MgrCommand {
  string cmdstring;
  string helpstring;
  string module;
  string perm;
  string availability;
} mgr_commands[] = {

#define COMMAND(parsesig, helptext, module, perm, availability) \
  {parsesig, helptext, module, perm, availability},

COMMAND("foo " \
	"name=bar,type=CephString", \
	"do a thing", "mgr", "rw", "cli")
};

void Mgr::handle_command(MCommand *m)
{
  int r = 0;
  std::stringstream ss;
  std::stringstream ds;
  bufferlist odata;
  std::string prefix;

  assert(lock.is_locked_by_me());

  map<string, cmd_vartype> cmdmap;

  // TODO enforce some caps
  
  // TODO background the call into python land so that we don't
  // block a messenger thread on python code.

  ConnectionRef con = m->get_connection();

  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    r = -EINVAL;
    goto out;
  }

  cmd_getval(cct, cmdmap, "prefix", prefix);

  if (prefix == "get_command_descriptions") {
    int cmdnum = 0;
    JSONFormatter *f = new JSONFormatter();
    f->open_object_section("command_descriptions");
    for (MgrCommand *cp = mgr_commands;
	 cp < &mgr_commands[ARRAY_SIZE(mgr_commands)]; cp++) {

      ostringstream secname;
      secname << "cmd" << setfill('0') << std::setw(3) << cmdnum;
      dump_cmddesc_to_json(f, secname.str(), cp->cmdstring, cp->helpstring,
			   cp->module, cp->perm, cp->availability);
      cmdnum++;
    }
    f->close_section();	// command_descriptions

    f->flush(ds);
    delete f;
    goto out;
  }

 out:
  std::string rs;
  rs = ss.str();
  odata.append(ds);
  dout(1) << "do_command r=" << r << " " << rs << dendl;
  //clog->info() << rs << "\n";
  if (con) {
    MCommandReply *reply = new MCommandReply(r, rs);
    reply->set_tid(m->get_tid());
    reply->set_data(odata);
    con->send_message(reply);
  }
}

