
"""
A RESTful API for Ceph
"""

# We must share a global reference to this instance, because it is the
# gatekeeper to all accesses to data from the C++ side (e.g. the REST API
# request handlers need to see it)
_global_instance = {'plugin': None}
def global_instance():
    assert _global_instance['plugin'] is not None
    return _global_instance['plugin']

import os
import logging
import logging.config
import json

import cherrypy
from django.core.servers.basehttp import get_internal_wsgi_application

from mgr_module import MgrModule
from mgr_log import log

from calamari_rest.manager.request_collection import RequestCollection
from calamari_rest.types import OsdMap, NotFound, Config, MdsMap, MonMap, \
    PgSummary, Health


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "calamari_rest.settings")

django_log = logging.getLogger("django.request")
django_log.addHandler(logging.StreamHandler())
django_log.setLevel(logging.DEBUG)

def recurse_refs(root, path):
    if isinstance(root, dict):
        for k, v in root.items():
            recurse_refs(v, path + "->%s" % k)
    elif isinstance(root, list):
        for n, i in enumerate(root):
            recurse_refs(i, path + "[%d]" % n)

    log.info("%s %d (%s)" % (path, sys.getrefcount(root), root.__class__))


class Module(MgrModule):
    COMMANDS = [
            {
                "cmd": "foo name=bar,type=CephString",
                "desc": "do a thing",
                "perm": "rw"
            }
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        _global_instance['plugin'] = self
        log.info("Constructing module {0}: instance {1}".format(
            __name__, _global_instance))
        self.requests = RequestCollection()

    def notify(self, notify_type, notify_id):
        log.info("Notify {0}".format(notify_type))
        if notify_type == "command":
            self.requests.on_completion(notify_id)
        elif notify_type == "osd_map":
            self.requests.on_map(OsdMap, get_sync_object(OsdMap))
        elif notify_type == "mon_map":
            self.requests.on_map(MonMap, get_sync_object(MonMap))
        else:
            log.warning("Unhandled notification type '{0}'".format(notify_type))

    def get_sync_object(self, object_type, path=None):
        if object_type == OsdMap:
            data = self.get("osd_map")

            assert data is not None

            data['tree'] = self.get("osd_map_tree")
            data['crush'] = self.get("osd_map_crush")
            data['crush_map_text'] = self.get("osd_map_crush_map_text")
            data['osd_metadata'] = self.get("osd_metadata")
            obj = OsdMap(data['epoch'], data)
        elif object_type == Config:
            data = self.get("config")
            obj = Config(0, data)
        elif object_type == MonMap:
            data = self.get("mon_map")
            obj = MonMap(data['epoch'], data)
        elif object_type == MdsMap:
            data = self.get("mds_map")
            obj = MdsMap(data['epoch'], data)
        elif object_type == PgSummary:
            # TODO
            raise NotImplementedError(object_type)
        elif object_type == Health:
            # TODO
            raise NotImplementedError(object_type)
        else:
            raise NotImplementedError(object_type)

        # TODO: move 'path' handling up into C++ land so that we only
        # Pythonize the part we're interested in
        if path:
            try:
                for part in path:
                    if isinstance(obj, dict):
                        obj = obj[part]
                    else:
                        obj = getattr(obj, part)
            except (AttributeError, KeyError) as e:
                raise NotFound(object_type, path)

        return obj

    def serve(self):
        app = get_internal_wsgi_application()
        cherrypy.config.update({
            'server.socket_port': 8002,
            'engine.autoreload.on': False
        })
        cherrypy.tree.graft(app, '/')

        # Dowser is a python memory debugging tool
        # import dowser
        # cherrypy.tree.mount(dowser.Root(), "/dowser")

        cherrypy.engine.start()
        cherrypy.engine.block()

    def handle_command(self, cmd):
        log.info("handle_command: {0}".format(json.dumps(cmd, indent=2)))
        prefix = cmd['prefix']
        if prefix == "foo":
            return (0, "", "")
        else:
            raise NotImplementedError(prefix)


