
"""
A RESTful API for Ceph
"""


import os

import cherrypy
from django.core.servers.basehttp import get_internal_wsgi_application

from mgr_data import get_sync_object
from mgr_log import log
from calamari_rest.types import OsdMap, MonMap
from calamari_rest.manager.request_collection import RequestCollection


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "calamari_rest.settings")


class State(object):
    """
    I'm the part that's visible to both the REST API and the
    notify() callbacks.  I run background jobs.
    """
    def __init__(self):
        self.requests = RequestCollection()

    def notify(self, what, what_id):
        pass


state = State()


def notify(notify_type, notify_id):
    log.info("Notify {0}".format(notify_type))
    if notify_type == "command":
        state.requests.on_completion(notify_id)
    elif notify_type == "osd_map":
        state.requests.on_map(OsdMap, get_sync_object(OsdMap))
    elif notify_type == "mon_map":
        state.requests.on_map(MonMap, get_sync_object(MonMap))
    else:
        log.warning("Unhandled notification type '{0}'".format(notify_type))


def serve():
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
