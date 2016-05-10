
# This is our magic hook into C++ land
import ceph_state  # NOQA
import cherrypy
from calamari_common.types import OsdMap, OSD_MAP
from mgr_data import get_sync_object

from mgr_log import log

import multiprocessing

"""
A lightweight server for running a minimal Calamari instance
in a single process.
"""

from cthulhu.manager.request_collection import RequestCollection
from django.core.servers.basehttp import get_internal_wsgi_application


import os

import threading

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


class CommandResult(object):
    def __init__(self):
        self.ev = threading.Event()
        self.outs = ""
        self.outb = ""
        self.r = 0

    def complete(self, r, outb, outs):
        self.r = r
        self.outb = outb
        self.outs = outs
        self.ev.set()

    def wait(self):
        self.ev.wait()
        return self.r, self.outb, self.outs


def notify(notify_type, notify_id):
    log.info("Notify {0}".format(notify_type))
    if notify_type == "command":
        state.requests.on_completion(notify_id)
    elif notify_type == "osd_map":
        state.requests.on_map(OsdMap, get_sync_object(OsdMap))
    else:
        log.warning("Unhandled notification type '{0}'".format(notify_type))


if False:
    import gunicorn
    import gunicorn.app.base


    class StandaloneApplication(gunicorn.app.base.BaseApplication):
        """
        This is the wrapper from the gunicorn docs for embedding gunicorn
        """
        def __init__(self, app, options=None):
            self.options = options or {}
            self.application = app
            super(StandaloneApplication, self).__init__()

        def load_config(self):
            config = dict([(key, value) for key, value in self.options.items()
                           if key in self.cfg.settings and value is not None])
            for key, value in config.items():
                self.cfg.set(key.lower(), value)

        def load(self):
            return self.application


    def number_of_workers():
        return (multiprocessing.cpu_count() * 2) + 1


    def serve():
        # result = CommandResult()
        # ceph_state.send_command(result,
        #         json.dumps({
        #     "prefix": "get_command_descriptions"}))
        # r, outb, outs = result.wait()
        # print r, outb, outs

        app = get_internal_wsgi_application()

        options = {
            'bind': '%s:%s' % ('127.0.0.1', '8002'),
            'workers': number_of_workers(),
        }
        StandaloneApplication(app, options).run()

    # import gevent.event
    # import gevent
    # import signal
    # from gevent.wsgi import WSGIServer
        #
        # wsgi = WSGIServer(('0.0.0.0', 8002), app)
        # wsgi.start()
        #
        # complete = gevent.event.Event()
        #
        # def shutdown():
        #     complete.set()
        #
        # gevent.signal(signal.SIGTERM, shutdown)
        # gevent.signal(signal.SIGINT, shutdown)
        #
        # while not complete.is_set():
        #     complete.wait(timeout=5)
else:
    def serve():
        app = get_internal_wsgi_application()
        cherrypy.config.update({
            'server.socket_port': 8002,
            'engine.autoreload.on': False
        })
        cherrypy.tree.graft(app, '/')
        cherrypy.engine.start()
        cherrypy.engine.block()
