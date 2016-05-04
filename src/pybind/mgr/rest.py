
# This is our magic hook into C++ land
import ceph_state  # NOQA

"""
A lightweight server for running a minimal Calamari instance
in a single process.
"""

#from cthulhu.manager.request_collection import RequestCollection
from django.core.servers.basehttp import get_internal_wsgi_application
import gevent.event
import gevent
import signal
import os
from gevent.wsgi import WSGIServer

import threading
import json

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "calamari_rest.settings")

# class State(object):
#     """
#     I'm the part that's visible to both the REST API and the
#     notify() callbacks.  I run background jobs.
#     """
#     def __init__(self):
#         self.requests = RequestCollection()
#
#
#     def notify(self, what, what_id):
#         pass
#
#
# state = State()

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


def serve():
    result = CommandResult()
    ceph_state.send_command(result,
            json.dumps({
        "prefix": "get_command_descriptions"}))
    r, outb, outs = result.wait()
    print r, outb, outs

    app = get_internal_wsgi_application()
    wsgi = WSGIServer(('0.0.0.0', 8002), app)
    wsgi.start()

    complete = gevent.event.Event()

    def shutdown():
        complete.set()

    gevent.signal(signal.SIGTERM, shutdown)
    gevent.signal(signal.SIGINT, shutdown)

    while not complete.is_set():
        complete.wait(timeout=5)
#
# def notify(what, what_id):
#     state.notify()
