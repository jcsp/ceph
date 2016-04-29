
# This is our magic hook into C++ land
import ceph_state

"""
A lightweight server for running a minimal Calamari instance
in a single process.
"""

from django.core.servers.basehttp import get_internal_wsgi_application
import gevent.event
import gevent
import signal
import os
from gevent.wsgi import WSGIServer

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "calamari_rest.settings")

# @app.route('/mds')
# def mds_list():
#     mdsmap = ceph_state.get("mdsmap")
#
#     result = []
#     for gid_str, info in mdsmap["info"].items():
#         result.append(info)
#
#     return Response(json.dumps(result), mimetype="application/json")
#
# @app.route('/osd')
# def osd_list():
#     osd_map = ceph_state.get("osdmap")
#
#     return Response(json.dumps(osd_map['osds']), mimetype="application/json")


def serve():
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
