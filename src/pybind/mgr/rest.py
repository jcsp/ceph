
import ceph_state

import json

from flask import Flask, Response
app = Flask("ceph")

@app.route('/mds')
def mds_list():
    mdsmap = ceph_state.get("mdsmap")

    result = []
    for gid_str, info in mdsmap["info"].items():
        result.append(info)

    return Response(json.dumps(result), mimetype="application/json")

@app.route('/osd')
def osd_list():
    osd_map = ceph_state.get("osdmap")


    return Response(json.dumps(osd_map['osds']), mimetype="application/json")

def serve():
    app.run()

