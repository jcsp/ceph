

# This is our magic hook into C++ land
import ceph_state
from calamari_rest.types import OsdMap, NotFound, Config, MdsMap, MonMap, \
    PgSummary, Health
from mgr_log import log


def recurse_refs(root, path):
    if isinstance(root, dict):
        for k, v in root.items():
            recurse_refs(v, path + "->%s" % k)
    elif isinstance(root, list):
        for n, i in enumerate(root):
            recurse_refs(i, path + "[%d]" % n)

    log.info("%s %d (%s)" % (path, sys.getrefcount(root), root.__class__))


def get_sync_object(object_type, path=None):
    if object_type == OsdMap:
        data = ceph_state.get("osd_map")

        assert data is not None

        data['tree'] = ceph_state.get("osd_map_tree")
        data['crush'] = ceph_state.get("osd_map_crush")
        data['crush_map_text'] = ceph_state.get("osd_map_crush_map_text")
        data['osd_metadata'] = ceph_state.get("osd_metadata")
        obj = OsdMap(data['epoch'], data)
    elif object_type == Config:
        data = ceph_state.get("config")
        obj = Config(0, data)
    elif object_type == MonMap:
        data = ceph_state.get("mon_map")
        obj = MonMap(data['epoch'], data)
    elif object_type == MdsMap:
        data = ceph_state.get("mds_map")
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


def get_server(hostname):
    return ceph_state.get_server(hostname)


def list_servers():
    return ceph_state.get_server(None)
