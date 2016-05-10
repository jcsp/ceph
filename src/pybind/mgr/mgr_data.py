

# This is our magic hook into C++ land
import ceph_state
from calamari_common.types import OsdMap, NotFound

from mgr_log import log
import json


def get_sync_object(object_type, path=None):
    if object_type == OsdMap:
        data = ceph_state.get("osd_map")
        assert data is not None

        data['tree'] = ceph_state.get("osd_map_tree")
        data['crush'] = ceph_state.get("osd_map_crush")
        data['crush_map_text'] = ceph_state.get("osd_map_crush_map_text")
        # FIXME: implement sync of OSD metadata between mon and mgr
        data['osd_metadata'] = []
        obj = OsdMap(data['epoch'], data)
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
