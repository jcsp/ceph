

"""
Helpers for writing django views and rest_framework ViewSets that get
their data from cthulhu with zeroRPC
"""
from collections import defaultdict
import logging

# Suppress warning from ZeroRPC's use of old gevent API
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning,
                        message=".*Queue\(0\) now equivalent to Queue\(None\);.*")
warnings.filterwarnings("ignore", category=DeprecationWarning,
                        message=".*gevent.coros has been renamed to gevent.lock.*")

from rest_framework import viewsets, status
from rest_framework.views import APIView
import time

from rest_framework.response import Response

from calamari_common.config import CalamariConfig

from calamari_common.types import OsdMap, SYNC_OBJECT_STR_TYPE, OSD, OSD_MAP, POOL, CLUSTER, CRUSH_RULE, ServiceId,\
    NotFound, SERVER
config = CalamariConfig()


class DataObject(object):
    """
    A convenience for converting dicts from the backend into
    objects, because django_rest_framework expects objects
    """
    def __init__(self, data):
        self.__dict__.update(data)

# This is our magic hook into C++ land
import ceph_state

class MgrClient(object):
    def get_sync_object_wrapped(self, object_type):
        if object_type == OsdMap:
            data = self.get_sync_object(OSD_MAP)
            return OsdMap(data['epoch'], data)
        else:
            raise NotImplementedError()

    def get_sync_object(self, object_name, path=None):
        if object_name == 'osd_map':
            data = ceph_state.get("osdmap")

            data['tree'] = ceph_state.get("osdmap_tree")
            data['crush'] = ceph_state.get("osdmap_crush")
            data['crush_map_text'] = ceph_state.get("osdmap_crush_map_text")
            # FIXME: implement sync of OSD metadata between mon and mgr
            data['osd_metadata'] = []
            obj = OsdMap(data['epoch'], data)
        else:
            raise NotImplementedError(object_name)

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
                raise NotFound(object_name, path)

        return obj

    def get(self, fs_id, object_type, object_id):
        """
        Get one object from a particular cluster.
        """

        if object_type == OSD:
            return self._osd_resolve(object_id)
        elif object_type == POOL:
            return self._pool_resolve(object_id)
        else:
            raise NotImplementedError(object_type)

    def get_valid_commands(self, fs_id, object_type, object_ids):
        """
        Determine what commands can be run on OSD object_ids
        """
        # FIXME: reinstate
        if False:
            if object_type != OSD:
                raise NotImplementedError(object_type)

            cluster = self._fs_resolve(fs_id)
            try:
                valid_commands = cluster.get_valid_commands(object_type, object_ids)
            except KeyError as e:
                raise NotFound(object_type, str(e))

            return valid_commands

        return []

    def _osd_resolve(self, cluster, osd_id):
        osdmap = self.get_sync_object_wrapped(OsdMap)

        try:
            return osdmap.osds_by_id[osd_id]
        except KeyError:
            raise NotFound(OSD, osd_id)

    def _pool_resolve(self, cluster, pool_id):
        osdmap = self.get_sync_object_wrapped(OsdMap)

        try:
            return osdmap.pools_by_id[pool_id]
        except KeyError:
            raise NotFound(POOL, pool_id)

    def server_by_service(self, services):
        # FIXME: implement in terms of OSD metadata
        return []

    def list(self, fs_id, object_type, list_filter):
        """
        Get many objects
        """

        osd_map = self.get_sync_object(OSD_MAP).data
        if osd_map is None:
            return []
        if object_type == OSD:
            result = osd_map['osds']
            if 'id__in' in list_filter:
                result = [o for o in result if o['osd'] in list_filter['id__in']]
            if 'pool' in list_filter:
                try:
                    osds_in_pool = self.get_sync_object_wrapped(OsdMap).osds_by_pool[list_filter['pool']]
                except KeyError:
                    raise NotFound("Pool {0} does not exist".format(list_filter['pool']))
                else:
                    result = [o for o in result if o['osd'] in osds_in_pool]

            return result
        elif object_type == POOL:
            return osd_map['pools']
        elif object_type == CRUSH_RULE:
            return osd_map['crush']['rules']
        else:
            raise NotImplementedError(object_type)


class RPCView(APIView):
    serializer_class = None
    log = logging.getLogger('django.request.profile')

    def __init__(self, *args, **kwargs):
        super(RPCView, self).__init__(*args, **kwargs)
        self.client = MgrClient()

    @property
    def help(self):
        return self.__doc__

    @property
    def help_summary(self):
        return ""

    def handle_exception(self, exc):
        try:
            return super(RPCView, self).handle_exception(exc)
        except NotFound as e:
                return Response(str(e), status=status.HTTP_404_NOT_FOUND)

    def metadata(self, request):
        ret = super(RPCView, self).metadata(request)

        actions = {}
        # TODO: get the fields marked up with whether they are:
        # - [allowed|required|forbidden] during [creation|update] (6 possible kinds of field)
        # e.g. on a pool
        # id is forbidden during creation and update
        # pg_num is required during create and optional during update
        # pgp_num is optional during create or update
        # nothing is required during update
        if hasattr(self, 'update'):
            if self.serializer_class:
                actions['PATCH'] = self.serializer_class().metadata()
        if hasattr(self, 'create'):
            if self.serializer_class:
                actions['POST'] = self.serializer_class().metadata()
        ret['actions'] = actions

        return ret


class RPCViewSet(viewsets.ViewSetMixin, RPCView):
    pass
