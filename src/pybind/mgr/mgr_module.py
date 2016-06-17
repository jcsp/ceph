
import ceph_state  #noqa


class MgrModule(object):
    COMMANDS = []

    def __init__(self, handle):
        self._handle = handle

    def notify(self, notify_type, notify_id):
        """
        Called by the ceph-mgr service to notify the Python plugin
        that new state is available.
        """
        pass

    def serve(self):
        """
        Called by the ceph-mgr service to start any server that
        is provided by this Python plugin.  The implementation
        of this function should block until it receives a signal.
        """
        pass

    def get(self, data_name):
        """
        Called by the plugin to load some cluster state from ceph-mgr
        """
        return ceph_state.get(self._handle, data_name)

    def get_server(self, hostname):
        """
        Called by the plugin to load information about a particular
        node from ceph-mgr.

        :param hostname: a hostame
        """
        return ceph_state.get_server(self._handle, hostname)

    def list_servers(self):
        """
        Like ``get_server``, but instead of returning information
        about just one node, return all the nodes in an array.
        """
        return ceph_state.get_server(self._handle, None)

    def send_command(self, *args, **kwargs):
        """
        Called by the plugin to send a command to the mon
        cluster.
        """
        ceph_state.send_command(self._handle, *args, **kwargs)

    def handle_command(self, cmd):
        """
        Called by ceph-mgr to request the plugin to handle one
        of the commands that it declared in self.COMMANDS

        Return a status code, an output buffer, and an
        output string.  The output buffer is for data results,
        the output string is for informative text.

        :param cmd: dict, from Ceph's cmdmap_t

        :return: 3-tuple of (int, str, str)
        """

        # Should never get called if they didn't declare
        # any ``COMMANDS``
        raise NotImplementedError()
