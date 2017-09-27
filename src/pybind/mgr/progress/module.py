
from mgr_module import MgrModule
import threading
import datetime
import uuid


import json


class Event(object):
    """
    A generic "event" that has a start time, completion percentage,
    and a list of "refs" that are (type, id) tuples describing which
    objects (osds, pools) this relates to.
    """
    def __init__(self, message, refs):
        self._message = message
        self._refs = refs

        self._started_at = datetime.datetime.utcnow()

    @property
    def message(self):
        return self._message

    @property
    def progress(self):
        raise NotImplementedError()

    def summary(self):
        return "{0} {1}".format(self.progress, self._message)

    def _progress(self, width):
        inner_width = width - 2
        out = "["
        done_chars = int(self.progress * inner_width)
        out += done_chars * '='
        out += inner_width - done_chars * '.'
        out += "]"

        return out

    def twoline_progress(self):
        """
        e.g.

        - Strudeling my fruitcake
            [===============..............]

        """
        return "- {0}\n    {1}".format(
                self._message, self._progress(30))

class RemoteEvent(Event):
    """
    An event that was published by another module: we know nothing about
    this, rely on the other module to continuously update us with
    progress information as it emerges.
    """
    def __init__(self, message, refs):
        self._message = message

    def set_progress(self, progress):
        self._progress = progress

    @property
    def progress(self):
        return progress


class PgRecoveryEvent(Event):
    """
    An event whose completion is determined by the recovery of a set of
    PGs to a healthy state.

    Always call update() immediately after construction.
    """
    def __init__(self, message, refs, which_pgs, evactuate_osds):
        super(PgRecoveryEvent, self).__init__(message, refs)

        self._pgs = which_pgs

        self._evacuate_osds = evactuate_osds

        self._original_pg_count = len(self._pgs)

        self._original_bytes_recovered = None

        self._progress = 0.0

        self.uuid = str(uuid.uuid4())

    def update(self, pg_dump, log):
        # FIXME: O(pg_num) in python
        # FIXME: far more fields getting pythonized than we really care about
        pg_to_state = dict([(p['pgid'], p) for p in pg_dump['pg_stats']])

        if self._original_bytes_recovered is None:
            self._original_bytes_recovered = {}
            for pg in self._pgs:
                pg_str = str(pg)
                log.debug(json.dumps(pg_to_state[pg_str], indent=2))
                self._original_bytes_recovered[pg] = pg_to_state[pg_str]['stat_sum']['num_bytes_recovered']

        complete_accumulate = 0.0

        # Calculating progress as the number of PGs recovered divided by the original
        # where partially completed PGs count for something between 0.0-1.0.
        # This is perhaps less faithful than lookign at the total number of bytes
        # recovered, but it does a better job of representing the work still
        # to do if there are a number of very few-bytes PGs that still need
        # the housekeeping of their recovery to be done.
        # This is subjective...

        complete = set()
        for pg in self._pgs:
            pg_str = str(pg)
            info = pg_to_state[pg_str]
            state = info['state']

            states = state.split("+")

            unmoved = bool(set(self._evacuate_osds) & (set(info['up']) | set(info['acting'])))

            if "active" in states and "clean" in states and not unmoved:
                complete.add(pg)
            else:
                # FIXME: handle apparent negative progress (e.g. if num_bytes_recovered goes backwards)
                # FIXME: handle apparent >1.0 progress (num_bytes_recovered incremented by more than num_bytes)
                if info['stat_sum']['num_bytes'] == 0:
                    # Empty PGs are considered 0% done until they are
                    # in the correct state.
                    pass
                else:
                    recovered = info['stat_sum']['num_bytes_recovered']
                    total_bytes = info['stat_sum']['num_bytes']
                    ratio = float(recovered -
                            self._original_bytes_recovered[pg]) / (total_bytes)

                    complete_accumulate += ratio

        self._pgs = list(set(self._pgs) ^ complete)
        completed_pgs = self._original_pg_count - len(self._pgs)
        self._progress = (completed_pgs + complete_accumulate) / self._original_pg_count

        log.info("Updated progress to {0} ({1})".format(
            self._progress, self._message
        ))

    @property
    def progress(self):
        return self._progress


class PgId(object):
    def __init__(self, pool_id, ps):
        self._pool_id = pool_id
        self._ps = ps

    def __cmp__(self, other):
        return (self._pool_id, self._ps) == (other._pool_id, other._ps)

    def __lt__(self, other):
        return (self._pool_id, self._ps) < (other._pool_id, other._ps)

    def __str__(self):
        return "{0}.{1:x}".format(self._pool_id, self._ps)


class Module(MgrModule):
    COMMANDS = [
        {"cmd": "progress",
         "desc": "Show progress of recovery operations",
         "perm": "r"},
        {"cmd": "progress clear",
         "desc": "Reset progress tracking",
         "perm": "rw"}
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)

        self._events = {}
        self._old_osd_map = None

        self._ready = threading.Event()
        self._shutdown = threading.Event()

        self._latest_osdmap = None

    def _osd_out(self, old_map, old_dump, new_map, osd_id):
        affected_pgs = []
        for pool in old_dump['pools']:
            pool_id = pool['pool']
            for ps in range(0, pool['pg_num']):
                self.log.debug("pool_id, ps = {0}, {1}".format(
                    pool_id, ps
                ))
                up_acting = old_map.pg_to_up_acting_osds(pool['pool'], ps)
                self.log.debug("up_acting: {0}".format(json.dumps(up_acting, indent=2)))
                if osd_id in up_acting['up'] or osd_id in up_acting['acting']:
                    affected_pgs.append(PgId(pool_id, ps))

        self.log.warn("{0} PGs affected by osd.{1} going out".format(
            len(affected_pgs), osd_id))

        if len(affected_pgs) == 0:
            # Don't emit events if there were no PGs
            return

        # TODO: reconcile with any existing event referring to this OSD going out
        ev = PgRecoveryEvent(
            "Rebalancing after OSD {0} marked out".format(osd_id),
            refs=[("osd", osd_id)],
            which_pgs=affected_pgs,
            evactuate_osds=[osd_id]
        )
        ev.update(self.get("pg_dump"), self.log)
        self._events[ev.uuid] = ev

    def _osdmap_changed(self, old_osdmap, new_osdmap):
        old_dump = old_osdmap.dump()
        new_dump = new_osdmap.dump()

        old_osds = dict([(o['osd'], o) for o in old_dump['osds']])

        for osd in new_dump['osds']:
            osd_id = osd['osd']
            new_weight = osd['in']
            if new_weight == 0.0:
                old_weight = old_osds[osd_id]['in']

                if old_weight > new_weight:
                    self.log.warn("osd.{0} marked out".format(osd_id))
                    self._osd_out(old_osdmap, old_dump, new_osdmap, osd_id)

    def notify(self, notify_type, notify_data):
        self._ready.wait()

        if notify_type == "osd_map":
            old_osdmap = self._latest_osdmap
            self._latest_osdmap = self.get_osdmap()

            self.log.info("Processing OSDMap change {0}..{1}".format(
                old_osdmap.get_epoch(), self._latest_osdmap.get_epoch()
            ))
            self._osdmap_changed(old_osdmap, self._latest_osdmap)
        elif notify_type == "pg_summary":
            data = self.get("pg_dump")
            for ev_id, ev in self._events.items():
                ev.update(data, self.log)
        else:
            self.log.debug("Ignore notification '{0}'".format(notify_type))
            pass

    def serve(self):
        self._latest_osdmap = self.get_osdmap()
        self.log.info("Loaded OSDMap, ready.")
        self._ready.set()

        # Workaround for notifications only going to modules with a serve()
        self._shutdown.wait()

    def shutdown(self):
        self._shutdown.set()

    def update(self, ev_id, ev_msg, ev_progress):
        """
        For calling from other mgr modules
        """
        try:
            ev = self._events[ev_id]
        except KeyError:
            ev = RemoteEvent(ev_msg)
            self._events[ev_id] = ev
            self.log.info("update: starting ev {0} ({1})".format(
                ev_id, ev_msg))
        else:
            self.log.debug("update: {0} on {1}".format(
                ev_progress, ev_msg))

        ev.set_progress(ev_progress)

    def complete(self, ev_id):
        """
        For calling from other mgr modules
        """
        try:
            ev = self._events[ev_id]
            self.log.info("complete: finished ev {0} ({1})".format(ev_id,
                ev.message))
            del self._events[ev_id]
        except KeyError:
            self.log.warn("complete: ev {0} does not exist".format(ev_id))
            pass

    def _handle_ls(self):
        if len(self._events):
            out = ""
            for ev_id, ev in self._events.items():
                out += ev.twoline_progress()
            return 0, out, ""
        else:
            return 0, "", "Nothing in progress"

    def _handle_clear(self):
        self._events = {}
        return 0, "", ""

    def handle_command(self, cmd):
        if cmd['prefix'] == "progress":
            return self._handle_ls()
        if cmd['prefix'] == "progress clear":
            # The clear command isn't usually needed - it's to enable
            # the admin to "kick" this module if it seems to have done
            # something wrong (e.g. we have a bug causing a progress event
            # that never finishes)
            return self._handle_clear()
        else:
            raise NotImplementedError(cmd['prefix'])
