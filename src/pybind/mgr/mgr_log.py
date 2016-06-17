
import logging

# TODO: plumb python logging into C++ logging so that we have one unified
# log from the ceph-mgr service and all the rotation/perms can be handled
# by the C++ service's packaging.


log = logging.getLogger("rest")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())
log.addHandler(logging.FileHandler("./rest.log"))
