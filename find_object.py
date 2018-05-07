#!/opt/ss/bin/python

import sys
from swift.common.internal_client import InternalClient
from swift.common.storage_policy import POLICIES

if len(sys.argv) < 4:
    print("Usage: %s <account> <container> <object> [y]")
    sys.exit()

account = sys.argv[1]
container = sys.argv[2]
obj = sys.argv[3]
post_container = False

if len(sys.argv) == 5:
    if sys.argv[4] in ['y', 'Y', 'yes', 'YES']:
        post_container = True

client = InternalClient('/etc/swift/internal-client.conf', 'check-cont', 3)

for p in POLICIES:
    print('Checking policy name: %s (%d)' % (p.name, p.idx))

    headers = { 'X-Backend-Storage-Policy-Index': p.idx}
    meta  = client.get_object_metadata(account, container, obj,
                                       headers=headers,
                                       acceptable_statuses=(2, 4))

    if 'x-timestamp' in meta:
        print('  >> Find object %s in policy %s' % (obj, p.name) )
        if post_container:
            print('create container in policy %s' % p.name )
            headers = { 'X-Storage-Policy': p.name}
            client.create_container(account, container, headers)
            break
    else:
        print('  >> Can not find %s in policy %s' % (obj, p.name))

