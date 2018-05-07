import sys
import os
import errno
from argparse import ArgumentParser
from collections import defaultdict

from swift.common.storage_policy import split_policy_string
from swift.obj.diskfile import get_data_dir
from swift.common.ring import Ring

parser = ArgumentParser()
parser.add_argument('-v', '--verbose', help='line oriented output',
                    default=False, action='store_true')
parser.add_argument('ring', help='specify the ring, infers datadir')
parser.add_argument('devices', help='root of devices tree for node',
                    nargs='?', default='/srv/node')
parser.add_argument('--targets', help='target device',
                    default=None)


def split(seq, n):
    """
    split seq into n pieices
    """
    seq = list(seq)
    for i in range(n):
        yield seq[i::n]


def get_ring_and_datadir(path):
    """
    :param path: path to ring

    :returns: a tuple, (ring, datadir)
    """
    ring_name = os.path.basename(path).split('.')[0]
    base, policy = split_policy_string(ring_name)
    if base == 'object':
        datadir = get_data_dir(policy)
    else:
        datadir = base + 's'
    return Ring(path), datadir


def main():
    args = parser.parse_args()
    device_root = args.devices
    targets = []
    if args.targets:
        targets = args.targets.split(',')

    ring, datadir = get_ring_and_datadir(args.ring)
    dev2parts = defaultdict(set)
    for replica, part2dev in enumerate(ring._replica2part2dev_id):
        for part, device_id in enumerate(part2dev):
            dev2parts[ring.devs[device_id]['device']].add(part)
            #print 'part: %s, device_id: %s' % (part, device_id)

    parts = defaultdict(set)
    same_parts = set()
    for target_dir in targets:
        if not same_parts:
	    same_parts = set(dev2parts[target_dir])
            continue
        same_parts = same_parts.intersection(set(dev2parts[target_dir]))

    print 'Find %d same partitions %s in %s' % (len(same_parts), same_parts, targets)


if __name__ == "__main__":
    sys.exit(main())