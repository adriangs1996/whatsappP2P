import unittest
from random import randrange
from dht.chord import request, Node, RemoteNodeReference
from time import sleep
from tracker import ClientInformationTracker, request_tracker_action
from hashlib import sha1
import os


class DHTTest(unittest.TestCase):

    def test_dht_put_and_get(self):
        address_list = [
            ("127.0.0.1", x) for x in set(
                map(lambda y: randrange(7000, 8000),
                    range(5))
            )
        ]

        nodes = []
        nodes.append(Node(address_list[0][0], address_list[0][1]))
        nodes[0].start_service()
        for host, port in address_list[1:]:
            print("Creating node %s:%s" % (host, port))
            i = randrange(len(nodes))
            n = Node(host, port, dest_host=(nodes[i].ip, nodes[i].port))
            nodes.append(n)
            n.start_service()
            sleep(1)
        print("Stabilizing")
        sleep(5)
        
        i = randrange(len(nodes))
        print("Creating Tracker")
        trac = ClientInformationTracker('127.0.0.1', 9999, [(nodes[i].ip, nodes[i].port)])
        trac.start_services()
        
        seeds = [
            ('12.10.92.87', x) for x in set(
                map(lambda y: randrange(8000, 9000), range(6))
            )
        ]
        for h in seeds:
            print(f"Putting {h}")
            self.assertTrue(
                request_tracker_action(
                    '127.0.0.1',
                    9999,
                    'register_client',
                    user="%s:%s" % (h[0], h[1]),
                    ip=h[0],
                    port=h[1]
                )
            )
            sleep(1)

        for h in seeds:
            print(f"retrieving {h}")
            self.assertEqual(
                h,
                request_tracker_action(
                    '127.0.0.1',
                    9999,
                    'locate',
                    user="%s:%s" % (h[0], h[1]),
                )
            )
            sleep(1)

if __name__ == '__main__':
    unittest.main()
