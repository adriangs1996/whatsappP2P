import unittest
from random import randrange
from dht.chord import request, Node, RemoteNodeReference
from time import sleep
from tracker import ClientInformationTracker, request_tracker_action
from hashlib import sha1
import os


class DHTTest(unittest.TestCase):

    def test_dht_consistency(self):
        print("Testing for consistency in DHT")
        # Create a Random DHT of 20 nodes
        address_list = [
            ("127.0.0.1", x) for x in set(
                map(lambda y: randrange(7000, 8000),
                    range(10))
            )
        ]
        nodes = []
        nodes.append(Node(address_list[0][0], address_list[0][1]))
        nodes[0].start_service()  # Bootstrap node
        for host, port in address_list[1:]:
            print("Creating node: %s:%s" % (host, port))
            i = randrange(len(nodes))
            n = Node(host, port, dest_host=(nodes[i].ip, nodes[i].port))
            nodes.append(n)
            n.start_service()
            sleep(0.3)
        # Give time for stabilization
        for i in range(400):
            print("Stabilization time " + f"{int(i/400 * 100)}%", end='\r')
            sleep(0.1)
        print("\nDone")

        print("Seeding DHT with some Keys")
        # Generate random addresess and hash them
        seeds = [
            ('12.10.92.87', x) for x in set(
                map(lambda y: randrange(8000, 9000), range(6))
            )
        ]
        hashes = [
            int(
                sha1(
                    bytes("%s:%d" % x, 'ascii')).hexdigest(),
                16
            )
            for x in seeds
        ]
        for h in hashes:
            print(f"Putting {h}")
            i = randrange(len(nodes))
            node = RemoteNodeReference(nodes[i].ip, nodes[i].port)
            node.put(h, "Hello There")
            self.assertTrue(True)
            print("Putted")
            sleep(3)

        print("Correctly added keys")
        print("Waiting to stabilize again")
        sleep(3)


if __name__ == '__main__':
    unittest.main()
