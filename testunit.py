#! /usr/bin/env python2

import unittest
import re
import mininet

class ChordTestUnit(unittest.TestCase):

    def test_chord_setUp(self):
        # Create a simple network topology
        net = Mininet()
        h1 = net.addHost('h1')
        h2 = net.addHost('h2')
        h3 = net.addHost('h3')
        h4 = net.addHost('h4')
        h5 = net.addHost('h5')
        h6 = net.addHost('h6')
        s1 = net.addSwitch('s1')
        c0 = net.addController('c0')

        # Connect host to switch
        net.addLink(h1, s1)
        net.addLink(h2, s1)
        net.addLink(h3, s1)
        net.addLink(h4, s1)
        net.addLink(h5, s1)
        net.addLink(h6, s1)

        # Start the network
        net.start()

        # Create Node1 on h1, wich doesnt known about any other node
        h1.cmd('python dht/handler.py -i 10.0.0.1 -p 8000')

        # Create other nodes and join them to the ring
        h2.cmd('python dht/handler.py -i 10.0.0.2 -p 8000 -t 10.0.0.1:8000')
        h3.cmd('python dht/handler.py -i 10.0.0.3 -p 8000 -t 10.0.0.1:8000')
        h4.cmd('python dht/handler.py -i 10.0.0.4 -p 8000 -t 10.0.0.1:8000')
        h5.cmd('python dht/handler.py -i 10.0.0.5 -p 8000 -t 10.0.0.1:8000')
        h6.cmd('python dht/handler.py -i 10.0.0.6 -p 8000 -t 10.0.0.1:8000')
