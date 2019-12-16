from mininet.net import Mininet
from mininet.node import Controller
from mininet.log import setLogLevel, info, debug, error
from mininet.cli import CLI
from mininet.topo import Topo

from mininet.node import Node


class LinuxRouter(Node):
    """"A Node with IP forwarding enabled."""

    def config(self, **params):
        super(LinuxRouter, self).config(**params)
        # Enable forwarding on the router
        self.cmd('sysctl net.ipv4.ip_forward=1')

    def terminate(self):
        self.cmd('sysctl net.ipv4.ip_forward=0')
        super(LinuxRouter, self).terminate()


class RouterTopo(Topo):
    "A LinuxRouter connecting three IP subnets"

    def build(self):
        """
        default_ip: IP address for r0-eth1
        """
        default_ip = '10.0.0.1/24'  # IP address for r0-eth1
        clients_n = 3
        servers_n = 4

        ip, _ = default_ip.split('/')

        default_route = 'via %s' % ip

        router = self.addNode('r1', cls=LinuxRouter, ip=default_ip)
        s1 = self.addSwitch('s1')

        clients = [self.addHost('client{0}-.{1}'.format((i + 1), (i + 2)), ip = '10.0.0.%i' % (i + 2), defaultRoute = default_route) for i in range(clients_n)]
        servers = [self.addHost('server{0}-.{1}'.format((i + 1), (i + 2 + clients_n)), ip = '10.0.0.%i' % (i + 2 + clients_n), defaultRoute = default_route) for i in range(servers_n)]

        self.addLink(s1, router, intfName2='r1-eth1', params2={'ip': default_ip})

        for i in range(clients_n):
            self.addLink(clients[i], s1)

        for i in range(servers_n):
            self.addLink(servers[i], s1)


topos = {'topo': (lambda: RouterTopo())}
