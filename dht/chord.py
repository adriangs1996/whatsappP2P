'''
CHORD Protocol Implementation for WhatsappP2P
'''
import socket
import logging
import re
from hashlib import sha1
from threading import Thread

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s", level=logging.DEBUG)

URL_REGEX = re.compile(r'chord://(?P<host>[A-Za-z]+):(?P<port>[1-9][0-9]{3,4})')
KEY_SIZE = 160
MAX_KEY = 2**KEY_SIZE

class KeyAddress(tuple):
    pass

class KeySelf(KeyAddress):
    pass

def __connect_node(host, port):
    sock = socket.socket()
    logging.debug('Connecting to host %s on port %d' % (host, port))
    sock.connect((host, port))
    return sock

def __parse_peer(data):
    if data.startswith(b'peer'):
        host, port, key = data.split()[1:]
        return KeyAddress([host, int(port.decode()), key])
    if data.startswith(b'none'):
        return None
    logging.error("Error found in __parse_peer: Invalid Key")
    raise ValueError("Invalid Key")

# RPC PROTOCOL
def request(url, action, key):
    url_dict = URL_REGEX.match(url).groupdict()
    host, port = url_dict['host'], int(url_dict['port'])

    try:
        sock = __connect_node(host, port)
    except socket.error:
        return False

    if isinstance(key, KeyAddress, KeySelf):
        body = bytes("%s %s %d %x" %(action, key[Node.Ip], key[Node.Port], key[Node.Id]), 'ascii')
    else:
        body = bytes("%s %x" % (action, key), 'ascii')
    try:
        sock.sendall(body)
        sock_file = sock.makefile('rb')
        response: bytes = sock_file.read()

        logging.debug("Response from %s: %s" % (url, response))
        # Parsing the response.

        # Response in case we ask for a node's identifier key
        if response.startswith(b'peer'):
            return __parse_peer(response)

        # Response in case of succesfull ping
        if response.startswith(b'alive'):
            peer_key = int(response.split()[1], base=16)
            return KeyAddress([host, port, peer_key])

        if response.startswith(b'none'):
            return None

    finally:
        sock.close()
    return response


class Node:
    '''
    Peer of the chord ring. Every peer is identified by its address.
    Once peers are succesfully inserted in the chord ring, they start
    serving RPC forever.
    '''
    Id = 2
    Port = 1
    Ip = 0

    def __init__(self, ip, port):
        self.identifier = sha1(bytes("%d%d" % (ip, port))).hexdigest()
        logging.debug("Creating node with id: %x" % self.identifier)
        self.finger = [None] * KEY_SIZE
        self.storage = {}
        self.ip = ip
        self.port = port
        self.node = KeySelf([self.ip, self.port. self.identifier])
        self.succesor = KeySelf([self.ip, self.port. self.identifier])
        self.predecesor = None
        self.next_finger = 1
        logging.debug("** Node %s:%d is online and ready **" % (self.ip, self.port))
        self.__serve_rpc()

    def find_succesor(self, key):

        # if our succesor is responsible for key, return it
        if key in range(self.identifier, self.succesor[Node.Id]):
            logging.debug("[+] Succesor is responsable for key %x" % key)
            return self.succesor

        # otherwise, look for the closest preceding node of the key and ask for his succesor
        target = self.closest_preceding_node(key)
        response = request("chord://%s:%d" % (target[Node.Ip], target[Node.Port]), 'find_succesor', key)
        if response:
            logging.debug("[+] Received node %s:%d from find_succesor request" % (response[Node.Ip], response[Node.Port]))
            return response
        logging.error("Couldn't find node responsible for key %x " % key)
        raise ValueError("Invalid Key")

    def closest_preceding_node(self, key):
        for i in range(KEY_SIZE - 1, -1, -1):
            if self.finger[i][Node.Id] in range(self.identifier + 1, key):
                return self.finger[i]

        return KeySelf([self.ip, self.port, self.identifier])

    def join(self, url):
        self.succesor = request(url, 'find_succesor', self.identifier)
        self.predecesor = None

    def stabilize(self):
        identifier = request("chord://%s:%d" % (self.succesor[Node.Ip], self.succesor[Node.Port]), 'get_predecessor', self.succesor[Node.Id])
        logging.debug("[+] Request predecessor from %s:%d" % (self.succesor[Node.Ip], self.succesor[Node.Port]))
        if identifier[Node.Id] in range(self.identifier, self.succesor[Node.Id]):
            self.succesor = identifier
        request("chord://%s:%d" % (self.succesor[Node.Ip], self.succesor[Node.Port]), 'notify', self.node)

    def check_predecessor(self):
        if self.predecesor and not request("chord://%s:%d" % (self.predecesor[Node.Ip], self.succesor[Node.Port]), 'ping', self.identifier):
            self.predecesor = None

    def notify(self, node):
        if self.predecesor is None or node[Node.Id] in range(node[Node.Id] + 1, self.identifier):
            self.predecesor = node
        return None

    def get_predecessor(self, key=None):
        return self.predecesor

    def fix_fingers(self):
        nxt = self.next_finger + 1
        if nxt > KEY_SIZE:
            nxt = 1
        self.finger[nxt] = self.find_succesor(self.identifier + 2**(nxt - 1))

    def __dispatch_rpc(self, action, key):
        assert hasattr(self, action)
        func = getattr(self, action)
        return func(key)

    def __serve_rpc(self):
        rpc_sock = socket.socket()
        rpc_sock.bind((self.ip, self.port))
        while True:
            client_sock, address = rpc_sock.accept()
            logging.debug('Receiving rpc request from %s:%d' % address)
            try:
                client_fd = client_sock.makefile('rb')
                req = client_fd.readlines()
                if req.startswith(b'notify'):
                    action, ip, port, identifier = req.split()
                    key = KeyAddress([ip.decode(), int(port.decode()), identifier.decode()])
                action, key = req.split()
                result = self.__dispatch_rpc(action.decode(), key.decode())
                client_fd = client_sock.makefile('wb')
                if result is None:
                    client_fd.write(b'none')
                elif result == b'alive':
                    client_fd.write(b'alive')
                elif isinstance(result, (KeyAddress, KeySelf)):
                    client_fd.write(bytes("peer %s %d %x" % (result[Node.Ip], result[Node.Port], result[Node.Id])))
            finally:
                client_sock.close()
