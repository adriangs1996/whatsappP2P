'''
CHORD Protocol Implementation for WhatsappP2P
'''
import socket
import logging
import re
from hashlib import sha1
from threading import Thread
from time import sleep

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s", level=logging.DEBUG)

URL_REGEX = re.compile(r'chord://(?P<host>[A-Za-z]+):(?P<port>[1-9][0-9]{3,4})')
KEY_SIZE = 160
MAX_KEY = 2**KEY_SIZE
SLEEP_TIME = 5

class KeyAddress(tuple):
    pass

class KeySelf(KeyAddress):
    pass

class ClientAddress(tuple):
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

        # Periodically calls stabilize
        Thread(target=self.periodically_stabilize).start()

        # Periodicallye calls fix_fingers
        Thread(target=self.periodically_fix_fingers).start()

        # Periodically calls check_predecessor
        Thread(target=self.periodically_check_predecessor).start()

        # Start RPC server
        self.__serve_rpc()

    def find_succesor(self, key):
        '''
        Find a new succesor for Node with identifier "key" if possible.
        '''
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
        '''
        Return the closest preceding node of "key" if exist, returns
        self otherwise.
        '''
        for i in range(KEY_SIZE - 1, -1, -1):
            if self.finger[i][Node.Id] in range(self.identifier + 1, key):
                return self.finger[i]

        return KeySelf([self.ip, self.port, self.identifier])

    def join(self, url):
        '''
        Connect peer to a CHORD ring given the address of a known CHORD peer.
        '''
        self.succesor = request(url, 'find_succesor', self.identifier)
        self.predecesor = None

    def stabilize(self):
        '''
        Verifies node's inmediate successor and notify it about us.
        '''
        identifier = request("chord://%s:%d" % (self.succesor[Node.Ip], self.succesor[Node.Port]), 'get_predecessor', self.succesor[Node.Id])
        logging.debug("[+] Request predecessor from %s:%d" % (self.succesor[Node.Ip], self.succesor[Node.Port]))
        if identifier and identifier[Node.Id] in range(self.identifier, self.succesor[Node.Id]):
            self.succesor = identifier
        request("chord://%s:%d" % (self.succesor[Node.Ip], self.succesor[Node.Port]), 'notify', self.node)

    def periodically_stabilize(self):
        '''
        Runs stabilize periodically.
        '''
        while True:
            sleep(SLEEP_TIME)
            logging.debug("[*] Running stabilize")
            self.stabilize()

    def periodically_fix_fingers(self):
        '''
        Runs fix fingers periodically.
        '''
        while True:
            sleep(SLEEP_TIME)
            logging.debug("[*] Running fix_fingers")
            self.fix_fingers()

    def periodically_check_predecessor(self):
        '''
        Runs check_predecessor periodically.
        '''
        while True:
            sleep(SLEEP_TIME)
            logging.debug("[*] Running check_predecessor")
            self.fix_fingers()

    def check_predecessor(self):
        '''
        Checks wheter predeces has failed.
        '''
        if self.predecesor and not request("chord://%s:%d" % (self.predecesor[Node.Ip], self.succesor[Node.Port]), 'ping', self.identifier):
            self.predecesor = None

    def notify(self, node):
        '''
        "node" thinks it might be our predecesor.
        '''
        if self.predecesor is None or node[Node.Id] in range(node[Node.Id] + 1, self.identifier):
            self.predecesor = node
        return None

    def get_predecessor(self, key=None):
        '''
        Wrapper around predecesor property to serve a RPC.
        '''
        return self.predecesor

    def fix_fingers(self):
        '''
        Refreshes finger table entries.
        '''
        nxt = self.next_finger + 1
        if nxt > KEY_SIZE:
            nxt = 1
        self.finger[nxt] = self.find_succesor(self.identifier + 2**(nxt - 1))

    def get(self, key):
        '''
        Returns value associated with key if we are responsible for it and we have it.
        '''
        return self.storage.get(key, default=None)

    def put(self, key, val):
        '''
        Updates/Defines a value associated with a key.
        '''
        self.storage[key] = val

    def __dispatch_rpc(self, action, key, val = None):
        assert hasattr(self, action)
        func = getattr(self, action)
        if val is None:
            return func(key)
        return func(key, val)

    def __serve_rpc(self):
        rpc_sock = socket.socket()
        rpc_sock.bind((self.ip, self.port))
        while True:
            client_sock, address = rpc_sock.accept()
            logging.debug('Receiving rpc request from %s:%d' % address)
            try:
                client_fd = client_sock.makefile('rb')
                req = client_fd.readlines()
                val = None

                if req.startswith(b'notify'):
                    action, ip, port, identifier = req.split()
                    key = KeyAddress([ip.decode(), int(port.decode()), identifier.decode()])

                elif req.startswith(b'put'):
                    action, key, ip, port = req.split()
                    key = hex(int(key.decode()))
                    val = ClientAddress([ip.decode(), int(port.decode())])

                else:
                    action, key = req.split()

                if val is None:
                    result = self.__dispatch_rpc(action.decode(), hex(int(key.decode())))

                else:
                    result = self.__dispatch_rpc(action.decode(), key, val)

                client_fd = client_sock.makefile('wb')
                if result is None:
                    client_fd.write(b'none')
                elif result == b'alive':
                    client_fd.write(b'alive')
                elif isinstance(result, (KeyAddress, KeySelf)):
                    client_fd.write(bytes("peer %s %d %x" % (result[Node.Ip], result[Node.Port], result[Node.Id])))
                elif isinstance(result, ClientAddress):
                    client_fd.write(bytes("address %s %d" % (result[Node.Ip], result[Node.Port])))
            finally:
                client_sock.close()
