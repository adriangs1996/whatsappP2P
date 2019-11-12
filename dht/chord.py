'''
CHORD Protocol Implementation for WhatsappP2P
'''
import logging
import re
from hashlib import sha1
from threading import Thread, BoundedSemaphore
from time import sleep
import zmq

REQ = zmq.REQ
REP = zmq.REP

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s", level=logging.DEBUG)

URL_REGEX = re.compile(r'chord://(?P<host>([A-Za-z0-9]|\.)+):(?P<port>[1-9][0-9]{3,4})')
KEY_SIZE = 160
MAX_KEY = 2**KEY_SIZE
SLEEP_TIME = 5

class KeyAddress(tuple):
    '''
    Represent the responding Node Address
    '''
    pass

class KeySelf(KeyAddress):
    '''
    Represent the address of the requesting Node
    '''
    pass

class ClientAddress(tuple):
    '''
    Rpresents the address of a client to store in the DHT
    '''
    pass

def __connect_node(host, port):
    context = zmq.Context()
    sock = context.socket(REQ)
    sock.connect('tcp://%s:%d' % (host, port))

    logging.debug('Connecting to host %s on port %d' % (host, port))

    return sock

def __parse_peer(data):
    return KeyAddress(
        [
            data['ip'],
            data['port'],
            data['id']
        ]
    )

# RPC PROTOCOL
def request(url, action, key):
    '''
    Request a procedure on node identified by url.
    '''
    url_dict = URL_REGEX.match(url).groupdict()
    host, port = url_dict['host'], int(url_dict['port'])

    sock = __connect_node(host, port)

    if isinstance(key, (KeyAddress, KeySelf)):
        body = {'action':action, 'ip':key[Node.Ip], 'port':key[Node.Port], 'id':key[Node.Id]}
    else:
        body = {'action': action, 'key': key}
    try:
        sock.send_json(body)

        logging.debug('Sended data')

        response = sock.recv_json()

        logging.debug("Response from %s: %s" % (url, response))
        # Parsing the response.

        # Response in case we ask for a node's identifier key
        if response['result'] == 'peer':
            return __parse_peer(response)

        # Response in case of succesfull ping
        if response['result'] == 'alive':
            peer_key = response['id']
            return KeyAddress([host, port, peer_key])

        if response['result'] == 'None':
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

    def __init__(self, ip, port, dest_host=None):
        self.identifier = int(sha1(bytes("%s%d" % (ip, port), 'ascii')).hexdigest(), 16)

        logging.debug("Creating node with id: 0x%x" % self.identifier)

        self.finger = [None] * KEY_SIZE
        self.storage = {}
        self.ip = ip
        self.port = port
        self.node = KeySelf([self.ip, self.port, self.identifier])
        self.succesor = KeySelf([self.ip, self.port, self.identifier])
        self.predecesor = None
        self.next_finger = 1
        self.lock = BoundedSemaphore()
        logging.debug("** Node %s:%d is online and ready **" % (self.ip, self.port))

        if not dest_host is None:
            self.join("chord://%s:%d" % dest_host)

        # Start RPC server
        rpcserver = Thread(target=self.__serve_rpc)
        rpcserver.setDaemon(True)
        rpcserver.start()

        # Periodically calls stabilize
        stabi = Thread(target=self.periodically_stabilize)
        stabi.setDaemon(True)
        stabi.start()

        # Periodicallye calls fix_fingers
        ffingers = Thread(target=self.periodically_fix_fingers)
        ffingers.setDaemon(True)
        ffingers.start()

        # Periodically calls check_predecessor
        cpredecessor = Thread(target=self.periodically_check_predecessor)
        cpredecessor.setDaemon(True)
        cpredecessor.start()

        ffingers.join()
        cpredecessor.join()
        rpcserver.join()

    def find_succesor(self, key):
        '''
        Find a new succesor for Node with identifier "key" if possible.
        '''
        # if our succesor is responsible for key, return it
        if self.succesor and key in range(self.identifier, self.succesor[Node.Id]):
            logging.debug("[+] Succesor is responsable for key %x" % key)
            return self.succesor

        # otherwise, look for the closest preceding node of the key and ask for his succesor
        target = self.closest_preceding_node(key)

        logging.info("Closest preceding node: {}".format(target))

        if isinstance(target, KeySelf):
            return target

        response = request(
            "chord://%s:%d" % (target[Node.Ip], target[Node.Port]),
            'find_succesor',
            key
        )

        if response:
            logging.debug("[+] Received node %s:%d from find_succesor request" % (
                response[Node.Ip],
                response[Node.Port]))
            return response

        logging.error("Couldn't find node responsible for key %x " % key)

    def closest_preceding_node(self, key):
        '''
        Return the closest preceding node of "key" if exist, returns
        self otherwise.
        '''
        for i in range(KEY_SIZE - 1, -1, -1):
            if self.finger[i] and self.finger[i][Node.Id] in range(self.identifier + 1, key):
                return self.finger[i]

        logging.debug("Closest preceding node is self: {}".format(self.node))
        return self.node

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
        identifier = request(
            "chord://%s:%d" % (self.succesor[Node.Ip], self.succesor[Node.Port]),
            'get_predecessor',
            self.succesor[Node.Id]
        )

        logging.debug(
            "[+] Request predecessor from %s:%d" % (self.succesor[Node.Ip],
                                                    self.succesor[Node.Port]))

        if identifier and identifier[Node.Id] in range(self.identifier, self.succesor[Node.Id]):
            self.succesor = identifier

        request(
            "chord://%s:%d" % (self.succesor[Node.Ip], self.succesor[Node.Port]),
            'notify', self.node
        )

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
        Checks whether predeces has failed.
        '''
        if self.predecesor and not request(
                "chord://%s:%d" % (self.predecesor[Node.Ip], self.succesor[Node.Port]),
                'ping', self.identifier):
            self.predecesor = None

        return None

    def notify(self, node):
        '''
        "node" thinks it might be our predecesor.
        '''
        if self.predecesor is None or node[Node.Id] in range(node[Node.Id] + 1, self.identifier):
            logging.info('Set {} as predecesor'.format(node))
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
        self.lock.acquire()
        result = self.storage.get(key, default=None)
        self.lock.release()
        return result

    def put(self, key, val):
        '''
        Updates/Defines a value associated with a key.
        '''
        self.lock.acquire()
        self.storage[key] = val
        self.lock.release()

    def ping(self, key):
        return 'alive'

    def __dispatch_rpc(self, action, key, val=None):

        logging.info('Dispatching action: "%s"' % action)
        assert hasattr(self, action)
        func = getattr(self, action)

        logging.info('Found corresponding procedure. val is {} in __dispatch_rpc'.format(val))

        if val is None:
            result = func(key)
            logging.info('Got result: {}'.format(result))
            return result
        return func(key, val)

    def __serve_rpc(self):

        context = zmq.Context()
        rpc_sock = context.socket(REP)
        rpc_sock.bind('tcp://*:%d' % self.port)

        while True:
            req = rpc_sock.recv_json()

            logging.debug("Received request {}".format(req))

            val = None
            action = req['action']

            if action == 'notify':
                ip, port, identifier = req['ip'], req['port'], req['id']
                key = KeyAddress([ip, port, identifier])

            elif action == 'put':
                key, ip, port = req['key'], req['ip'], req['port']
                val = ClientAddress([ip, port])

            else:
                key = req['key']

            result = self.__dispatch_rpc(action, key, val)

            logging.debug('Result is {}'.format(result))

            if result is None:
                rpc_sock.send_json({'result':'None'})

            elif result == 'alive':
                rpc_sock.send_json({'result':'alive', 'key':self.identifier})

            elif isinstance(result, (KeyAddress, KeySelf)):
                rpc_sock.send_json({
                    'result':'peer',
                    'ip': result[Node.Ip],
                    'port': result[Node.Port],
                    'id': result[Node.Id]
                })

            elif isinstance(result, ClientAddress):
                rpc_sock.send_json({
                    'result': 'address',
                    'ip': result[Node.Ip],
                    'port': result[Node.Port]
                })

            logging.debug('Closing connection')
