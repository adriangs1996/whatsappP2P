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

logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(message)s",
        level=logging.DEBUG
     )

URL_REGEX = re.compile(
    r'chord://(?P<host>([A-Za-z0-9]|\.)+):(?P<port>[1-9][0-9]{3,4})'
    )
KEY_SIZE = 160
MAX_KEY = 2**KEY_SIZE
SLEEP_TIME = 5


class NoResponseException(Exception):
    '''
    Exception raised when send a message and no server response was found after
    a timeout.
    '''
    pass


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
    Represents the address of a client to store in the DHT
    '''
    pass


def _in_interval(node, node_left, node_right):
    # handle normal case (a < b)
    if node_left < node_right:
        return node_left <= node <= node_right

    # handle the 'ring' case
    return node_left <= node or node <= node_right


def __connect_node(host, port):
    context = zmq.Context()
    sock = context.socket(REQ)
    sock.connect('tcp://%s:%d' % (host, port))

    logging.debug('Connecting to host %s on port %d' % (host, port))

    return sock


def __parse_peer(data):
    return KeyAddress(
        [
            data['ip_address'],
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

    if isinstance(key, tuple):
        body = {
            'action': action,
            'ip_address': key[Node.Ip],
            'port': key[Node.Port],
            'id': key[Node.Id]
        }
    else:
        body = {'action': action, 'key': key}
    try:
        sock.send_json(body)

        logging.debug('Sended data')

        tries = 7
        timeout = 1000
        while tries:
            # Wait for an incoming response
            logging.info(
                "Waiting for an incoming response: timeout set to %d secs",
                timeout//1000
                )
            if sock.poll(timeout=timeout, flags=zmq.POLLIN):
                break
            tries -= 1
            timeout *= 2

        if not tries:
            raise NoResponseException()

        response = sock.recv_json()

        logging.debug("Response from %s: %s" % (url, response))
        # Parsing the response.

        # Response in case we ask for a node's identifier key
        if response['result'] == 'peer':
            return __parse_peer(response)

        # Response in case of succesfull ping
        if response['result'] == 'alive':
            peer_key = response['key']
            return KeyAddress([host, port, peer_key])

        if response['result'] == 'None':
            return None

    finally:
        sock.close()
    return response['result']


class Node:
    '''
    Peer of the chord ring. Every peer is identified by its address.
    Once peers are succesfully inserted in the chord ring, they start
    serving RPC forever.
    '''
    Id = 2
    Port = 1
    Ip = 0
    MaxSuccesorsList = 4

    def __init__(self, ip_address, port, dest_host=None):
        self.identifier = int(sha1(bytes(
            "%s%d" % (ip_address, port),
            'ascii')
        ).hexdigest(), 16)

        logging.debug("Creating node with id: 0x%x" % self.identifier)

        self.finger = [None] * KEY_SIZE
        self.storage = {}
        self.ip_address = ip_address
        self.port = port
        self.node = KeySelf([self.ip_address, self.port, self.identifier])
        self.succesor = []
        self.predecesor = None
        self.next_finger = 1
        self.lock = BoundedSemaphore()
        logging.debug("** Node %s:%d is online and ready **" % (
            self.ip_address, self.port
            ))

        if dest_host is not None:
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

    def _get_live_succesor(self):
        blacklist = []
        result = KeySelf([self.ip_address, self.port, self.identifier])

        for node in self.succesor:
            try:

                request(
                    "chord://%s:%d" % (node[Node.Ip], node[Node.Port]),
                    "ping",
                    self.identifier
                )
                result = node

            except NoResponseException:
                blacklist.append(node)
                continue

        for black_node in blacklist:
            self.succesor.remove(black_node)

        return result

    def find_succesor(self, key):
        '''
        Find a new succesor for Node with identifier "key" if possible.
        '''
        # if our succesor is responsible for key, return it
        succesor = self._get_live_succesor()
        if _in_interval(key, self.identifier, succesor[Node.Id]):
            logging.debug("[+] Succesor is responsable for key %x", key)
            return succesor

        # otherwise, look for the closest preceding node of the key and ask
        # for his succesor
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
            logging.debug("[+] Received node %s:%d from find_succesor\
                 request" % (
                response[Node.Ip],
                response[Node.Port]))
            return response

        logging.error("Couldn't find node responsible for key %x ", key)

    def closest_preceding_node(self, key):
        '''
        Return the closest preceding node of "key" if exist, returns
        self otherwise.
        '''
        for i in range(KEY_SIZE - 1, -1, -1):
            if self.finger[i] and _in_interval(
                    self.finger[i][Node.Id],
                    self.identifier + 1,
                    key
            ):
                return self.finger[i]

        logging.debug("Closest preceding node is self: {}".format(self.node))
        return self.node

    def join(self, url):
        '''
        Connect peer to a CHORD ring given the address of a known CHORD peer.
        '''
        self.succesor.append(request(url, 'find_succesor', self.identifier))
        self.predecesor = None

    def reconciliate(self, node=None):
        """
        Wrapper method to obtain the succesor list of a node.
        """
        return self.succesor

    def stabilize(self):
        '''
        Verifies node's inmediate successor and notify it about us.
        '''
        succesor = self._get_live_succesor()
        identifier = request(
            "chord://%s:%d" % (succesor[Node.Ip], succesor[Node.Port]),
            'get_predecessor',
            succesor[Node.Id]
        )

        logging.debug(
            "[+] Request predecessor from %s:%d" % (succesor[Node.Ip],
                                                    succesor[Node.Port]))

        if identifier and _in_interval(
            identifier[Node.Id],
            self.identifier,
            succesor[Node.Id]
              ):
            # Negotiate succesors list with new succesor
            succesor_list = request(
                "chord://%s:%d" % (identifier[Node.Ip], identifier[Node.Port]),
                "reconciliate",
                self.identifier
            )

            if len(succesor_list) >= Node.MaxSuccesorsList:
                succesor_list.pop()
            succesor_list.insert(0, identifier)
            self.succesor = succesor_list

        if identifier:
            try:
                request(
                    "chord://%s:%d" % (
                        identifier[Node.Ip],
                        identifier[Node.Port]
                        ),
                    'notify',
                    self.node
                )
            except NoResponseException:
                pass

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
        try:
            request(
                "chord://%s:%d" % (
                    self.predecesor[Node.Ip],
                    self.succesor[Node.Port]
                     ),
                'ping', self.identifier
            )
        except(NoResponseException, AttributeError):
            self.predecesor = None

    def notify(self, node):
        '''
        "node" thinks it might be our predecesor.
        '''
        if self.predecesor is None or _in_interval(
            node[Node.Id],
            self.predecesor[Node.Id],
            self.identifier
        ):

            logging.info('Set {} as predecesor'.format(node))
            self.predecesor = node

    def get_predecessor(self, key=None):
        '''
        Wrapper around predecesor property to serve a RPC.
        '''
        return self.predecesor

    def fix_fingers(self):
        '''
        Refreshes finger table entries.
        '''
        self.next_finger = self.next_finger + 1
        if self.next_finger > KEY_SIZE:
            self.next_finger = 1
        self.finger[self.next_finger] = self.find_succesor(
            self.identifier + 2**(self.next_finger - 1)
        )

    def get(self, key):
        '''
        Returns value associated with key if we are responsible for it and we
        have it.
        '''
        result = self.storage.get(key, default=None)
        return result

    def _single_put(self, key, val):
        '''
        Update/ Defines a value associated with a key, only on this node
        '''
        self.storage[key] = val

    def put(self, key, val):
        '''
        Updates/Defines a value associated with a key on this node and all his
        succesors.
        '''
        self.storage[key] = val
        for node in self.succesor:
            try:
                request(
                    "chord://%s:%d" % (node[Node.Ip], node[Node.Port]),
                    "_single_put",
                    (val[0], val[1], key)
                )
            except NoResponseException:
                pass

    def ping(self, key):
        return 'alive'

    def __dispatch_rpc(self, action, key, val=None):
        assert hasattr(self, action)
        func = getattr(self, action)

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
                ip_address, port, identifier =\
                    req['ip_address'],\
                    req['port'],\
                    req['id']
                key = KeyAddress([ip_address, port, identifier])

            elif action == 'put':
                key, ip_address, port =\
                    req['id'],\
                    req['ip_address'],\
                    req['port']
                val = ClientAddress([ip_address, port])

            else:
                key = req['key']

            result = self.__dispatch_rpc(action, key, val)

            logging.debug('Result is {}'.format(result))

            if result is None:
                rpc_sock.send_json({'result': 'None'})

            elif isinstance(result, list):
                rpc_sock.send_json({'result': result})

            elif result == 'alive':
                rpc_sock.send_json({'result': 'alive', 'key': self.identifier})

            elif isinstance(result, (KeyAddress, KeySelf)):
                rpc_sock.send_json({
                    'result': 'peer',
                    'ip_address': result[Node.Ip],
                    'port': result[Node.Port],
                    'id': result[Node.Id]
                })

            elif isinstance(result, ClientAddress):
                rpc_sock.send_json({
                    'result': 'address',
                    'ip_address': result[Node.Ip],
                    'port': result[Node.Port]
                })
