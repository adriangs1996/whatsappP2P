'''
CHORD Protocol Implementation for WhatsappP2P
'''
import logging
import re
from hashlib import sha1
from threading import Thread, BoundedSemaphore
from time import sleep
import zmq

blacklist_mutex = BoundedSemaphore()
rpc_mutex = BoundedSemaphore()

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
SLEEP_TIME = 3

context = zmq.Context()


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
    if node_left <= node_right:
        return node_left <= node <= node_right

    # handle the 'ring' case
    return node_left <= node or node <= node_right


def __connect_node(host, port):
    sock = context.socket(zmq.REQ)
    sock.connect('tcp://%s:%s' % (host, port))
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
def request(url, action, key, timeout=100, tries=8):
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
        try:
            body['message'] = key[3]
        except IndexError:
            pass
    else:
        body = {'action': action, 'key': key}
    try:
        sock.send_json(body)

        while tries:
            # Wait for an incoming response
            if sock.poll(timeout=timeout, flags=zmq.POLLIN):
                response = sock.recv_json()
                break
            tries -= 1
            timeout *= 2

        if not tries:
            sock.close()
            raise NoResponseException()

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

        self.finger = [None] * KEY_SIZE
        self.storage = {}
        self.ip_address = ip_address
        self.port = port
        self.node = KeySelf([self.ip_address, self.port, self.identifier])
        self.succesor = []
        self.queued_messages = {}
        self.predecesor = None
        self.next_finger = 0
        self.lock = BoundedSemaphore()
        logging.debug("** Node %s:%d with key %d is online and ready **" % (
            self.ip_address, self.port, self.identifier
            ))

        if dest_host is not None:
            self.join("chord://%s:%d" % dest_host)
        else:
            self.finger[0] = self.node

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
        result = self.node
        for node in self.succesor:
            try:
                request(
                    "chord://%s:%d" % (node[Node.Ip], node[Node.Port]),
                    "ping",
                    self.identifier,
                    tries=1
                )
                # self.finger[0] = node
                return node

            except NoResponseException:
                logging.debug("Failed")
                blacklist.append(node)

        self.succesor = [nod for nod in self.succesor if nod not in blacklist]

        # for black_node in blacklist:
        #     try:  # race condition here
        #         logging.debug("Deleting {} from successor list".format(black_node))
        #         self.succesor.remove(black_node)
        #         logging.debug("Removed {}".format(black_node))
        #     except ValueError:  # another process already remove it
        #         logging.debug("Not removing it")
        #     finally:
        #         logging.debug("This is the blacklist {}".format(blacklist))
        return result

    def find_succesor(self, key):
        '''
        Find a new succesor for Node with identifier "key" if possible.
        '''
        # if our succesor is responsible for key, return it
        succesor = self._get_live_succesor()
        if succesor != self.node and _in_interval(
            key,
            (self.identifier + 1) % 2**KEY_SIZE,
            succesor[Node.Id]
        ):
            return succesor

        # otherwise, look for the closest preceding node of the key and ask
        # for his succesor
        target = self.closest_preceding_node(key)

        if target == self.node:
            return target

        try:
            self.lock.acquire()
            response = request(
                "chord://%s:%d" % (target[Node.Ip], target[Node.Port]),
                'find_succesor',
                key,
                timeout=100,
                tries=1
            )
            self.lock.release()
            if response:
                return response
        except NoResponseException:
            return self.node

    def closest_preceding_node(self, key):
        '''
        Return the closest preceding node of "key" if exist, returns
        self otherwise.
        '''
        for i in range(KEY_SIZE - 1, -1, -1):
            if self.finger[i] and _in_interval(
                    self.finger[i][Node.Id],
                    (self.identifier + 1) % 2**KEY_SIZE,
                    (key - 1) % 2**KEY_SIZE
            ):
                return self.finger[i]

        return self.node

    def join(self, url):
        '''
        Connect peer to a CHORD ring given the address of a known CHORD peer.
        '''
        succ = request(url, 'find_succesor', self.identifier, timeout=5000, tries=1)
        self.succesor.append(succ)
        logging.debug("Set {} as our succesor".format(self.succesor[0]))
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
        if succesor != self.node:
            try:
                identifier = request(
                    "chord://%s:%d" % (succesor[Node.Ip], succesor[Node.Port]),
                    'get_predecessor',
                    succesor[Node.Id],
                    tries=1
                )
            except NoResponseException:
                identifier = None

            if identifier and identifier != self.node and _in_interval(
                identifier[Node.Id],
                (self.identifier + 1) % 2**KEY_SIZE,
                (succesor[Node.Id] - 1) % 2**KEY_SIZE
            ):
                # Negotiate succesors list with new succesor
                succesor_list = request(
                    "chord://%s:%d" % (identifier[Node.Ip], identifier[Node.Port]),
                    "reconciliate",
                    self.identifier,
                    tries=1
                )

                if len(succesor_list) >= Node.MaxSuccesorsList:
                    succesor_list.pop()
                if identifier not in succesor_list:
                    logging.info("Added {} as our succesor".format(identifier))
                    succesor_list.insert(0, identifier)
                self.succesor = succesor_list

            if identifier and identifier != self.node:
                try:
                    request(
                        "chord://%s:%d" % (
                            identifier[Node.Ip],
                            identifier[Node.Port]
                            ),
                        'notify',
                        self.node,
                        tries=1
                    )
                except NoResponseException:
                    pass

            elif identifier is None:
                try:
                    request(
                        "chord://%s:%d" % (
                            succesor[Node.Ip],
                            succesor[Node.Port]
                        ),
                        'notify',
                        self.node,
                        tries=1
                    )
                except NoResponseException:
                    succesor = None

        if self.predecesor and not self.succesor:
            logging.info("Added {} as our succesor from predecesor".format(self.predecesor))
            try:
                request(
                            "chord://%s:%d" % (
                                self.predecesor[Node.Ip],
                                self.predecesor[Node.Port]
                                ),
                            'notify',
                            self.node,
                            tries=1
                        )
                self.succesor.insert(0, self.predecesor)
            except NoResponseException:
                pass

    def periodically_stabilize(self):
        '''
        Runs stabilize periodically.
        '''
        while True:
            sleep(SLEEP_TIME)
            self.stabilize()

    def periodically_fix_fingers(self):
        '''
        Runs fix fingers periodically.
        '''
        while True:
            sleep(SLEEP_TIME)
            self.fix_fingers()

    def periodically_check_predecessor(self):
        '''
        Runs check_predecessor periodically.
        '''
        while True:
            sleep(SLEEP_TIME)
            self.check_predecessor()

    def check_predecessor(self):
        '''
        Checks whether predeces has failed.
        '''
        try:
            request(
                "chord://%s:%d" % (
                    self.predecesor[Node.Ip],
                    self.predecesor[Node.Port]
                     ),
                'ping', self.identifier,
                tries=1
            )
        except(NoResponseException, TypeError):
            logging.debug("No predecesor found")
            self.predecesor = None

    def notify(self, node):
        '''
        "node" thinks it might be our predecesor.
        '''
        self.check_predecessor()
        if self.predecesor is None or (_in_interval(
            node[Node.Id],
            (self.predecesor[Node.Id] + 1) % 2**KEY_SIZE,
            (self.identifier - 1) % 2**KEY_SIZE
        ) and self.predecesor != node):

            logging.info('Set {} as predecesor in notify'.format(node))
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
        if self.next_finger >= KEY_SIZE:
            self.next_finger = 0
        self.lock.acquire()
        self.finger[self.next_finger] = self.find_succesor(
            self.identifier + 2**(self.next_finger)
        )
        self.lock.release()

    def _single_enqueue_message(self, key, val, message):
        try:
            self.queued_messages[key].append(message)
        except KeyError:
            self.queued_messages[key] = [message]

    def _enqueue_message(self, key, val, message):
        try:
            self.queued_messages[key].append(message)
        except KeyError:
            self.queued_messages[key] = [message]

        for peer in self.succesor:
            try:
                request(
                    "chord://%s:%d" % (peer[Node.Ip], peer[Node.Port]),
                    "_single_enqueue_message",
                    (val[0], val[1], key, message),
                    tries=1
                )
            except NoResponseException:
                pass

    def _dequeue_messages(self, key):
        result = self.queued_messages.get(key, default=None)
        if result is not None:
            self.queued_messages[key].clear()
        return result

    def dequeue_messages(self, key):
        peer = self.closest_preceding_node(key)

        request(
            "chord:%s:%d" % (peer[Node.Ip], peer[Node.Port]),
            "_dequeue_messages",
            key,
            tries=1
        )

    def enqueue_message(self, key, val, message):
        peer = self.closest_preceding_node(key)

        request(
            "chord://%s:%d" % (peer[Node.Ip], peer[Node.Port]),
            "_enqueue_message",
            (val[0], val[1], key, message)
        )

    def _get(self, key):
        return self.storage.get(key, default=None)

    def get(self, key):
        '''
        Returns value associated with key if we are responsible for it and we
        have it.
        '''
        peer = self.closest_preceding_node(key)
        result = request(
            "chord://%s:%d" % (peer[Node.Ip], peer[Node.Port]),
            "_get",
            key
        )
        return result

    def put(self, key, val):
        peer = self.closest_preceding_node(key)

        request(
            "chord://%s:%d" % (peer[Node.Ip], peer[Node.Port]),
            "_put",
            (val[0], val[1], key)
        )

    def _single_put(self, key, val):
        '''
        Update/ Defines a value associated with a key, only on this node
        '''
        self.storage[key] = val

    def _put(self, key, val):
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
            return result
        return func(key, val)

    def __serve_rpc(self):

        context = zmq.Context()
        rpc_sock = context.socket(zmq.REP)
        rpc_sock.bind('tcp://*:%s' % self.port)

        while True:
            req = rpc_sock.recv_json()
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
