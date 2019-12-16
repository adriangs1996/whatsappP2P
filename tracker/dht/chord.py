import socket
from hashlib import sha1
from time import sleep
import threading
import logging
import random
import re
from cloudpickle import dumps, loads

logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(message)s",
        level=logging.DEBUG
     )

KEY_SIZE = 160
MAX_TRIES = 5
DATA_RECV = 1024
STABILIZE_INTERVAL = 1
FINGERS_INTERVAL = 4
UPDATE_SUCCESORS_INTERVAL = 1
MAX_SUCCESORS = 7  # log2(160) ~ 7
URL_REGEX = re.compile(
    r'chord://(?P<host>([A-Za-z0-9]|\.)+):(?P<port>[1-9][0-9]{3,4})'
    )


# RPC PROTOCOL
def request(url, action, *args):
    '''
    Request a procedure on node identified by url.
    '''
    url_dict = URL_REGEX.match(url).groupdict()
    host, port = url_dict['host'], int(url_dict['port'])

    # TODO: WRAPP AROUND THIS TO ALLOW ACTIONS IN CALLBACKS
    # Create a remote Object to represent the connection
    remote = RemoteNodeReference(host, port)
    if hasattr(remote, action):
        func = getattr(remote, action)
        return func(*args)
    else:
        raise Exception("Invalid Request")


def between(c, a, b):
    a = a % 2**KEY_SIZE
    b = b % 2**KEY_SIZE
    c = c % 2**KEY_SIZE
    if a < b:
        return a <= c < b
    return a <= c or c < b


class NoResponseException(Exception):
    pass


def repeat_after_time(sleepTime):
    def func_wrapper(func):
        def inner(self, *args, **kwargs):
            while 1:
                sleep(sleepTime)
                ret = func(self, *args, **kwargs)
                if not ret:
                    return
        return inner
    return func_wrapper


def repeat_when_socket_fail(retries):
    def func_wrapper(func):
        def inner(self, *args, **kwargs):
            retry_count = 0
            while retry_count < retries:
                try:
                    ret = func(self, *args, **kwargs)
                    return ret
                except socket.error:
                    sleep(2**retry_count)
                    retry_count += 1
            if retry_count == retries:
                raise NoResponseException
        return inner
    return func_wrapper


def safe_connection_required(func):
    def inner(self, *args, **kwargs):
        # Make a Thread safe socket connection
        self.lock.acquire()
        self.open_connection()
        ret = func(self, *args, **kwargs)
        self.close_connection()
        self.lock.release()
        return ret
    return inner


class RemoteNodeReference(object):
    '''
    Reference to a remote node to wrapp RPC and ease the implementation
    '''
    def __init__(self, node_ip, node_port):
        self.ip = node_ip
        self.port = node_port
        self.lock = threading.BoundedSemaphore()

    def __str__(self):
        return f"<{self.ip}:{self.port}>"

    def __repr__(self):
        return str(self)

    def id(self, offset=0):
        digest = int(
            sha1(
                bytes("%s:%d" % (self.ip, self.port), 'ascii')
                ).hexdigest(),
            16)
        return (digest + offset) % 2**(KEY_SIZE)

    def open_connection(self):
        self.socket_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_.connect((self.ip, self.port))

    def close_connection(self):
        self.socket_.close()
        self.socket_ = None

    def send(self, data):
        self.socket_.sendall(dumps(data) + b"!!")
        self.trace = data

    def recv(self):
        data = b''
        while 1:
            rec = self.socket_.recv(64)
            if rec[-2:] == b"!!":
                data += rec[:-2]
                break
            data += rec
        return data

    def ping(self):
        try:
            sock = socket.socket()
            sock.connect((self.ip, self.port))
            sock.sendall(b"pinging!!")
            sock.close()
            return True
        except socket.error:
            return False

    @safe_connection_required
    def get_succesors(self):
        self.send(["get_succesors"])
        response = self.recv()
        if response == b"":
            return []
        response = loads(response)
        return map(lambda x: RemoteNodeReference(x[0], x[1]), response)

    @safe_connection_required
    def succesor(self):
        self.send(["succesor"])
        response = self.recv()
        response = loads(response)
        return RemoteNodeReference(response[0], response[1])

    @safe_connection_required
    def predecessor(self):
        self.send(["predecessor"])
        response = loads(self.recv())
        return RemoteNodeReference(response[0], response[1])

    @safe_connection_required
    def closest_preceding_node(self, key):
        self.send(["closest_preceding_node", key])
        response = loads(self.recv())
        return RemoteNodeReference(response[0], response[1])

    @safe_connection_required
    def find_succesor(self, key):
        self.send(["find_successor", key])
        response = loads(self.recv())
        return RemoteNodeReference(response[0], response[1])

    @safe_connection_required
    def notify(self, node):
        remote_node_reference = RemoteNodeReference(node.ip, node.port)
        remote_node_reference.lock = None
        self.send(['notify', remote_node_reference])

    @safe_connection_required
    def simple_put(self, key, val):
        self.send(["simple_put", key, val])

    @safe_connection_required
    def put(self, key, val):
        self.send(["put", key, val])

    @safe_connection_required
    def get(self, key):
        self.send(["get", key])
        response = loads(self.recv())
        return response

    @safe_connection_required
    def enqueue_message(self, key, msg):
        self.send(["enqueue_message", key, msg])

    @safe_connection_required
    def simple_enqueue(self, key, msg):
        self.send(["simple_enqueue", key, msg])

    @safe_connection_required
    def simple_dequeue(self, key):
        self.send(["simple_dequeue", key])

    @safe_connection_required
    def dequeue_messages(self, key):
        self.send(["dequeue_messages", key])
        msg_list = loads(self.recv())
        return msg_list

    @safe_connection_required
    def get_keys(self, key):
        self.send(["get_keys", key])
        key_val, key_msg = loads(self.recv())
        return key_val, key_msg

    @safe_connection_required
    def remove_key(self, key):
        self.send(["remove_key", key])


# TODO: Agregar logica para almacenar las llaves, y negociarlas con la entrada\
    #  de nuevos nodos
class Node:
    def __init__(self, node_ip, node_port, dest_host=None):
        self.ip = node_ip
        self.port = node_port
        self.succesors = []
        self._predecessor = None
        self.fingers = [None] * KEY_SIZE
        self.callbacks = {}
        self.storage = {}
        self.messages = {}

        self.join(dest_host)

    def __str__(self):
        return f"<{self.ip}:{self.port}>"

    def __repr__(self):
        return str(self)

    def join(self, dest_host):
        if dest_host:
            remote_node_reference = RemoteNodeReference(
                dest_host[0],
                dest_host[1]
            )
            succ = self.fingers[0] = remote_node_reference.find_succesor(
                self.id()
            )
            # Negotiate with succesor the keys
            # keys <= self.identifier
            key_val, key_msg = succ.get_keys(self.id())
            for k, v in key_val:
                self.simple_put(k, v)
            for k, msg in key_msg:
                self.simple_enqueue(k, msg)

        else:
            self.fingers[0] = self

    def get_keys(self, key):
        # Get our storaged keys that are now responsability of key
        key_val = []
        key_msg = []
        keys = []
        for storkey in self.storage.keys():
            if storkey <= key:
                key_val.append((storkey, self.storage[storkey]))
                keys.append(storkey)
        for storkey in self.messages.keys():
            if storkey <= key:
                key_msg.append((storkey, self.messages[storkey]))
                keys.append(storkey)
        node = self.succesors[-1:]
        if node and node[0].ping():
            for k in keys:
                node[0].remove_key(k)
        return key_val, key_msg

    def remove_key(self, key):
        self.storage.pop(key, None)
        self.messages.pop(key, None)
        return True

    def id(self, offset=0):
        digest = int(
            sha1(
                bytes("%s:%d" % (self.ip, self.port), 'ascii')
                ).hexdigest(),
            16)
        return (digest + offset) % 2**(KEY_SIZE)

    def ping(self):
        return True

    def succesor(self) -> RemoteNodeReference:
        for remote in [self.fingers[0]] + self.succesors:
            if remote.ping():
                self.fingers[0] = remote
                return remote
        logging.info("No Succesor Found, reseting to ourselves")
        self.fingers[0] = self
        return self

    @repeat_after_time(STABILIZE_INTERVAL)
    @repeat_when_socket_fail(MAX_TRIES)
    def stabilize(self):
        succesor = self.succesor()
        if succesor.id() != self.fingers[0].id():
            self.fingers[0] = succesor

        pred = succesor.predecessor()
        if pred is not None and between(pred.id(), self.id(1), succesor.id())\
                and self.id(1) != succesor.id() and pred.ping():
            self.fingers[0] = pred

        self.succesor().notify(self)
        return True

    def predecessor(self):
        return self._predecessor

    def notify(self, remote_node_reference):
        if self.predecessor() is None or not self.predecessor().ping()\
            or between(
            remote_node_reference.id(),
            self.predecessor().id(1),
            self.id()
        ):
            self._predecessor = remote_node_reference

    def find_successor(self, key):
        if self.predecessor() and between(
            key,
            self.predecessor().id(1),
            self.id(1)
        ):
            return self
        node = self.find_predecessor(key)
        return node.succesor()

    def find_predecessor(self, key):
        node = self

        if node.succesor().id() == node.id():
            return node
        while not between(key, node.id(1), node.succesor().id(1)):
            node = node.closest_preceding_node(key)
        return node

    def closest_preceding_node(self, key):
        for node in reversed(self.succesors + self.fingers):
            if node is not None and between(
                node.id(),
                self.id(1),
                key
            ) and node.ping():
                return node
        return self

    @repeat_after_time(FINGERS_INTERVAL)
    def fix_fingers(self):
        i = random.randrange(KEY_SIZE - 1) + 1
        self.fingers[i] = self.find_successor(self.id(2**i))
        return True

    @repeat_after_time(UPDATE_SUCCESORS_INTERVAL)
    def update_succesors(self):
        # Manage cases when we are not alone in the ring
        succ = self.succesor()
        if succ.id() != self.id():
            successors = [succ]
            succ_list = succ.get_succesors()
            if succ_list:
                successors += succ_list
            self.succesors = successors
        return True

    def get_succesors(self):
        if len(self.succesors) == MAX_SUCCESORS:
            return [
                (node.ip, node.port)
                for node in self.succesors[:MAX_SUCCESORS - 1]
            ]
        else:
            return[
                (node.ip, node.port) for node in self.succesors
            ]

    def recv(self, sock):
        data = b''
        while 1:
            rec = sock.recv(64)
            if rec[-2:] == b"!!":
                data += rec[:-2]
                break
            data += rec
        return data

    def __dispatch_rpc(self, action, *args):
        if hasattr(self, action):
            func = getattr(self, action)
        else:
            func = self.callbacks[action]
        return func(*args)

    def serve_rpc_requests(self):
        # Create the socket server
        server_sock = socket.socket()
        server_sock.bind((self.ip, self.port))
        server_sock.listen(100)  # TODO: Pherhaps use a threshold here??

        while True:
            try:
                client_sock, addr = server_sock.accept()
                threading.Thread(
                    target=self.handle_client,
                    args=(client_sock, addr)).start()
            except socket.error:
                logging.info("Error in the RPC server socket. Continue")
                continue

    def handle_client(self, client_sock, addr):
        request = self.recv(client_sock)
        # Ignore garbage
        if request != b"pinging":
            request = loads(request)
            command = request[0]
            request = request[1:]
            # Valid command
            if hasattr(self, command) or command in self.callbacks.keys():
                # This is needed because cloudpickle wont serialize lock\
                #  objects
                if command == "notify":
                    request[0].lock = threading.BoundedSemaphore()
                response = self.__dispatch_rpc(command, *request)

                if isinstance(response, (RemoteNodeReference, Node)):
                    response = (response.ip, response.port)

                client_sock.sendall(dumps(response) + b"!!")
        else:
            # logging.debug(f"Receive ping from {addr}")
            pass
        client_sock.close()

    def start_service(self):
        stabilize_daemon = threading.Thread(target=self.stabilize)
        fix_fingers_daemon = threading.Thread(target=self.fix_fingers)
        update_succesors_daemon = threading.Thread(
            target=self.update_succesors
        )
        rpc_daemon = threading.Thread(target=self.serve_rpc_requests)

        rpc_daemon.start()
        stabilize_daemon.start()
        fix_fingers_daemon.start()
        update_succesors_daemon.start()

        # while 1:
        #     logging.debug(" ** ******* NODE STATE *********\n" +
        #                   f"Succesors: {self.succesors}\n" +
        #                   f"Predecesor: {self.predecessor()}\n" +
        #                   f"Current Succesor: {self.succesor()}")
        #     sleep(5)

    def put(self, key, val):
        logging.debug(f"Putting {key} in <{self.ip}:{self.port}")
        if between(key, self.predecessor().id(1), self.id()):
            self.storage[key] = val
            # make that our succesors update the key
            for node in [self.fingers[0]] + self.succesors:
                if node.ping():
                    node.simple_put(key, val)
        else:
            logging.debug(f"{self} not responsible for key {key}")
            node = self.find_successor(key)
            node.put(key, val)
        return True

    def simple_put(self, key, val):
        self.storage[key] = val

    def get(self, key):
        # If we are responsible for key, return it
        if between(key, self.predecessor().id(1), self.id()):
            return self.storage.get(key, False)
        else:
            # Find the node responsible for that key
            node = self.find_successor(key)
            return node.get(key)

    def enqueue_message(self, key, msg):
        # If we are responsible for key, then enqueue msg
        if between(key, self.predecessor().id(1), self.id(1)):
            try:
                self.messages[key].append(msg)
            except KeyError:
                self.messages[key] = [msg]
            # Update queue of succesors
            for node in [self.fingers[0]] + self.succesors:
                if node.ping():
                    node.simple_enqueue(key, msg)
        else:
            # Search for responsible of key
            node = self.find_successor(key)
            node.enqueue_message(key, msg)
        return True

    def simple_enqueue(self, key, msg):
        try:
            self.messages[key].append(msg)
        except KeyError:
            self.messages[key] = []

    def dequeue_messages(self, key):
        # If We are responsible for key, dequeue it and return
        if between(key, self.predecessor().id(1), self.id(1)):
            msg_list = self.messages.get(key, [])
            self.messages[key] = []
            # Remove msgs entries in succesors
            for node in [self.fingers[0]] + self.succesors:
                if node.ping():
                    node.simple_dequeue(key)
            return msg_list
        else:
            # Find responsible for key
            node = self.find_successor(key)
            response = node.dequeue_messages(key)
            return response

    def simple_dequeue(self, key):
        self.messages[key] = []

    def register(self, callback_name, callback):
        self.callbacks[callback_name] = callback

    def unregister(self, callback_name):
        try:
            self.callbacks.pop(callback_name)
        except KeyError:
            pass
