import socket
from hashlib import sha1
from time import sleep
import threading
import logging
import random
from cloudpickle import dumps, loads

logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(message)s",
        level=logging.DEBUG
     )

KEY_SIZE = 160
MAX_TRIES = 5
DATA_RECV = 1024
STABILIZE_INTERVAL = 2
FINGERS_INTERVAL = 1
UPDATE_SUCCESORS_INTERVAL = 1
MAX_SUCCESORS = 7  # log2(160) ~ 7


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

    def id(self, offset=0):
        digest = int(sha1(bytes("%s:%d" % (self.ip, self.port))).hexdigest(), 16)
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
            sock.sendall(b"!!")
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
        self.send(["get_succesor"])
        response = self.recv()
        response = loads(response)
        return RemoteNodeReference(response[0], response[1])

    @safe_connection_required
    def predecessor(self):
        self.send(["get_predecessor"])
        response = loads(self.recv())
        return RemoteNodeReference(response[0], response[1])

    @safe_connection_required
    def get_closest_preceding_node(self, key):
        self.send(["get_closest_preceding_node", key])
        response = loads(self.recv())
        return RemoteNodeReference(response[0], response[1])

    @safe_connection_required
    def find_succesor(self, key):
        self.send(["find_succesor", key])
        response = loads(self.recv)
        return RemoteNodeReference(response[0], response[1])

    @safe_connection_required
    def notify(self, node):
        self.send(['notify', RemoteNodeReference(node.ip, node.port)])


# TODO: Agregar logica para almacenar las llaves, y negociarlas con la entrada\
    #  de nuevos nodos
# TODO: Agregar logica para registrar callbacks
class Node:
    def __init__(self, node_ip, node_port, dest_host=None):
        self.ip = node_ip
        self.port = node_port
        self.succesors = []
        self._predecessor = None
        self.fingers = [None] * KEY_SIZE
        self.callbacks = {}
        self.storage = {}

        self.join(dest_host)

    def join(self, dest_host):
        if dest_host:
            remote_node_reference = RemoteNodeReference(dest_host[0], dest_host[1])
            self.fingers[0] = remote_node_reference.find_succesor(self.id())
        else:
            self.fingers[0] = self

    def id(self, offset=0):
        digest = int(sha1(bytes("%s:%d" % (self.ip, self.port))).hexdigest(), 16)
        return (digest + offset) % 2**(KEY_SIZE)

    def ping(self):
        return True

    def succesor(self) -> RemoteNodeReference:
        for remote in [self.fingers[0]] + self.succesors:
            if remote.ping():
                self.fingers[0] = remote
                return remote
        logging.info("No Succesor Found, Exiting")
        exit(-1)

    @repeat_after_time(STABILIZE_INTERVAL)
    @repeat_when_socket_fail(MAX_TRIES)
    def stabilize(self):
        succesor = self.succesor()
        if succesor.id() != self.fingers[0].id(1):
            self.fingers[0] = succesor

        pred = succesor.predecessor()
        if pred is not None and between(pred.id(), self.id(1), succesor.id(1)) and self.id(1) != succesor.id() and pred.ping():
            self.fingers[0] = pred

        self.succesor().notify(self)
        return True

    def predecessor(self):
        return self._predecessor

    def notify(self, remote_node_reference):
        if self.predecessor() is None or between(remote_node_reference.id(), self.predecessor().id(1), self.id(1)) or not self.predecessor().ping():
            self._predecessor = remote_node_reference

    def find_successor(self, key):
        if self.predecessor() and between(key, self.predecessor().id(1), self.id(1)):
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
            if node is not None and between(node.id(), self.id(1), key) and node.ping():
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

    def get_succesors(self):
        return [(node.ip, node.port) for node in self.succesors[:MAX_SUCCESORS - 1]]

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
        server_sock.listen()  # TODO: Pherhaps use a threshold here??

        while True:
            try:
                client_sock, _ = server_sock.accept()
            except socket.error:
                logging.info("Error in the RPC server socket. Exiting")
                exit(-1)
            request = self.recv(client_sock)
            # Ignore garbage
            if request:
                request = loads(request)
                command = request[0]
                request = request[1:]
                # Valid command
                if hasattr(self, command) or command in self.callbacks.keys():
                    response = self.__dispatch_rpc(command, *request)

                    if isinstance(response, (RemoteNodeReference, Node)):
                        response = (response.ip, response.port)

                    client_sock.sendall(dumps(response) + b"!!")
            client_sock.close()

    def start_service(self):
        stabilize_daemon = threading.Thread(target=self.stabilize)
        fix_fingers_daemon = threading.Thread(target=self.fix_fingers)
        update_succesors_daemon = threading.Thread(target=self.update_succesors)
        rpc_daemon = threading.Thread(target=self.__dispatch_rpc)

        rpc_daemon.start()
        stabilize_daemon.start()
        fix_fingers_daemon.start()
        update_succesors_daemon.start()
