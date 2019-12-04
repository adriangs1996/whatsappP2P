'''
Main implementation of an address tracker for clients to known who they want
to talk to.
'''
from hashlib import sha1
import logging
from threading import Thread
from cloudpickle import dumps, loads
import zmq
from dht.chord import request, NoResponseException, RemoteNodeReference


logging.basicConfig(
    format="%(asctime)s %(levelname)s: %(message)s",
    level=logging.DEBUG
    )


def request_tracker_action(tracker_ip, tracker_port, action, **kwargs):
    '''
    Tracker request can only contain 3 actions: check_client, register_client\
         or locate.
    @param tracker_ip   : Known tracker ip to ask to
    @param tracker_port : The tracker port where service is active
    @param action       : Desire action to execute on the tracker
    @kwargs             : Keyword args with the following keys:
    user := username to trackto (either to check or register or locate)
    ip   := ip of the sender (only needed for check or register)
    port := port of the sender service (only needed for check or register)
    message:= Message (object)
    '''
    # Create the client socket
    client_context = zmq.Context()
    client_sock = client_context.socket(zmq.REQ)
    assert(action in (
        'locate',
        'check_client',
        'register_client',
        'enqueue_message'
    ))
    client_sock.connect("tcp://%s:%d" % (tracker_ip, tracker_port))
    if action in ('check_client', 'register_client'):
        client_sock.send_json(
            {'action': action,
             'id': kwargs['user'],
             'ip': kwargs['ip'],
             'port': kwargs['port']
             }
            )
    elif action == "enqueue_message":
        message = kwargs['message']
        marshalled_message = dumps(message)
        client_sock.send_json(
            {
                'action': action,
                'marshalled': marshalled_message,
                'ip': kwargs['ip'],
                'port': kwargs['port'],
                'id': message.receiver
            }
        )
    # The other posibility is only 'locate'
    else:
        client_sock.send_json(
            {
                'action': action,
                'id': kwargs['user']
            }
            )
    # Check if server is responding
    # clients should test for a server response to know whether
    # it's active, or is down.
    tries = 8
    timeout = 1000
    while tries:
        if client_sock.poll(timeout=timeout, flags=zmq.POLLIN):
            break
        tries -= 1
        timeout *= 2
    # No server response
    if not tries:
        client_sock.close()
        raise NoResponseException

    response = client_sock.recv_json()['response']
    if isinstance(response, list):
        rep = []
        for message in response:
            rep.append(loads(message))
        response = rep

    client_sock.close()
    return response


class ClientInformationTracker:
    '''
    Tracker minimo para servir informacion de los clientes, asi como para\
         fungir como servidor de registro.
    '''

    def __init__(self, address, port, bootstrap_chord_peers):
        '''
        @param address: String with the dot notation of the ip for this\
             tracker.
        @param port   : Int with the port to bind this tracker.
        @bootstrap_chord_peers: List of ip-port tuples of well known chord\
             nodes.
        '''
        self.chord_peers = [
            RemoteNodeReference(
                node[0],
                node[1]
            ) for node in bootstrap_chord_peers
        ]   # [(ip, port)]
        self.ip_address = address
        self.port = port

        server_thread = Thread(target=self.serve_requests)
        server_thread.setDaemon(True)
        server_thread.start()

        server_thread.join()

    def check_client(self, client_id, client_ip, client_port):
        if not isinstance(client_id, str) or not len(client_id) <= 20:
            return False

        client_key = sha1(bytes("%s" % client_id, 'ascii'))

        logging.info("verifiying if client %s is registered", client_id)

        chord_peer = self.__find_alive_chord()

        response = request("chord://%s:%d" % chord_peer, 'get', client_key)

        # If client is correctly checked, then update its address
        if response:
            response = request(
                'chord://%s:%d' % chord_peer,
                'put',
                client_key,
                (client_ip, client_port,)
            )

        # Return, if any, all queued messages for this client
            response = request(
                "chord://%s:%d" % chord_peer,
                "dequeue_messages",
                client_key
            )
            return response

    def register_client(self, client_id, client_ip, client_port):
        # This check is needed to ensure 160 bits key for sha1
        if not isinstance(client_id, str) or not len(client_id) <= 20:
            return False

        client_key = sha1(bytes("%s" % client_id, 'ascii'))

        logging.debug(
            "Verifiying if client %s is already register in DHT",
            client_id)

        chord_peer = self.__find_alive_chord()

        response = request("chord://%s:%d" % chord_peer, 'get', client_key)

        if not response:

            logging.debug(
                'Client %s is not registered, registering',
                client_id)

            response = request(
                'chord://%s:%d' % chord_peer,
                'put',
                client_key,
                (client_ip, client_port)
            )

            logging.debug('Succesfully added %x to db', client_key)

            return True

        logging.debug("Client already registered")
        return False

    def __serve_client_info(self, client_id):
        client_key = sha1(bytes("%s" % client_id, 'ascii'))

        logging.debug("Requesting client %x address", client_key)

        chord_peer = self.__find_alive_chord()

        response = request("chord://%s:%d" % chord_peer, 'get', client_key)

        logging.debug("Received {} for client".format(response))

        return response

    def __find_alive_chord(self):
        for node in self.chord_peers:
            if node.ping():
                return (node.ip, node.port)
        logging.info("[-] No known chord peer alive. Exiting")
        exit(-1)

        raise Exception("No chord alive")

    def ping(self):
        '''
        Respond to ping requests
        '''
        return True

    def enqueue_message(self, client_id, client_ip, client_port, message):
        user_id = sha1(bytes("%s" % client_id, 'ascii'))

        chord_peer = self.__find_alive_chord()
        request(
            "chord://%s:%d" % chord_peer,
            'enqueue_message',
            user_id,
            message
        )
        return True

    def __dispatch_object_method(self, method, *args):
        assert hasattr(self, method)
        func = getattr(self, method)
        result = func(*args)
        return result

    def serve_requests(self):
        '''
        RPC to serve clients requests.
        '''
        context = zmq.Context()
        server_sock = context.socket(zmq.REP)
        server_sock.bind("tcp://%s:%d" % (self.ip_address, self.port))

        while True:
            req = server_sock.recv_json()

            action = req['action']
            client_id = req['id']
            args = [client_id]

            if action in (
                'check_client',
                'register_client',
                'enqueue_message'
            ):
                cliend_address = req['ip']
                client_port = req['port']
                args += [cliend_address, client_port]

            elif action == 'locate':
                action = '__serve_client_info'

            if action == "enqueue_message":
                args += [req['marshalled']]

            result = self.__dispatch_object_method(action, *args)
            server_sock.send_json({'response': result})
