'''
Main implementation of an address tracker for clients to known who they want
to talk to.
'''
from hashlib import sha1
import logging
from threading import Thread
import zmq
from dht.chord import request, NoResponseException


logging.basicConfig(
    format="%(asctime)s %(levelname)s: %(message)s",
    level=logging.DEBUG
    )


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
        self.chord_peers = bootstrap_chord_peers   # [(ip, port)]
        self.ip_address = address
        self.port = port

        server_thread = Thread(target=self.serve_requests)
        server_thread.setDaemon(True)
        server_thread.start()

        server_thread.join()

    def check_client(self, client_id):
        if not isinstance(client_id, str) or not len(client_id) <= 20:
            return False

        client_key = sha1(bytes("%s" % client_id, 'ascii'))

        logging.info("verifiying if client %s is registered", client_id)

        chord_peer = self.__find_alive_chord()

        response = request("chord://%s:%d" % chord_peer, 'get', client_key)

        return response is not None

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

        if response is None:

            logging.debug(
                'Client %s is not registered, registering',
                client_id)

            response = request(
                'chord://%s:d' % chord_peer,
                'put',
                (client_ip, client_port, client_key)
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
        for peer in self.chord_peers:
            try:
                request(
                    "chord://%s:%d" % peer,
                    'ping',
                    0
                )
                return peer
            except NoResponseException:
                continue

        raise Exception("No chord alive")

    def ping(self):
        '''
        Respond to ping requests
        '''
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

            if action == 'register':
                cliend_address = req['ip']
                client_port = req['port']
                args += [cliend_address, client_port]

            elif action == 'locate':
                action = '__serve_client_info'

            result = self.__dispatch_object_method(action, *args)
            server_sock.send_json({'response': result})
