'''
Este modulo provee los peers de una DHT implementada con Chord.
'''

import random
import time
import logging
import socket
import socketserver
import re

URL_REGEX = re.compile(r'(?P<host>[A-Za-z]+):(?P<port>[1-9][0-9]{3,4})/(?P<nodeid>\w+)')
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.DEBUG)

CHAIN = 3
CHORDS = 30
MAX_KEY = 2**30
CHORD_UPDATE_INTERVAL = 5

# Estas clases son solamente para diferenciar una tupla de una direccion
# y cada direccion, de la direccion propia del nodo al que se le hace
# el request.
class Address(tuple):
    '''
    Representa una direccion diferente a la del nodo que se solicita.
    '''
    pass


class Me(Address):
    '''
    Representa la misma direccion del nodo al que se le pregunta.
    '''
    pass


def __connect(url):
    '''
    Funcion basica para establecer una conexion.
    '''
    sock = socket.socket()
    if isinstance(url, bytes):
        url = str(url, 'asccii')
    url_parse = URL_REGEX.match(url).groupdict()
    host, port, nodeid = url_parse['host'], int(url_parse['port']), url_parse['nodeid']
    logging.debug('Connecting to %s:%d' % (host, port))
    sock.connect((host, port))
    return sock

def __parse_node(data):
    '''
    Parsea datos referentes a un nodo del DHT. La cadena se espera que tenga la
    forma "peer {key} {url}"
    '''
    if data.startswith(b'peer'):
        key, url = data.split()[1:]
        return Address([int(key, base=16), url])

    if data.startswith(b'none'):
        return None

    raise ValueError('Wrong response from peer %s' % data)

def requests(url, operation, key, value=None):
    '''
    Metodo para despachar operaciones en cada nodo.
    RPC.
    El valor devuelto es la respuesta procesada, o raw si no coincide
    con ninguno de los formatos especificados.
    '''
    logging.debug('Requesting from %s operation %s key %x value %s' %
                  (url, operation, key, value))
    sock = __connect(url)
    body = bytes("%s %x\n" % (operation, key), 'asccii')
    if value:
        body += bytes("%d\n" % len(value), 'asccii')
        body += value

    try:
        sock.sendall(body)
        sock_file = sock.makefile('rb')
        response = sock_file.readline()

        # Parsear la respuesta en dependencia de la operacion que se haya requerido
        if response.startswith(b'value'):
            logging.debug(response)
            length = int(response.split()[1])
            return sock_file.read(length)

        if response.startswith(b'none'):
            raise KeyError("Key %x not in DHT" % key)

        if response.startswith(b'peer'):
            logging.debug('RAW response %s' % response)
            return __parse_node(response)

        if response.startswith(b'me'):
            key = int(response.split()[1], base=16)
            return Me([key, url])

        if response.startswith(b'chain'):
            chain = []
            for line in sock_file:
                chain.append(__parse_node(line))
            return chain

    finally:
        sock.close()

    return response

def belongs_to_interval(key, left, right):
    '''
    Determina si key pertenece al intervalo [left, right].
    Aqui hay que tener presente que la estructura es un anillo,
    luego es posible que right < left.
    '''
    # Si el intervalo solo contiene un punto entonces key no pertenece
    if left == right:
        return False

    # Primer caso, left < right
    if left < right:
        return left <= key < right

    # Segundo caso right < left
    if right < left:
        return right > key or left <= key

class Peer:
    '''
    Esta clase representa cada nodo en el anillo de CHORD. Cada nodo esta identificado
    por su URL, donde la URL se compone por el host y puerto que almacena el proceso
    correspondiente al nodo, y el nombre del nodo, por ejemplo:
    10.10.10.95:5432/node2
    '''
    def __init__(self, port=5432, key=None):
        if key is None:
            self.key = random.randint(0, MAX_KEY)
        else:
            self.key = key

        logging.info('Peer key: %x' % self.key)
        self.chords = [None] * CHORDS
        self.chain = [None]
        self.storage = {}
        self.port = port

    def connect(self, url):
        '''
        Connectarse a la dht utilizando la url de cualquier nodo conectado. Esto implica
        que debe existir un nodo inicial al cual se le puede aplicar el protocolo 'Join'
        de CHORD.
        '''
        logging.info('Connecting to : %s' % url)
        old = self.find_re(self.key, connecting=url)
        logging.debug(old)
        self.chain = [old] + requests(url, 'accept', self.key, bytes(str(self.port), 'ascii'))

        for i in range(CHORDS):
            key = (self.key + 2**i) % MAX_KEY
            if not belongs_to_interval(key, self.key, old[0]):
                self.chords[i] = self.find_re(key, connecting=url)


    def accept(self, key, url):
        '''
        Acepta un nuevo nodo en la DHT. Las siguientes acciones son realizadas:
        - colocar el nuevo nodo despues de uno mismo (conventirlo en nuestro sucesor)
        - reasignar una parte de nuestro espacio de llaves al nuevo nodo.
        '''
        self.chain = [(key, url)] + self.chain

        for i in range(CHORDS):
            key = (self.key + 2**i) % MAX_KEY
            if self.chords[i] is None and not belongs_to_interval(key, self.key, self.chain[0][0]):
                self.chords[i] = self.chain[0]

    def start(self):
        '''
        Empieza el funcionamiento del peer.
        '''
        pass

    def find(self, key):
        '''
        Encuentra un peer que esta mas cerca del responsable de la llave 'key'.
        De ser el actual el responsable, devuelve None, de lo contrario se devuelve una\
        tupla (url, key).
        '''
        pass

    def find_re(self, key, connecting=None):
        '''
        Encuentra el nodo que es responsable de la llave 'key'.
        Devuelve None en case de ser uno mismo el responsable, sino se retorna una tupla (url, key).
        '''
        return ("",None)

    def get(self, key):
        '''
        Devuelve el valor de la llave 'key', donde quiera que este guardada.
        '''
        pass

    def put(self, key, value):
        '''
        Guarda el valor '(key, value)' en la DHT.
        '''
        pass

    def _updaste_chords(self):
        pass
