'''
Este modulo provee de una implementacion para los peers de una DHT implementada con Chord.
'''

import random
import time
import logging

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.DEBUG)

CHAIN = 3
CHORDS = 30
MAX_KEY = 2**30
CHORD_UPDATE_INTERVAL = 5

class Peer:

    def __init__(self, port=5432, key=None):
        if key is None:
            self.key = random.randint(0,MAX_KEY)
        else:
            self.key = key

        logging.info('Peer key: %x' % self.key)
        self.chords = [None] * CHORDS
        self.chain = [None]
        self.storage = {}
        self.port = port

    def connect(self, url):
        '''
        Connectarse a la dht utilizando la url de cualquier nodo conectado
        '''
        logging.info('Connecting to : %s' % url)
        old = self.find_re(self.key, connecting=url)

    def accept(self, key, url):
        '''
        Acepta un peer a a la DHT. Las siguientes acciones son realizadas:
        - colocar el nuevo peer despues de uno mismo (conventirlo en nuestro sucesor)
        - reasignar una parte de nuestro espacio de llaves al nuevo peer.
        '''
        pass

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
        Encuentra el peer que es responsable de la llave 'key'.
        Devuelve None en case de ser uno mismo el responsable, sino se retorna una tupla (url, key).
        '''
        pass

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
