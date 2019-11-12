'''
Implementacion fundamental del tracker. Este tracker utilizara el modelo REQUEST\
- REPLY para comunicarse con los clientes.
'''
import zmq
from setupdb import setup_client
from time import sleep


class ClientInformationTracker:
    '''
    Tracker minimo para servir informacion de los clientes, asi como para fungir como\
    servidor de registro.
    '''

    def __init__(self, database_name, address, port):

        # Inicializar la base de datos o cargarla en caso de que ya exista
        self.client_database = setup_client(database_name)
        if self.client_database is None:
            print("Error while setting up the database")
            exit(0)

        # Llevar un record de los clientes que ya se han autentificado.
        # Si un cliente no pertenece a este conjunto, entonces un request
        # de informacion le sera negado
        self.registered_clients = set()

        context = zmq.Context()
        # Crea un servidor para Replicar a cada peticion de los clientes.
        self.sock = context.socket(zmq.REP)
        self.sock.bind(f'tcp://{address}:{port}')

    def __register_client(self, *client):
        raise NotImplementedError()

    def __serve_client_info(self, client_id):
        raise NotImplementedError()

    async def say_alive(self):
        pass

    def serve_requests(self):
        '''
        Atiende los pedidos de los clientes y responde con el servicio correspondiente.
        '''
        # Dummy code just to test for now
        while 1:
            print("waiting for received data")
            data = self.sock.recv_string()
            print(f'received data = {data}')
            self.sock.send_string("Hi from server")
