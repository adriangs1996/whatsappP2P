'''
Implementacion fundamental del tracker. Este tracker utilizara el modelo REQUEST\
- REPLY para comunicarse con los clientes.
'''
import zmq
from setupdb import setup_client

REP = 4

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

        alive = self.say_alive()

        # Llevar un record de los clientes que ya se han autentificado.
        # Si un cliente no pertenece a este conjunto, entonces un request
        # de informacion le sera negado
        self.registered_clients = set()

        with zmq.Context() as context:
            # Crea un servidor para Replicar a cada peticion de los clientes.
            self.sock = context.socket(REP)
            self.sock.bind(f'http://{address}:{port}')
            # Atender a peticiones mientras el tracker este activo
            self.serve_requests()

    def __register_client(self, *client):
        raise NotImplementedError()

    def __serve_client_info(self, client_id):
        raise NotImplementedError()

    async def say_alive(self):
        raise NotImplementedError()

    def serve_requests(self):
        pass
