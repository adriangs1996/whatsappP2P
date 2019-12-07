import zmq
import cloudpickle
import json
import socket
from myqueue import Queue as myQueue
from threading import Thread
from random import randint
loads = cloudpickle.loads

class Client:  

    def __init__(self, *args, **kwargs):   #todo implement get server addrs from config file #// done
        # .*args = server_addr, self.ip, self.port si se esta instanciando un cliente nuevo, 
        # #si no *args = client_identifier para restaurarlo'
        # if len(args) <= 2:
        #     Client.restore_client(args[1])
        # else:
        try:
            Client.restore_client(self)
        except:
            self.ip = socket.gethostbyname(socket.gethostname())           
            self.port = randint(3001, 9000)
            self.contacts_info = {}          # {contact_name : {'addr': address (*tuple ip,port*), 'online' : bool,  'conversation': myQueue(messages)}}
            #self.chats = {}             # {contact_name : list with the messages in the conversation}
            self.outgoing_queue = {}    # {contact_name : queue with pending messages}
            #self.online_contacts = {}
            self.registered = False

        #self.server_sock = self.__open_socket__()
        #self.is_sock_open = True
        #self.server_sock.connect(f'http://{self.server_addr}')
        self.servers = self.__read_config__()   # trackers addresses, list of tuple (ip,port) (str,int)  
    
        if self.registered:
            self.loggin()

        self.context = zmq.Context()

        self.__start_client__()

        self.process_pending_messages()
        
        self.incomming_thread = Thread(target= self.__handle_incomming__)
        self.incomming_thread.setDaemon(True)
        self.incomming_thread.start()

    def __start_client__(self):
        #this is to start the client sockets
        #context = zmq.Context()             #! this might be troublesome, in case of error check if this is the cause, try to solve it with global context
        self.incoming_sock = self.context.socket(zmq.REP)
        self.incoming_sock.bind(f'tcp://{self.ip}:{self.port}') 
        self.outgoing_sock = self.context.socket(zmq.REQ)
        

    def __handle_incomming__(self):
        while True:
            messg = self.incoming_sock.recv_pyobj()
            if not isinstance(messg, Message):
                if messg == 'ping':
                    self.incoming_sock.send_string('online')
            else:
                self.__log_message__(messg.sender, messg)
                # try:
                #     self.contacts_info[messg.sender]['conversation'].smart_enqueue(messg)
                # except KeyError:
                #     self.contacts_info[messg.sender]['conversation'] = myQueue([messg])
                self.incoming_sock.send_json({'response': True})


    def register(self, username) -> bool:          #// done
    
        '''
        Este procedimiento se reserva para la creacion de clientes que no esten presentes en el\
        servidor. Permite interactuar con el servidor para registrar un nuevo cliente \
        proporcionando un nombre de usuario, contrasena, y un telefono, y el servidor devolvera\
        un id con el que se identifica ese cliente a partir de ese momento.
        '''        
                
        for t_ip,t_port in self.servers:
            try:
                reply = request_tracker_action(t_ip, t_port, 'register_client', user= username, ip= self.ip, port= self.port)
                print('sent request from client')
                if reply:
                    self.registered = True
                    self.username = username
                    break            
            except:
                continue

            if not reply:
                print('not reply')
                raise Exception

        return self.registered


    def loggin(self):                                   #// done
        '''
        Una vez registrado el cliente, con cada inicio de sesion habra que logearse en el sistema.\
        Para ello se utiliza el identificador del cliente y se le proporciona el usuario y el passw\
        al servidor. El cliente debe guardar un hash de este passsword en algun estado, para poder\
        enviarlo al servidor con cada inicio de sesion.
        '''
        for t_ip,t_port in self.servers:
            try:
                reply = request_tracker_action(t_ip, t_port, 'check_client', user= self.username, ip= self.ip, port= self.port)
                for item in reply:
                    self.__log_message__(item.sender, item)
            except:
                continue
            


    def send_message_client(self, target_client, message) -> bool:          #//done, i think
        '''
        Envia un mensaje al cliente destino.
        '''
        if target_client not in self.contacts_info.keys():
            self.add_contact(target_client)
        address = self.get_peer_address(target_client)
        if not self.__send_message__(address, message):
            self.enqueue_message(target_client, message)
        else:
            self.__log_message__(target_client, message)

    def __send_message__(self, target_address, message) -> bool:   # target_address is a string of the form 'ip_address:port' #//done
        reply = None
        if self.outgoing_sock.closed:
            self.outgoing_sock = self.context.socket(zmq.REQ)
        try:
            self.outgoing_sock.connect(f'tcp://{target_address}')
        except:
            return False
        
        self.outgoing_sock.send_pyobj(message)

        tries = 10
        tout = 1000
        while tries:
            if self.outgoing_sock.poll(timeout= tout):
                break
            tries -= 1
            tout *= 2
        if not tries:
            self.outgoing_sock.close()
            return False

        reply = self.outgoing_sock.recv_json()
        return reply['response']
       
    def process_pending_messages(self):                     #// done
        for contact in self.outgoing_queue.keys():
            address = self.get_peer_address(contact)
            queue = self.outgoing_queue[contact]
            while not queue.isEmpty():
                message = queue.peek()
                if not self.__send_message__(address, message):
                    if self.send_message_to_server_queue(message):
                        self.__log_message__(contact, message)
                        self.outgoing_queue[contact].pop()                
                else:
                    self.__log_message__(contact, message)
                    self.outgoing_queue[contact].pop()

    
    def send_message_to_server_queue(self, message):        #// done
        for t_ip,t_port in self.servers:
            try:
                reply = request_tracker_action(t_ip, t_port, 'enqueue_message', message)
                if reply:
                    return True
            except:
                return False
                    

    def __open_socket__(self, sock_type= zmq.REQ):          #// done
        '''
        creates a zmq socket for you

        >- sock_type: one of the zmq types of socket. e.g: zmq.REQ
        '''
        context = zmq.Context()
        socket = context.socket(sock_type) 
        return socket

    def send_message_group(self, target_group, message):    #todo
        '''
        Envia un mensaje al grupo destino.
        '''
        raise NotImplementedError()

    def add_contact(self, contact_name):                    #// done
        self.contacts_info[contact_name] = {}
        self.update_contact_info(contact_name)
        return self.contacts_info[contact_name]

    def update_contact_info(self, contact_name):            #// done
        for (t_ip, t_port) in self.servers:
            try:
                response = request_tracker_action(t_ip, t_port, 'locate', user= contact_name)
                if response:
                    self.contacts_info[contact_name]['addr'] = response
                    return
            except:
                continue   
       
    
    def get_peer_address(self, peer_name):                  #//auxiliary method, done
        r_ip,r_port = self.contacts_info[peer_name]['addr']
        return f'{r_ip}:{r_port}'

    def enqueue_message(self, target_client, message):      #//auxiliary method, done
        if target_client not in self.outgoing_queue.keys():
            self.outgoing_queue[target_client] = myQueue(auto_growth= True)
        self.outgoing_queue[target_client].enqueue(message)        


    def __log_message__(self, target_client, message):      #//auxiliary method, done
        if self.contacts_info[target_client]['conversation'] == None:
            self.contacts_info[target_client]['conversation'] = myQueue()
        self.contacts_info[target_client]['conversation'].smart_enqueue(message)

    def send_adj_client(self, target_client, target_file):  #todo
        '''
        Envia un envie archivo al cliente destino.
        '''
        raise NotImplementedError()

    def post_adj(self, target_group, target_file):          #todo
        '''
        Envia un archivo a todos los miembros del grupo destino.
        '''
        raise NotImplementedError()

    def __encrypt(self, message, key):                      #todo
        '''
        Mecanismo para cifrar un mensaje
        '''
        raise NotImplementedError()

    def __decrypt(self, message, key):                      #todo
        '''
        Mecanismo para descifrar un mensaje
        '''
        raise NotImplementedError()

    def __generate_client_key(self):                        #todo
        '''
        Genera una llave privada y otra global para este cliente. \
        Este procedimiento debe ser invocado como parte del mecanismo de registro.
        '''
        raise NotImplementedError()

    def save_client_state(self):                            #// done, i think
        '''
        Salva el estado del cliente, de modo que cada inicio de la app no requiera de todo un nuevo\
        proceso de registro y nueva generacion de las llaves, asi como de las sesiones.
        '''

        d={}
        for key in self.__dict__.keys():
            t = type(self.__dict__[key])
            if t is not zmq.Socket and t is not zmq.Context:                
                d[key] = self.__dict__[key]
        filestream = open(f'wsp_client.bin', mode ='wb')
        cloudpickle.dump(d, filestream)
        filestream.close()

    @staticmethod
    def restore_client(self):                         #// done, i think
        '''
        Carga el estado de un cliente guardado y devuelve una instancia de Client listo para usar.
        '''
        filestream = open(f'wsp_client.bin', mode= 'rb')
        client_info = cloudpickle.load(filestream)
        filestream.close()

        for key in client_info:
            self.__dict__[key] = client_info[key]

    async def discover_online_contacts(self):               #// done, not conviced
        '''
        Realiza un algoritmo de autodescubrimiento, basado en la informacion guardada sobre\
        los contactos de este cliente, de modo que se puede saber quienes estan online. \
        Este metodo debe ser asincrono para poder ejecutarse cada cierto periodo de tiempo\
        y que actualice los datos del cliente cada vez que obtenga resultados.
        '''        
        for contact in self.contacts_info.keys():
            self.contacts_info[contact]['online'] = self.check_online(contact)

    def check_online(self, contact_name):                   #// auxiliary method, done
        c_ip,c_port = self.contacts_info[contact_name]['addr']
        #contx = zmq.Context()
        socket = self.context.socket(zmq.REQ)
        socket.connect(f'tcp://{c_ip}:{c_port}')
        socket.send_pyobj('ping')
        tries = 10
        time = 1000
        while tries:
            if socket.poll(timeout= time, flags= zmq.POLLIN):
                break
            tries -= 1
            time *= 2
        if not tries:
            socket.close()
            return False
        reply = socket.recv_string()
        socket.close()
        if reply:
            return True

    def __hash__(self):
        # Permitir que los clientes sean usados como llaves de diccionarios.
        # Un posible valor de hash para los clientes puede ser una combinacion de
        # su llave publica con su llave privada
        raise NotImplementedError()

    def search_contact(self, contact_name):
        for t_ip,t_port in self.servers:
            try:
                reply = request_tracker_action(t_ip, t_port, 'locate', user= contact_name)
                print(reply)
            except NoResponseException:
                print('entered exception')
                return None
            
            return reply

    def delete_contact(self, contact_name):
        self.contacts_info.pop(contact_name)

    def exit(self):
        self.save_client_state()
        self.incomming_thread.join()
        self.incoming_sock.close()
        self.outgoing_sock.close()        

    def __read_config__(self):
        fd = open('config-file.conf', 'r')
        config = json.load(fd)
        servers = [(x,y) for x,y in config]
        return servers


        
        
        


class Group:
    '''
    Provee una abstraccion de grupo, todos los clientes que pertenezcan al grupo deben poder \
    recibir los mensajes que sean enviados a este, asi como ver quienes de los miembros del \
    grupo estan actualmente en linea.
    '''
    def __init__(self, name, group_id, *args):
        '''
        @name : Nombre con el cual se debe identificar al grupo
        @group_id: Un entero que representa unicamente a un grupo
        @args: clientes que pertenecen al grupo
        '''
        self.clients = list(args)
        self.group_name = name
        self.group_id = group_id

    async def discover_online_members(self):
        """
        Algoritmo que permite a un grupo seguir la pista de los usuarios conectados. Es asincrono.
        """
        raise NotImplementedError()

    def post_message(self, sender_client: Client, message):
        '''
        Envia el mensaje a todos los miembros del grupo
        '''
        for client in self.clients:
            sender_client.send_message_client(client, message)

    # Permitir que la clase grupo sea hashable y por tanto pueda formar parte de llaves en
    # un diccionario
    def __hash__(self):
        return hash(self.group_id)

    @property
    def group_id(self):
        '''
        Devuelve el id del grupo
        '''
        return self.group_id

    @property
    def group_name(self):
        '''
        Devuelve el nombre
        '''
        return self.group_name

class Message:
    '''
    Esta clase representa un mensaje a ser enviado por un cliente.\
    Oferece informacion sobre el cliente que lo envia, su contenido y\
    una marca temporal que indica el momento en que se envio.
    '''
    def __init__(self, text, sender, timestamp):
        self.sender = sender    # username : string
        self.receiver           # reciever username : string
        self.text = text
        self.timestamp = timestamp

    # Permitir que los mensajes puedan ser almacenados como llaves de diccionarios
    def __hash__(self):
        return hash(self.text, self.sender)

    @property
    def text(self):
        '''
        Devuelve el texto del mensaje
        '''
        return self.text

    @property
    def sender(self):
        '''
        Devuelve el cliente que creo el mensaje
        '''
        return self.sender

    @property
    def receiver(self):
        '''
        Devuelve el cliente al que se le envió el mensaje
        '''
        return self.receiver

    @property
    def timestamp(self):
        '''
        Devuelve la fecha en la que se envio el mensaje
        '''
        return self.timestamp
        


# def request_tracker_action(tracker_ip, tracker_port, action, **kwargs):
#     '''
#     Tracker request can only contain 3 actions: check_client, register_client\
#             or locate.
#     @param tracker_ip   : Know tracker ip to ask to
#     @param tracker_port : The tracker port where service is active
#     @param action       : Desire action to execute on the tracker
#     @kwargs             : Keyword args with the following keys:
#     user := username to trackto (either to check or register or locate)
#     ip   := ip of the sender (only needed for check or register)
#     port := port of the sender service (only needed for check or register)
#     '''
#     # Create the client socket
#     client_context = zmq.Context()
#     client_sock = client_context.socket(zmq.REQ)
#     assert(action in ('locate', 'check_client', 'register_client'))
#     client_sock.connect("tcp://%s:%d" % (tracker_ip, tracker_port))
#     if action in ('check_client', 'register_client'):
#         client_sock.send_json(
#             {'action': action,
#                 'id': kwargs['user'],
#                 'ip': kwargs['ip'],
#                 'port': kwargs['port']
#                 }
#             )
#     # The other posibility is only 'locate'
#     else:
#         client_sock.send_json(
#             {
#                 'action': action,
#                 'id': kwargs['user']
#             }
#             )
#     # Check if server is responding
#     # clients should test for a server response to know whether
#     # it's active, or is down.
#     tries = 8
#     timeout = 1000
#     while tries:
#         if client_sock.poll(timeout=timeout, flags=zmq.POLLIN):
#             break
#         tries -= 1
#         timeout *= 2
#     # No server response
#     if not tries:
#         client_sock.close()
#         raise NoResponseException

#     response = client_sock.recv_json()['response']
#     client_sock.close()
#     return response

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
    assert(action in ('locate', 'check_client', 'register_client', 'enqueue_message'))
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
    timeout = 10
    while tries:
        if client_sock.poll(timeout=timeout, flags=zmq.POLLIN):
            break
        tries -= 1
        timeout *= 2
    # No server response
    if not tries:
        client_sock.close()
        raise NoResponseException()

    response = client_sock.recv_pyobj()
    if isinstance(response, list):
        rep = []
        for message in response:
            rep.append(loads(message))
        response = rep

    client_sock.close()
    return response


class NoResponseException(Exception):
    pass