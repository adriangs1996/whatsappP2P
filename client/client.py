

class Client:
    '''
    Clase base para implementar los clientes de ScrollMAT.\
    Cada cliente tiene acceso a un conjunto de funciones\
    elementales como son, entrar a un servidor de ScrollMAT,\
    registrarse en ScrollMAT, enviar un mensaje a otro cliente, \
    enviar un mensaje a un grupo, enviar un archivo a otro cliente,\
    publicar un archivo en un grupo, etc. Incluso, este cliente implementara \
    las funcionalidades elementales para poder enviar los mensajes cifrados, o sea \
    cifrado y descifrado de mensajes y archivos. \
    Para instanciar un cliente, es necesario introducir la direccion de un servidor de \
    ScrollMAT conocido, y pasar por todo el mecanismo de registro, login , etc, o simplemente \
    se puede cargar un cliente ya guardado utilizando la funcionalidad estatica *restore_client()*.
    '''

    def __init__(self, *args, **kwargs):
        raise NotImplementedError()

    def register(self, username, password, phone):
        '''
        Este procedimiento se reserva para la creacion de clientes que no esten presentes en el\
        servidor. Permite interactuar con el servidor para registrar un nuevo cliente \
        proporcionando un nombre de usuario, contrasena, y un telefono, y el servidor devolvera\
        un id con el que se identifica ese cliente a partir de ese momento.
        '''
        raise NotImplementedError()

    def loggin(self):
        '''
        Una vez registrado el cliente, con cada inicio de sesion habra que logearse en el sistema.\
        Para ello se utiliza el identificador del cliente y se le proporciona el usuario y el passw\
        al servidor. El cliente debe guardar un hash de este passsword en algun estado, para poder\
        enviarlo al servidor con cada inicio de sesion.
        '''
        raise NotImplementedError()

    def send_message_client(self, target_client, message):
        '''
        Envia un mensaje al cliente destino.
        '''
        raise NotImplementedError()

    def send_message_group(self, target_group, message):
        '''
        Envia un mensaje al grupo destino.
        '''
        raise NotImplementedError()

    def send_adj_client(self, target_client, target_file):
        '''
        Envia un envie archivo al cliente destino.
        '''
        raise NotImplementedError()

    def post_adj(self, target_group, target_file):
        '''
        Envia un archivo a todos los miembros del grupo destino.
        '''
        raise NotImplementedError()

    def __encrypt(self, message, key):
        '''
        Mecanismo para cifrar un mensaje
        '''

    def __decrypt(self, message, key):
        '''
        Mecanismo para descifrar un mensaje
        '''
        raise NotImplementedError()

    def __generate_client_key(self):
        '''
        Genera una llave privada y otra global para este cliente. \
        Este procedimiento debe ser invocado como parte del mecanismo de registro.
        '''
        raise NotImplementedError()

    def save_client_state(self):
        '''
        Salva el estado del cliente, de modo que cada inicio de la app no requiera de todo un nuevo\
        proceso de registro y nueva generacion de las llaves, asi como de las sesiones.
        '''
        raise NotImplementedError()

    @staticmethod
    def restore_client(identifier):
        '''
        Carga el estado de un cliente guardado y devuelve una instancia de Client listo para usar.
        '''
        raise NotImplementedError()

    async def discover_online_contacts(self):
        '''
        Realiza un algoritmo de autodescubrimiento, basado en la informacion guardada sobre\
        los contactos de este cliente, de modo que se puede saber quienes estan online. \
        Este metodo debe ser asincrono para poder ejecutarse cada cierto periodo de tiempo\
        y que actualice los datos del cliente cada vez que obtenga resultados.
        '''
        raise NotImplementedError()

    # Permitir que los clientes sean usados como llaves de diccionarios.
    # Un posible valor de hash para los clientes puede ser una combinacion de
    # su llave publica con su llave privada
    def __hash__(self):
        raise NotImplementedError()


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
        self.sender = sender
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
    def timestamp(self):
        '''
        Devuelve la fecha en la que se envio el mensaje
        '''
        return self.timestamp
