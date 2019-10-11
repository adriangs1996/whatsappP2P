'''
Interface para interactuar con la base de datos de los clientes en cada Tracker.
'''
import sqlite3 as lite


def setup_client(database_name):
    '''
    Crea la Base de Datos de los clientes para un tracker.
    '''
    connection = None
    try:
        connection = lite.connect(database_name)
        # Crea un cursor con el mismo comportamiento que un diccionario
        # de python
        connection.row_factory = lite.Row

        cur = connection.cursor()
        cur.execute("CREATE TABLE clients (id INT, name TEXT, phone INT,"+
                    "username TEXT, password TEXT, address INT)")
        connection.commit()
        return connection

    except lite.Error as exception:

        if not connection is None:
            connection.rollback()

        print(exception)
        return connection

def insert_client_row(database_connection: lite.Connection,
                      _id: int,
                      name: str,
                      phone: int,
                      username: str,
                      password: str,
                      address: int):
    '''
    Inserta un nuevo registro en la Base de Datos de los clientes.
    '''
    with database_connection:
        cursor = database_connection.cursor()
        cursor.execute(f'INSERT INTO clients VALUES({_id} {name}' +
                       f' {phone} {username} {password} {address}')

def update_client_address(db_connection: lite.Connection,
                          client_id: int,
                          new_address: str):
    '''
    Actualiza el valor de la direccion ip del cliente.
    Util para los metodos de localizacion.
    '''
    with db_connection:
        cursor = db_connection.cursor()
        cursor.execute(f'UPDATE clients SET address={new_address}'+
                       f' WHERE id = {client_id}')
