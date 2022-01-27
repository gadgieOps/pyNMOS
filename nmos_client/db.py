from typing import Any
import json
import psycopg2
from psycopg2 import pool
from threading import Thread
import logging
import websocket
from nmos_client.registry import Registry
from nmos_client.utility import RegistryNodeShared


class Database(Registry, RegistryNodeShared):
    """
    Interacts with the postgre database
    """

    def __init__(self, name: str, user: str, password: str, host: str, port: int) -> None:
        """

        Parameters
        ----------
        name: name of the database0
        user: username to log into database with
        password: password to log into data base with
        host: database IP or dns name
        port: port the database is listening on
        """
        self.log: logging.Logger = logging.getLogger(__name__)

        self.name: str = name
        self.user: str = user
        self.password: str = password
        self.host: str = host
        self.port: int = port

        self.websockets: dict[str:websocket] = {}

        # Create a pool of connections to the database
        self.log.info(f'Opening DB connection pool: {self.user}@{self.host}:{self.port}')
        self.db_connection_pool = psycopg2.pool.ThreadedConnectionPool(1, 7, user=self.user, password=self.password,
                                                                       host=self.host, port=self.port,
                                                                       database=self.name)
        if self.db_connection_pool:
            self.log.info('Created connection pool for database')

    ###
    # WS
    #

    def open_ws(self, id: str, ws_href: str, resource: str) -> None:
        """
        Parameters
        ----------
        id: UID of a subscription - used as websocket key
        resource: the resource that this websocket is subscribed to. Used to create database tables.

        ws_href: URL for the websocket
        """

        self.log.info(f'Creating websocket connection to {ws_href}')
        # https://stackoverflow.com/questions/26980966/using-a-websocket-client-as-a-class-in-python
        self.websockets[id] = \
            websocket.WebSocketApp(ws_href, on_message=lambda websock, message: self.__on_message(websock, message),
                                   on_open=lambda websock: self.__on_open(websock, resource))

        websock_thread = Thread(target=self.websockets[id].run_forever)
        websock_thread.start()

    def __on_open(self, websock: websocket.WebSocketApp, resource: str) -> None:
        """
        Creates a table in the database for each new websocket. If a table already exists for the resource,
        deletes it first
        Parameters
        ----------
        websock: websocket instance
        resource: The resource that the websocket is subscribed to. Used as table name in database.
        """

        self.log.info(f'WS OPENED: {websock.url} {resource}')

        if self.__check_table_exists(resource):
            self.log.warning(f'Found stale table for {resource}. Removing ...')
            self.__delete_table(resource)

        self.__create_table(resource)

    def __on_message(self, websock: websocket.WebSocketApp, message: str) -> None:
        """
        Receives message from web socket. Decides if it is added, removed, modified, sync event and passes onto
        necessary method.

        Parameters
        ----------
        websock: websocket instance
        message: from websocket
        """
        # Convert JSON message to python dict
        message = json.loads(message)

        self.log.info(f'Message received on websocket from subscription: {message["flow_id"]}')
        self.log.info(f'Message contains {len(message["grain"]["data"])} events')
        self.log.debug(f'Message data: {message}')

        topic = message['grain']['topic'][1:-1]

        event = ''

        # Extract data into a list of tuples list('UID', DATA). This is to allow multiple records to be updated into
        # the database in a single transaction.
        pre_data = []
        post_data = []
        for data in message['grain']['data']:
            if 'post' in data.keys() and 'pre' not in data.keys():
                post_data.append((data['post']['id'], json.dumps(data['post'])))
                event = 'create'
            elif 'pre' in data.keys() and 'post' not in data.keys():
                pre_data.append((data['pre']['id'], json.dumps(data['pre'])))
                event = 'delete'
            elif 'pre' in data.keys() and 'post' in data.keys():
                post_data.append((data['post']['id'], json.dumps(data['post'])))
                pre_data.append((data['pre']['id'], json.dumps(data['pre'])))
                if data['pre'] == data['post']:
                    event = 'sync'
                else:
                    event = 'modify'

        if event == 'create':
            self.__create_record(topic, post_data)
        elif event == 'delete':
            self.__delete_record(topic, pre_data)
        elif event == 'sync':
            self.__create_record(topic, pre_data)
        elif event == 'modify':
            self.__delete_record(topic, pre_data)
            self.__create_record(topic, pre_data)

        ###
        # Database
        #

    ###
    # Database transactions
    #

    def __create_table(self, table_name: str) -> None:
        """
        Creates a table on the database.

        Currently, can only create a table for the base resources.

        Parameters
        ----------
        table_name name of the table to be created


        """
        self.log.info(f'Creating table: {table_name}')
        transaction = (f'''CREATE TABLE {table_name}
                   (UID TEXT PRIMARY KEY     NOT NULL,
                   DATA           JSONB    NOT NULL);''')
        self.__transact(transaction)

    def __delete_table(self, table_name: str) -> None:
        """
        Removes a table from the database
        Parameters
        ----------
        table_name name of the table to remove
        """
        self.log.info(f'Removing table: {table_name}')
        transaction = f'DROP TABLE {table_name};'
        self.__transact(transaction)

    def __check_table_exists(self, table_name: str) -> bool:
        """
        Queries the database for the existence of a table.
        Parameters
        ----------
        table_name name of the table to be searched for

        Returns
        -------
        True/False depending on if the table is found
        """

        self.log.info(f'Checking database for table: {table_name}')
        transaction = f"select exists(select relname from pg_class where relname = '{table_name}')"

        if self.__transact(transaction, check=True):
            self.log.info(f'Found {table_name} table in database')
            return True
        else:
            self.log.info(f'Did not find {table_name} table in database')
            return False

    def __create_record(self, table: str, data: list[tuple]) -> None:
        """
        Creates records in a table.
        Parameters
        ----------
        table the table to add the records to
        data the data to put into the table.
        """
        self.log.debug(f'Adding records to database table: {table}')

        a = ''
        for t in data:
            a += f"('{t[0]}', '{t[1]}'), "

        transaction = f"INSERT INTO {table} (UID,DATA) VALUES "
        transaction += a[:-2]

        self.__transact(transaction)

    def __delete_record(self, table: str, data: list[tuple]) -> None:
        """
        Removes records from a table.
        Parameters
        ----------
        table the table to add the records to
        data the data to put into the table.
        """

        a = '('
        for t in data:
            a += f"'{t[0]}', "

        transaction = f"DELETE FROM {table} WHERE UID IN"
        transaction += a[:-2]
        transaction += ')'

        self.log.debug(f'Removing records from database table: {table}')
        self.__transact(transaction)

    def __check_record_exists(self, table: str, id: str) -> bool:
        """
        Tests for the existance of a record in a table
        Parameters
        ----------
        table: the table that the record is (or isn't)
        id: the UID of the record to be queried

        Returns
        -------
        True/False depending on if the record is found

        """

        self.log.debug(f'Checking table for UID: {id}')
        transaction = f"SELECT UID FROM {table} WHERE UID = '{id}'"

        if self.__transact(transaction, check=True):
            self.log.debug(f'Found {id} in {table}')
            return True
        else:
            self.log.debug(f'Did not find {id} in {table}')
            return False

    def __transact(self, transaction: str, check: bool = False, fetch: bool = False) -> Any:
        """
        Sends a single transaction to the database. A single transaction may have multiple records.
        Parameters
        ----------
        transaction SQL string that is send to the database
        check: is used to check if a value exists or not in the database

        Returns
        -------
        True/False. Used to determine if a post transaction query of the database was successful. Only used when the
        mode is read. Write transactions return false.

        """

        found = False
        results = ''

        self.log.debug('Requesting connection from connection pool ..')
        connection = self.db_connection_pool.getconn()

        if connection:
            self.log.debug('Got connection from pool')
        else:
            self.log.error('Unable to get connection from pool')

        cursor = connection.cursor()

        try:
            cursor.execute(transaction)
            if check:
                found = cursor.fetchone()[0]
            if fetch:
                results = cursor.fetchall()
            connection.commit()
        except(Exception) as e:
            self.log.error(e)
            connection.rollback()
        cursor.close()
        self.db_connection_pool.putconn(connection)

        if fetch:
            return results
        else:
            return found

    ###
    # Inherited
    #

    def _search_reg(self, path: str, *keys: str, **qstr: str) -> Any:
        """
        Inherited from Registry. This method takes the supplied data and returns data formatted from the server.
        This method overrides the HTTP behaviour of interacting a NMOS registry and instead constructs SQL strings to
        query the database.
        """

        if len(qstr.keys()) > 1:
            raise ValueError(f'Can only supply one query string, got: {len(qstr.keys())}')

        if qstr:

            for k, v in qstr.items():
                key = str(k)
                value = str(v)

            d = self.__transact(f"SELECT data from {path} WHERE data ->> '{key}' = '{value}';", fetch=True)
        else:
            d = self.__transact(f'SELECT data FROM {path}', fetch=True)

        data = [record[0] for record in d]

        if not data:
            self.log.error(f'query returned no results for {path}')
            raise LookupError(f'query returned no results for {path}')
        else:
            return self._filter_data(data, *keys)
