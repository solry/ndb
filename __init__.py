import logging
import psycopg2
import psycopg2.extras


class DataBase:
    """Just create this Class instance passing connection parameters and enjoy"""

    def __init__(self, host, database, user, password, logging_level='debug'):
        """
        :param host: ip-address or DNS name of database host
        :param database: name of database
        :param user: username that have permissions to connect to database
        :param password: password
        :param logging_level: Choose from [ None, 'debug', 'info', 'warning', 'error' ]
                              This param set logging severity for all log messages
        """
        self.host = host
        self.password = password
        self.user = user
        self.database = database
        self.connect = psycopg2.connect(host=self.host, database=self.database, user=self.user, password=self.password)
        self.cursor = self.connect.cursor()
        self.dcursor = self.connect.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        if logging_level in [None, 'debug', 'info', 'warning', 'error']:
            self.logging_level = logging_level
        else:
            raise AssertionError('Wrong logging_level param')

        self.autocommit = True
        self._log(f'Connection established to host {host}')

    def do_connect(self):
        self.connect = psycopg2.connect(host=self.host, database=self.database, user=self.user, password=self.password)
        self.cursor = self.connect.cursor()
        self.dcursor = self.connect.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def check_and_reconnect(self):
        try:
            self.cursor.execute("SELECT 1")
            return True
        except psycopg2.OperationalError:
            self.do_connect()

        if self.connect.closed:
            return False
        return True

    def _log(self, msg, *args, **kwargs):
        """
        :param msg: message to be logged
                    Attention! Severity level of this log entry is defined in __init__ by logging_level param
        :param args: logging module args
        :param kwargs: logging module kwargs
        """
        if self.logging_level:
            log = getattr(logging, self.logging_level)
            log(msg, *args, **kwargs)

    def set_session(self, isolation_level=None, readonly=None, deferrable=None, autocommit=None):
        """
        :param isolation_level: set the isolation level for the next transactions/statements.
        The value can be one of the literal values READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ,
        SERIALIZABLE or the equivalent constant defined in the extensions module.

        :param readonly: if True, set the connection to read only; read/write if False.

        :param deferrable: if True, set the connection to deferrable; non deferrable if False.
        Only available from PostgreSQL 9.1.

        :param autocommit: switch the connection to autocommit mode: not a PostgreSQL session setting
        but an alias for setting the autocommit attribute.
        """
        if autocommit is not None:
            self.autocommit = autocommit
        self.connect.set_session(isolation_level, readonly, deferrable, autocommit)

    def commit(self):
        """Commit any pending transaction to the database."""
        return self.connect.commit()

    def rollback(self):
        """Roll back to the start of any pending transaction."""
        return self.connect.rollback()

    def close(self):
        """Close connection to database"""
        self.cursor.close()
        self.connect.close()

    def execute(self, query, param_tuple):
        self.check_and_reconnect()

        cursor = self.cursor

        # param_tuple = tuple((psycopg2.Binary(param) if isinstance(param, bytes) else param for param in param_tuple))

        try:
            cursor.execute(query, param_tuple)
        except (psycopg2.ProgrammingError, psycopg2.IntegrityError, psycopg2.InternalError) as error:
            self._log(error)
            self.rollback()
            raise error

        if self.autocommit:
            self.connect.commit()

        if any([query.upper().startswith("SELECT"),
                'RETURNING' in query.upper()]):
            return cursor.fetchall()
        else:
            return cursor.rowcount

    def _execute_base(self, cursor, query_string):
        """
        Base executor for queries
        Warning! This wrapper is not SQL-injection safe!
        Params are pass to queries by formatted string, not by psycopg execute method
        :param cursor: that might be dict cursor or regular cursor
        :param query_string: query itself

        :return:
        if this query begins with SELECT or contain RETURNING:
            return list of dicts or list of tuples (depends on cursor type)
        else:
            return count of affected rows
        """
        self.check_and_reconnect()
        self._log(f'Trying execute: {query_string}')
        try:
            cursor.execute(query_string)
        except (psycopg2.ProgrammingError, psycopg2.IntegrityError, psycopg2.InternalError) as error:
            self._log(error)
            self.rollback()
            raise error

        if self.autocommit:
            self.connect.commit()

        if any([query_string.upper().startswith("SELECT"),
                'RETURNING' in query_string.upper()]):
            return cursor.fetchall()
        else:
            return cursor.rowcount

    def query(self, query_string):
        """
        Execute query with regular cursor

        :param query_string: query itself
        :return: return _execute_base object - list of tuples
        """
        return self._execute_base(self.cursor, query_string)

    def dquery(self, query_string):
        """
        Execute query with dict cursor

        :param query_string: query itself
        :return: return _execute_base object - list of dicts
        """
        return self._execute_base(self.dcursor, query_string)

    def insert(self, table, insert_dict, returning=None):
        """
        Use this method to make an easy INSERT statement.
        Instead of VALUES, just give insert_dict:
            keys will becomes column names
            values will becomes column values
        :param table: which table do you like to INSERT INTO
        :param insert_dict: key-val that will be mapped to COLUMNS-VALUES
        :param returning: query will return you column value of inserted entry

        :return:
        if returning_id is True:
            return id of inserted entry
        else:
            return count of affected rows
        """
        key_str = ''
        val_str = ''
        for idx, pair in enumerate(insert_dict.items()):
            key, val = pair
            key_str += key

            if val is None:
                val_str += 'Null'
            elif val is True or val is False:
                val_str += str(val)
            else:
                val_str += f'$insert_string${val}$insert_string$'

            if idx + 1 != len(insert_dict):
                key_str += ','
                val_str += ','

        query_string = f'INSERT INTO {table} ({key_str}) VALUES ({val_str})'

        if returning:
            query_string += f' RETURNING {returning}'
            return self._execute_base(self.cursor, query_string)[0]

        return self._execute_base(self.cursor, query_string)

    def update(self, table, update_dict, where):
        """
        Use this method to make an easy UPDATE statement.
        Instead of SET values, just give update_dict:
            keys will becomes column names
            values will becomes column values
        :param table: which table do you like to UPDATE
        :param update_dict: key-val that will be mapped to COLUMNS-VALUES
        :param where: condition of update in regular SQL syntax

        :return:
            return count of affected rows
        """

        body = ''

        for idx, pair in enumerate(update_dict.items()):
            key, val = pair

            if val is None:
                val = 'Null'
            elif type(val) == str:
                val = f'\'{val}\''
            elif type(val) == int:
                pass
            else:
                raise TypeError(f'Type of value is incorrect: {type(val)}')

            body += f'\"{key}\"={val}'

            if idx + 1 != len(update_dict):
                body += ','

        query_string = f'UPDATE {table} SET {body} WHERE {where}'

        return self._execute_base(self.cursor, query_string)
