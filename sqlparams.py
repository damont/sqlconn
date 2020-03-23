from sqlconn import SQL_PARAMS


class SQLParams(object):
    """
    Contains the parameters needed for connecting to a SQL database
    """

    def __init__(self, _host, _database, _username, _password, _port, _type):
        """
        Stores the necessary parameters for connecting to a database

        :param _host: The host or server name. Include the instance if needed.
        :param _database: Name of the database.
        :param _username: The user that should be connecting.
        :param _password: The password for the user.
        :param _port: Only used for Postgres tables at the moment.
        :param _type: Either SQLConn.MSSQL, SQLConn.POSTGRES, SQLConn.SQLITE
        """
        self.host = _host
        self.database = _database
        self.username = _username
        self.password = _password
        self.port = _port
        self.type = _type

        # We allow changing the default database and this is something that should be updated if we do.
        self.diff_database = False

    @classmethod
    def from_json(cls, sql_nickname):
        """
        Based upon the input nickname, we look for the sql parameters in the dictionary contained at the top of
        this file. NOTE:  In order to see the currently available sql nicknames use the provided function,
        get_sql_nicknames.

        :param sql_nickname: A nickname representing a set of sql parameters.
        :return: Returns an SQLParams object that can be used to create a SQLConn object.
        """
        if sql_nickname in list(SQL_PARAMS.keys()):
            wanted_sql_params = SQL_PARAMS[sql_nickname]
        else:
            raise KeyError('{0:s} is not a valid sql nickname'.format(sql_nickname))
        return cls(wanted_sql_params['host'],
                   wanted_sql_params['database'],
                   wanted_sql_params['username'],
                   wanted_sql_params['password'],
                   wanted_sql_params['port'],
                   wanted_sql_params['type'])

    @staticmethod
    def get_sql_nicknames():
        """
        :return: Returns the available list of sql nicknames.
        """
        return list(SQL_PARAMS.keys())

    def get_nickname(self):
        """
        Provides the nickname for the current group of sql parameters
        """
        for key in SQL_PARAMS.keys():
            if ((SQL_PARAMS[key]['host'] == self.host) and
                    ((SQL_PARAMS[key]['database'] == self.database) or self.diff_database) and
                    (SQL_PARAMS[key]['username'] == self.username) and
                    (SQL_PARAMS[key]['password'] == self.password) and
                    (SQL_PARAMS[key]['port'] == self.port) and
                    (SQL_PARAMS[key]['type'] == self.type)):
                return key
        # If we get here then we were not able to match our parameters to anything in our SQL dictionary
        raise NameError('SQL Parameters do not match any nickname')

    @staticmethod
    def add_sql_nickname(nickname, sqlparams_object):
        """
        This function is not thread-safe. Please only use this function before creating any threads/processes and just
        add to the beginning of a file.

        :param nickname: The nickname that will be used to retrieve the parameters for the connection.
        :param sqlparams_object: Since the parameters object already has everything needed for a connection, we will
                                 create the connection based upon the parameters.
        """
        if nickname in list(SQL_PARAMS.keys()):
            raise KeyError('This nickname is already taken')

        SQL_PARAMS[nickname] = {
                "host": sqlparams_object.host,
                "database": sqlparams_object.database,
                "username": sqlparams_object.username,
                "password": sqlparams_object.password,
                "port": sqlparams_object.port,
                "type": sqlparams_object.type
            }
