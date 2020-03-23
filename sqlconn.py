import pandas as pd  # We use pandas SQL functions to retrieve our table queries as pandas dataframes
import pytds.login  # provided because SQL Servers need an authentication method (ntlm)
import sqlalchemy  # the underlying SQL connections are managed by SQLAlchemy
import os
from contextlib import contextmanager

from sqlconn.sqlparams import SQLParams
from sqlconn.mssqlbridge import MsSQLBridge
from sqlconn.mssqlnoauthbridge import MsSQLNoauthBridge
from sqlconn.postgresbridge import PostgresBridge
from sqlconn.snowflakebridge import SnowflakeBridge
from sqlconn.sqllitebridge import SQLLiteBridge
from sqlconn.basesqlbridge import BaseSQLBridge


class SQLConn(object):
    """
    Provides an interface to a SQL database, hiding the type of server (hopefully).
    """

    # Constants for the different type of SQL databases we currently support.
    MSSQL = 'mssql'
    MSSQLNOAUTH = 'mssqlnoauth'
    POSTGRES = 'postgresql+psycopg2'
    SQLITE = 'sqlite'
    SNOWFLAKE = 'snowflake'

    BULK_FORCE = 0
    BULK_OFF = 1
    BULK_CHANCE = 2

    def __init__(self, _sql_params):
        """
        Use the SQL parameters to create our SQL Alchemy engine. An object of the class is not intended to be shared
        by multiple threads as the SQLAlchemy engine is not thread-safe

        :param _sql_params: An SQLParams object that contains the necessary information to connect to any of our
                            database types.
        """
        self.sql_params = _sql_params
        self.sql_bridge = self.bridge_factory(self.sql_params.type)
        self.sql_engine = self.sql_bridge.get_engine(_sql_params)

    @classmethod
    def get_connection(cls, sql_nickname):
        """
        The preferred method of obtaining a SQL connection.

        :param sql_nickname: A nickname representing sql parameters for a connection.
        :return: Returns a SQLConn object.
        """
        return cls(SQLParams.from_json(sql_nickname=sql_nickname))

    @classmethod
    def change_database(cls, sql_nickname, database_name):
        """
        Gets the SQL Connection for the provided nickname, but then creates a new connection for the database passed.

        :param sql_nickname: A nickname representing SQL parameters for a connection.
        :type sql_nickname: str
        :param database_name: Database name that replaces default databse name.
        :type database_name: str
        :return: A SQLConn object
        :rtype: SQLConn
        """
        conn = SQLConn.get_connection(sql_nickname)
        params = conn.sql_params
        params.database = database_name
        params.diff_database = True
        return SQLConn(params)

    @staticmethod
    def get_sql_nicknames():
        """
        :return: Returns the available list of sql nicknames.
        """
        return SQLParams.get_sql_nicknames()

    def get_nickname(self):
        return self.sql_params.get_nickname()

    def get_dataframe(self, sql, **kwargs):
        """
        Simply execute the provided sql and return a dataframe with the results

        :param sql: The sql query that needs to be executed that should include a select statement
        :return: Returns the results of the sql query as a pandas dataframe
        """
        assert 'select'.upper() in sql.upper()
        df = pd.DataFrame()
        try:
            with self.sql_engine.connect() as connection:
                df = pd.read_sql(sql, connection, **kwargs)
        except Exception as e:
            # We treat the exceptions we see as connection errors and the best way we know to handle them is by
            # resetting the pool of connections for the engine.
            self.sql_engine.dispose()
            with self.sql_engine.connect() as connection:
                df = pd.read_sql(sql, connection, **kwargs)
        return df

    def execute_sql(self, sql):
        """
        Simply execute the query

        :param sql: The sql query that needs to be executed
        """
        try:
            with self.sql_engine.connect() as connection:
                connection.execute(sql)
        except Exception as e:
            # We treat the exceptions we see as connection errors and the best way we know to handle them is by
            # resetting the pool of connections for the engine.
            self.sql_engine.dispose()
            with self.sql_engine.connect() as connection:
                connection.execute(sql)

    def get_engine(self):
        """
        Provides the SQLAlchemy engine in case the user wants to do some direct SQL queries we do not have available

        :return: SQLAlchemy engine for the SQL database
        """
        return self.sql_engine

    def append_to_table(self, table_name, data_to_append, if_exists='append', schema=None, bulk_copy=BULK_CHANCE,
                        chance_min_length=100, **kwargs):
        """
        Attempts to append the provided dataframe to the provided sql table name. The method will first remove any
        columns in the dataframe that are not available in the table. NOTE: THIS ONLY WORKS FOR POSTGRESQL AT THE
        MOMENT!!!!

        :param table_name: The name of the sql table to append the dataframe.
        :param data_to_append: Either a dataframe or pandas series object.
        :param if_exists: Provides an option to override the to_sql parameter for how we treat a possible existing table
        :param schema: Provides option to override the to_sql schema parameter.
        :param bulk_copy: We allow three different options here, either force, off, or chance. The chance option will
                          do a bulk copy if the length of the dataframe passed is > 100.
        :param chance_min_length: If someone sends bulk chance, then we will try to bulk load the table to the
                                  database if the length of the dataframe is greater than this value.
        """
        if type(data_to_append) == pd.Series:
            temp_df = pd.DataFrame(data_to_append).transpose()
        else:
            temp_df = data_to_append.copy(deep=True)

        table_name, schema_name = self.get_names(table=table_name,
                                                 schema=schema)
        sql_columns_list = self.sql_bridge.get_columns(table_name=table_name,
                                                       schema_name=schema_name)

        if sql_columns_list:
            temp_df = self.sql_bridge.get_df_interesection(temp_df, sql_columns_list)
            table_state = BaseSQLBridge.TABLE_STATE_EXISTS
        else:
            table_state = BaseSQLBridge.TABLE_STATE_NO_EXISTS

        if bulk_copy == SQLConn.BULK_CHANCE and len(temp_df) > chance_min_length:
            bulk_copy = SQLConn.BULK_FORCE
        try:
            if bulk_copy == SQLConn.BULK_FORCE:
                self.sql_bridge.bulk_load(bulk_df=temp_df,
                                          table_name=table_name,
                                          schema_name=schema_name,
                                          table_state=table_state,
                                          if_exists=if_exists,
                                          **kwargs)
        except:
            bulk_copy = SQLConn.BULK_OFF

        if bulk_copy in [SQLConn.BULK_OFF, SQLConn.BULK_CHANCE]:
            try:
                temp_df.to_sql(table_name, self.sql_engine, if_exists=if_exists, index=False, schema=schema_name, **kwargs)
            except Exception as e:
                # We treat the exceptions we see as connection errors and the best way we know to handle them is by
                # resetting the pool of connections for the engine.
                self.sql_engine.dispose()
                temp_df.to_sql(table_name, self.sql_engine, if_exists=if_exists, index=False, schema=schema_name, **kwargs)

    def bridge_factory(self, sql_type):
        """
        :param sql_type: One of our supported SQL Types
        :type sql_type: str
        :return: Returns a bridge object that helps with type specific SQL operations.
        :rtype: BaseSQLBridge
        """
        if sql_type == SQLConn.SNOWFLAKE:
            return SnowflakeBridge(sql_conn=self)
        elif sql_type == SQLConn.MSSQL:
            return MsSQLBridge(sql_conn=self)
        elif sql_type == SQLConn.MSSQLNOAUTH:
            return MsSQLNoauthBridge(sql_conn=self)
        elif sql_type == SQLConn.POSTGRES:
            return PostgresBridge(sql_conn=self)
        elif sql_type == SQLConn.SQLITE:
            return SQLLiteBridge(sql_conn=self)
        else:
            raise RuntimeError(f'We do not support a bulk load for SQLConn connection type {self.sql_params.type}')

    def get_names(self, table, schema=None):
        """
        Parses out the names of our table and our schema. We first try to extract the schema name from the table name
        and if that doesn't work then we look for the schema parameter, and last default to our bridge's default
        schema.

        :param table: Table name. Could include schema could not include schema.
        :type table: str
        :param schema: Name of the schema
        :type schema: str
        :return: table name, schema name
        :rtype: str, str
        """
        table_name = table.split('.')[-1]
        if len(table.split('.')) > 1:
            schema_name = table.split('.')[-2]
        else:
            schema_name = self.sql_bridge.default_schema() if schema is None else schema
        return table_name, schema_name
