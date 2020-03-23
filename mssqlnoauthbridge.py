import sqlalchemy

from sqlconn.basemssqlbridge import BaseMsSQLBridge


class MsSQLNoauthBridge(BaseMsSQLBridge):
    """
    Handles SQL Server relations not needing ntlm authorization that need to be specific to the different SQL types.
    """

    def __init__(self, sql_conn):
        """
        Save off any parameters passed to the function

        :param sql_conn: A SQLConnection to a database of our type
        :type sql_conn: SQLConn
        """
        super(MsSQLNoauthBridge, self).__init__(sql_conn)

    def get_engine(self, sql_params):
        """
        Creates an engine for the sql connector.

        :param sql_params: Parameters needed to create the engine
        :type sql_params: SQLParams
        :return: Returns the engine
        :rtype: sqlALchemy engine
        """
        # The NOAUTH version uses pytds, but does not pass the 'auth' connection argument
        if '\\' in sql_params.host:
            return sqlalchemy.create_engine('{0}+pytds://{1}:{2}@{3}/{4}'.format(sql_params.type,
                                                                                 sql_params.username,
                                                                                 sql_params.password,
                                                                                 sql_params.host,
                                                                                 sql_params.database),
                                            connect_args={'autocommit': True})
        else:
            return sqlalchemy.create_engine('{0}+pytds://{1}:{2}@{3}:{4}/{5}'.format(sql_params.type,
                                                                                     sql_params.username,
                                                                                     sql_params.password,
                                                                                     sql_params.host,
                                                                                     sql_params.port,
                                                                                     sql_params.database),
                                            connect_args={'autocommit': True})
