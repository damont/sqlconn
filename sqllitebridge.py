import sqlalchemy

from sqlconn.basesqlbridge import BaseSQLBridge


class SQLLiteBridge(BaseSQLBridge):
    """
    Handles any SQL Lite relations that need to be specific to the different SQL types.
    """

    def __init__(self, sql_conn):
        """
        Save off any parameters passed to the function

        :param sql_conn: A SQLConnection to a database of our type
        :type sql_conn: SQLConn
        """
        super(SQLLiteBridge, self).__init__(sql_conn)

    def get_engine(self, sql_params):
        """
        Creates an engine for the sql connector.

        :param sql_params: Parameters needed to create the engine
        :type sql_params: SQLParams
        :return: Returns the engine
        :rtype: sqlalchemy engine
        """
        return sqlalchemy.create_engine('{0}:///{1}'.format(sql_params.type,
                                                            sql_params.database))

    @staticmethod
    def default_schema():
        """
        :return: Returns the default schema for the SQL type
        :rtype: str
        """
        return 'main'

    def bulk_load(self, bulk_df, table_name, schema_name, table_state=BaseSQLBridge.TABLE_STATE_UNKNOWN,
                      if_exists='append', **kwargs):
        """
        Perform a bulk copy into the table.

        :param bulk_df: Dataframe values to copy into the table
        :type bulk_df: pd.DataFrame
        :param table_name: Name of the table
        :type table_name: str
        :param schema_name: Name of the schema
        :type schema_name: str
        :param table_state: Tells whether the table exists or not
        :type table_state: int
        :param if_exists: Follows the pandas SQL functions if exists
        :type if_exists: str
        :param kwargs: Key word arguments if needed for the bulk load
        :type kwargs: dictionary
        """
        raise RuntimeError('We do not handle bulk loads for SQL Lite')
