import io
import sqlalchemy

from sqlconn.basesqlbridge import BaseSQLBridge


class PostgresBridge(BaseSQLBridge):
    """
    Handles any postgres relations that need to be specific to the different SQL types.
    """

    def __init__(self, sql_conn):
        """
        Save off any parameters passed to the function

        :param sql_conn: A SQLConnection to a database of our type
        :type sql_conn: SQLConn
        """
        super(PostgresBridge, self).__init__(sql_conn)

    def get_engine(self, sql_params):
        """
        Creates an engine for the sql connector.

        :param sql_params: Parameters needed to create the engine
        :type sql_params: SQLParams
        :return: Returns the engine
        :rtype: sqlalchemy engine
        """
        return sqlalchemy.create_engine('{0}://{1}:{2}@{3}:{4}/{5}'.format(sql_params.type,
                                                                           sql_params.username,
                                                                           sql_params.password,
                                                                           sql_params.host,
                                                                           sql_params.port,
                                                                           sql_params.database))

    @staticmethod
    def default_schema():
        """
        :return: Returns the default schema for the SQL type
        :rtype: str
        """
        return 'public'

    def bulk_load(self, bulk_df, table_name, schema_name, table_state=BaseSQLBridge.TABLE_STATE_UNKNOWN, if_exists='append', **kwargs):
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
        # load_df, table_existence = self._align_df(true_table_name, df, schema=schema)
        load_df = bulk_df.copy(deep=True)
        string_data_io = io.StringIO()
        load_df.to_csv(string_data_io, sep='|', index=False)

        self._determine_table(bulk_df=bulk_df,
                              table_name=table_name,
                              schema_name=schema_name,
                              table_state=table_state,
                              if_exists=if_exists)

        string_data_io.seek(0)
        columns = string_data_io.readline()  # remove header
        columns = columns.replace("|", ",")
        with self.sql_connection.get_engine().connect() as connection:
            with connection.connection.cursor() as cursor:
                copy_cmd = "COPY %s.%s (%s) FROM STDIN DELIMITER '|' CSV" % (schema_name,
                                                                             table_name,
                                                                             columns)
                cursor.copy_expert(copy_cmd, string_data_io)
            connection.connection.commit()

    def table_cleanup(self, table_name, schema_name):
        """
        Drops the table

        :param table_name: Table name
        :type table_name: str
        :param schema_name: Schema name
        :type schema_name: str
        """
        # We must try and drop the table as well as anything in the staging
        self.sql_connection.execute_sql(f"""DROP TABLE IF EXISTS {schema_name}.{table_name}""")
