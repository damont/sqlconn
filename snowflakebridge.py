import os
import sqlalchemy
from pathlib import Path

from sqlconn.basesqlbridge import BaseSQLBridge


class SnowflakeBridge(BaseSQLBridge):
    """
    Handles any Snowflake relations that need to be specific to the different SQL types.
    """

    def __init__(self, sql_conn):
        """
        Save off any parameters passed to the function

        :param sql_conn: A SQLConnection to a database of our type
        :type sql_conn: SQLConn
        """
        super(SnowflakeBridge, self).__init__(sql_conn)

    def get_engine(self, sql_params):
        """
        Creates an engine for the sql connector.

        :param sql_params: Parameters needed to create the engine
        :type sql_params: SQLParams
        :return: Returns the engine
        :rtype: sqlalchemy engine
        """
        return sqlalchemy.create_engine(f'{sql_params.type}://{sql_params.username}:'
                                        f'{sql_params.password}@{sql_params.host}/'
                                        f'{sql_params.database}')

    @staticmethod
    def default_schema():
        """
        :return: Returns the default schema for the SQL type
        :rtype: str
        """
        return 'dbo'

    @classmethod
    def alter_names(cls, table_name, schema_name):
        """
        For Snowflake, the column inspection SQL always returns capitalized strings for the column names

        :param table_name: Name of the table
        :type table_name: str
        :param schema_name: Name of the schema
        :type schema_name: str
        :return: Return the uppercase table and schema
        :rtype: str, str
        """
        return table_name.upper(), schema_name.upper()

    def get_df_interesection(self, df, sql_columns):
        """
        We need to remove any columns that are in the df but not in the sql_columns. We have to make a function for
        the snowflake class because the sql columns are returned all caps.

        :param df: Dataframe to remove from
        :type df: pd.DataFrame
        :param sql_columns: List of columns from the sql database that we are adding the df to.
        :type sql_columns: list
        :return: Return the dataframe with columns removed.
        :rtype: pd.DataFrame
        """
        sql_columns_upper = [x.upper() for x in sql_columns]
        df_columns = list(df.columns)
        df_columns_upper = [x.upper() for x in df_columns]
        bad_columns = list(set(df_columns_upper).difference(set(sql_columns_upper)))
        bad_indices = []
        for col in bad_columns:
            if col in df_columns_upper:
                bad_indices.append(df_columns.index(col))
        for idx in bad_indices:
            df.drop(df_columns[idx], axis=1, inplace=True)
        return df

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
        full_csv_name = Path(kwargs['tmp_dir'], table_name + '.csv')
        self.file_cleanup(full_csv_name=full_csv_name,
                          table_name=table_name)
        self._determine_table(bulk_df=bulk_df,
                              table_name=table_name,
                              schema_name=schema_name,
                              table_state=table_state,
                              if_exists=if_exists)
        self.save_to_csv(bulk_df=bulk_df,
                         full_csv_name=full_csv_name)
        self._push_to_table(table_name=table_name,
                            schema_name=schema_name,
                            full_csv_name=full_csv_name)
        self.file_cleanup(full_csv_name=full_csv_name,
                          table_name=table_name)

    def _push_to_table(self, table_name, schema_name, full_csv_name):
        """
        Writes the CSV file into the user's snowflake staging area and then performs a COPY INTO into the table

        :param table_name: Name of the table
        :type table_name: str
        :param schema_name: Name of the schema
        :type schema_name: str
        :param full_csv_name: Full path name to the CSV
        :type full_csv_name: str
        """
        self.sql_connection.execute_sql(f"""PUT file://{full_csv_name} @~/tmp/{table_name}.csv;""")
        # Need to create the table in between, but it will handle everything after we
        self.sql_connection.execute_sql(f"""COPY INTO {schema_name}.{table_name} 
                                            FROM '@~/tmp/{table_name}.csv' 
                                            FILE_FORMAT= (TYPE=CSV, FIELD_DELIMITER = "\t")""")

    def file_cleanup(self, full_csv_name, table_name):
        """
        Removes the csv file from the file system and removes the file from the user's staging area

        :param table_name: Name of the table
        :type table_name: str
        :param full_csv_name: Full path name to the CSV
        :type full_csv_name: str
        """
        if os.path.exists(full_csv_name):
            os.remove(full_csv_name)
        # We must try and drop the table as well as anything in the staging
        self.sql_connection.execute_sql(f"""REMOVE @~/tmp/{table_name}.csv""")
