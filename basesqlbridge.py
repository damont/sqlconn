from abc import ABC, abstractmethod
import pandas as pd
import os


class BaseSQLBridge(ABC):
    """
    Our SQL Bridge classes are meant to serve as a bridge between the SQLConn class and any SQL type specific calls
    and decisions that need to be made.

    THIS CLASS SHOULD NOT EVER BE INSTANTIATED, BUT SHOULD BE INHERITED FROM AND ITS BASE CLASSES SHOULD BE USED!!!!
    """
    TABLE_STATE_UNKNOWN = 0
    TABLE_STATE_EXISTS = 1
    TABLE_STATE_NO_EXISTS = 2

    def __init__(self, sql_conn):
        """
        Save off any parameters passed to the function

        :param sql_conn: A SQLConnection to a database of our type
        :type sql_conn: SQLConn
        """
        self.sql_connection = sql_conn

    @abstractmethod
    def get_engine(self, sql_params):
        """
        Creates an engine for the sql connector.

        :param sql_params: Parameters needed to create the engine
        :type sql_params: SQLParams
        :return: Returns the engine
        :rtype: sqlALchemy engine
        """
        pass

    @staticmethod
    @abstractmethod
    def default_schema():
        """
        :return: Returns the default schema for the SQL type
        :rtype: str
        """
        pass

    def get_columns(self, table_name, schema_name):
        """
        Returns a list of the table columns.

        :param table_name: Name of the table
        :type table_name: str
        :param schema_name: Name of the schema
        :type schema_name: str
        :return: Returns a list of the columns. If empty list then the table does not exist.
        :rtype: list
        """
        columns_df = self.sql_connection.get_dataframe(self.columns_sql(table_name, schema_name))
        if len(columns_df) > 0:
            return list(columns_df['column_name'])
        else:
            return []

    @classmethod
    def columns_sql(cls, table_name, schema_name):
        """
        :param table_name: Name of the table
        :type table_name: str
        :param schema_name: Name of the schema
        :type schema_name: str
        :return: Returns the SQL for retrieving the column names
        :rtype: str
        """
        table_name, schema_name = cls.alter_names(table_name, schema_name)
        return f"""SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                    AND   table_schema = '{schema_name}'"""

    @classmethod
    def alter_names(cls, table_name, schema_name):
        """
        :param table_name: Name of the table
        :type table_name: str
        :param schema_name: Name of the schema
        :type schema_name: str
        :return: Returns the same values passed into the function
        :rtype: str, str
        """
        return table_name, schema_name

    def get_df_interesection(self, df, sql_columns):
        """
        We need to remove any columns that are in the df but not in the sql_columns

        :param df: Dataframe to remove from
        :type df: pd.DataFrame
        :param sql_columns: List of columns from the sql database that we are adding the df to.
        :type sql_columns: list
        :return: Return the dataframe with columns removed.
        :rtype: pd.DataFrame
        """
        df_columns = list(df.columns)
        bad_columns = list(set(df_columns).difference(set(sql_columns)))
        return df.drop(bad_columns, axis=1)

    @abstractmethod
    def bulk_load(self, bulk_df, table_name, schema_name, table_exists=TABLE_STATE_UNKNOWN, if_exists='append', **kwargs):
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
        pass

    def _determine_table(self, bulk_df, table_name, schema_name, table_state, if_exists):
        """
        Will DETERMINE whether or not we need to drop (D) the table and whether or not we need to create (C) the table.
        Below is the intersection of how we act based upon the value of table_state and if_exists.

                    exists        not exists
        replace     D + C         C
        append      X             C

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
        """

        assert if_exists in ['replace', 'append'], 'We only support replace and append values for if_exists!'

        if table_state == BaseSQLBridge.TABLE_STATE_UNKNOWN:
            if self.get_columns(table_name, schema_name):
                table_state = BaseSQLBridge.TABLE_STATE_EXISTS
            else:
                table_state = BaseSQLBridge.TABLE_STATE_NO_EXISTS

        pd_sql_engine = pd.io.sql.pandasSQL_builder(self.sql_connection.get_engine(),
                                                    schema=schema_name)

        # First thing need to decide if we should drop the table.
        if table_state == BaseSQLBridge.TABLE_STATE_EXISTS and if_exists == 'replace':
            self.sql_connection.execute_sql(f"""DROP TABLE {schema_name}.{table_name};""")
            table_state = BaseSQLBridge.TABLE_STATE_NO_EXISTS

        if table_state == BaseSQLBridge.TABLE_STATE_NO_EXISTS:
            table = pd.io.sql.SQLTable(table_name, pd_sql_engine, frame=bulk_df,
                                       index=False, schema=schema_name)
            table.create()

    @staticmethod
    def save_to_csv(bulk_df, full_csv_name):
        """
        Save off the dataframe into the provided CSV file name.

        :param bulk_df: Dataframe we are saving off to CSV
        :type bulk_df: pd.DataFrame
        :param full_csv_name: Full path name to the CSV
        :type full_csv_name: Path
        """
        bulk_df.to_csv(full_csv_name, header=False, index=False, sep='\t', encoding='ascii')

    @staticmethod
    def _remove_csv(full_csv_name):
        """
        Removes the CSV from the file system.

        :param full_csv_name: Full path name to the CSV
        :type full_csv_name: Path
        """
        if os.path.exists(full_csv_name):
            os.remove(full_csv_name)