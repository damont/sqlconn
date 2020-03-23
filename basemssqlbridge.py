import os
from subprocess import call
from abc import abstractmethod
from pathlib import Path

from sqlconn.basesqlbridge import BaseSQLBridge


class BaseMsSQLBridge(BaseSQLBridge):
    """
    Handles shared SQL Server relations that need to be specific to the different SQL types.
    """

    def __init__(self, sql_conn):
        """
        Save off any parameters passed to the function

        :param sql_conn: A SQLConnection to a database of our type
        :type sql_conn: SQLConn
        """
        super(BaseMsSQLBridge, self).__init__(sql_conn)

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
    def default_schema():
        """
        :return: Returns the default schema for the SQL type
        :rtype: str
        """
        return 'dbo'

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
        if os.name != 'nt':
            full_script_name = Path(kwargs['tmp_dir'], table_name + '_loader_script.sh')
            full_csv_name = Path(kwargs['tmp_dir'], table_name + '.csv')
            self.file_cleanup(full_csv_name=full_csv_name,
                              full_script_name=full_script_name)
            self._determine_table(bulk_df=bulk_df,
                                  table_name=table_name,
                                  schema_name=schema_name,
                                  table_state=table_state,
                                  if_exists=if_exists)
            self.save_to_csv(bulk_df=bulk_df,
                             full_csv_name=full_csv_name)
            self._push_to_table(table_name=table_name,
                                schema_name=schema_name,
                                full_csv_name=full_csv_name,
                                full_script_name=full_script_name)
            self.file_cleanup(full_csv_name=full_csv_name,
                              full_script_name=full_script_name)
        else:
            raise RuntimeError('We cannot do SQL Server bulk load on Windows systems')

    def _push_to_table(self, table_name, schema_name, full_csv_name, full_script_name):
        """
        Creates the shell script that moves the CSV file contents into the database table

        :param table_name: Name of the table
        :type table_name: str
        :param schema_name: Name of the schema
        :type schema_name: str
        :param full_csv_name: Full path name to the CSV
        :type full_csv_name: Path
        :param full_script_name: The full path the shell script
        :type full_script_name: Path
        """
        bcp_load_string = "{0} {1}.{2} in '{3}' -U SONIC.COM\\\{4} " \
                          "-P {5} -S '{6}' -c -b 50000".format('freebcp',
                                                               self.sql_connection.sql_params.database,
                                                               schema_name + '.' + table_name,
                                                               full_csv_name,
                                                               self.sql_connection.sql_params.username.split('\\')[-1],
                                                               self.sql_connection.sql_params.password,
                                                               self.sql_connection.sql_params.host)
        with open(full_script_name, 'w') as f:
            f.write('#!/bin/bash\n')
            f.write('export TDSVER=7.2\n')
            f.write('export TDSPORT={0}\n'.format(self.sql_connection.sql_params.port))
            f.write(bcp_load_string)
        self.make_executable(full_script_name)
        call(['sh', full_script_name])

    @staticmethod
    def make_executable(file_name):
        """
        Used on unix systems for giving permissions so the script can actually be run

        :param file_name: Name of the file that needs the permissions.
        :type file_name: Path
        """
        st = os.stat(file_name).st_mode
        st |= (st & 0o444) >> 2
        os.chmod(file_name, st)

    def file_cleanup(self, full_csv_name, full_script_name):
        """
        Removes the csv file from the file system and removes the file from the user's staging area

        :param full_csv_name: Full path name to the CSV
        :type full_csv_name: Path
        :param full_script_name: Full path name to the CSV
        :type full_script_name: Path
        """
        if os.path.exists(full_csv_name):
            os.remove(full_csv_name)
        if os.path.exists(full_script_name):
            os.remove(full_script_name)
