import datetime
import pandas
import numbers
import socket

from sqlconn.sqlconn import SQLConn


class SQLQueue(object):
    """
    The purpose of this class is to provide a queue that can be accessed across multiple environments.
    """

    # These are the names of columns that will be added and managed by the queue.
    SQ_PRIORITY = 'sq_priority'
    SQ_STATUS = 'sq_status'
    SQ_ID = 'sq_id'
    SQ_PUT_TIME = 'sq_put_time'
    SQ_PUT_HOSTNAME = 'sq_put_hostname'
    SQ_CLAIM_TIME = 'sq_claim_time'
    SQ_CLAIM_HOSTNAME = 'sq_claim_hostname'
    SQ_GET_TIME = 'sq_get_time'
    SQ_GET_HOSTNAME = 'sq_get_hostname'
    SQ_FINISH_TIME = 'sq_finish_time'

    # We restrict the priority of the queue between these two values.
    MAX_PRIORITY = 10
    MIN_PRIORITY = 1

    # These are the different status the objects in the queue can have.
    STATUS_AVAILABLE = 'AVAILABLE'
    STATUS_CLAIMED = 'CLAIMED'
    STATUS_PROGRESS = 'IN_PROGRESS'
    STATUS_COMPLETED = 'COMPLETED'
    STATUS_NOEXIST = 'DOESNOTEXIST'
    STATUS_EXCEPTION = 'EXCEPTION'
    STATUS_DESTROYED = 'DESTROYED'
    STATUS_RECOVERABLE = 'RECOVERABLE'

    def __init__(self, sql_conn, squeue_name):
        """
        Constructor for a SQLQueue class. Will ensure that the squeue exists.

        :param sql_conn: A SQLConn connection object that will be used to access the queue.
        :param squeue_name: The name squeue (Really the name of the table)
        """
        self.sql_conn = sql_conn
        self.squeue = squeue_name
        try:
            self.sql_conn.execute_sql('SELECT sq_priority, sq_status, sq_id FROM {0:s} LIMIT 1'.format(self.squeue))
        except:
            raise RuntimeError('Trouble selecting from {0:s}'.format(self.squeue))

    def put(self, df, priority_included=True, priority=MIN_PRIORITY):
        """
        Adds all the rows in the dataframe to the squeue.

        :param df: Dataframe containing the necessary rows.
        :param priority_included: Tells whether or not the dataframe object has a priority column. True if it does.
        :param priority: If it does not have a priority column, then this priority will be given to all of the
                         rows in the dataframe.
        """
        if self.SQ_PRIORITY not in list(df.columns):
            # Don't trust the caller, they might have named the priority column incorrectly
            priority_included = False

        priority = self.MAX_PRIORITY if priority > self.MAX_PRIORITY else priority
        priority = self.MIN_PRIORITY if priority < self.MIN_PRIORITY else priority

        if priority_included is not True:
            # If priority is not already present in the dataframe then we need to add a priority column
            df.loc[:, self.SQ_PRIORITY] = priority

        # Need to add unique identifiers to the list
        df.loc[:, self.SQ_STATUS] = self.STATUS_AVAILABLE
        df.loc[:, self.SQ_PUT_HOSTNAME] = socket.gethostname()

        self.sql_conn.append_to_table(table_name=self.squeue,
                                      data_to_append=df)

    def claim(self, conditional_claim=None, join_text=''):
        """
        Claims the next highest priority available row.

        :return: Returns the squeue ID for the row.
        """
        # The assumption is we just want to claim the next available highest priority and have its id number returned
        # to us so we can use that later to retrieve it.
        if not conditional_claim:
            conditional_claim = ''
        else:
            conditional_claim = 'and ' + conditional_claim
        with self.sql_conn.get_engine().connect() as connection:
            sql_select = """SELECT {3:s} FROM {0:s} 
                            {6:s}
                            WHERE {1:s} = '{2:s}' {5:s} 
                            ORDER BY {4:s} DESC, {3:s} ASC LIMIT 1 FOR UPDATE OF {0:s};""".format(self.squeue,
                                                                                                  self.SQ_STATUS,
                                                                                                  self.STATUS_AVAILABLE,
                                                                                                  self.SQ_ID,
                                                                                                  self.SQ_PRIORITY,
                                                                                                  conditional_claim,
                                                                                                  join_text)
            row_id_df = pandas.read_sql(sql_select, connection)
            if len(row_id_df) > 0:
                sql_update = """UPDATE {0:s} SET {1:s} = '{2:s}',
                                                 {3:s} = now(),
                                                 {6:s} = '{7:s}' 
                                WHERE {4:s} = {5:d};""".format(self.squeue,
                                                               self.SQ_STATUS,
                                                               self.STATUS_CLAIMED,
                                                               self.SQ_CLAIM_TIME,
                                                               self.SQ_ID,
                                                               row_id_df.loc[0, self.SQ_ID],
                                                               self.SQ_CLAIM_HOSTNAME,
                                                               socket.gethostname())
                connection.execute(sql_update)
                return row_id_df.loc[0, self.SQ_ID]
        return -1

    def get(self, row_id, join_text=''):
        """
        Returns the row that needs work.

        :param row_id: The squeue ID that was returned from the get function.
        :return: Returns the corresponding dataframe row.
        """
        # Will return the row as a dataframe without the squeue specific values
        sql_update = """UPDATE {0:s} SET {1:s} = now(), 
                                         {2:s} = '{3:s}',
                                         {4:s} = '{5:s}'  
                        WHERE {6:s} = {7:d}""".format(self.squeue,
                                                      self.SQ_GET_TIME,
                                                      self.SQ_STATUS,
                                                      self.STATUS_PROGRESS,
                                                      self.SQ_GET_HOSTNAME,
                                                      socket.gethostname(),
                                                      self.SQ_ID,
                                                      row_id)
        self.sql_conn.execute_sql(sql_update)
        sql_select = """SELECT * FROM {0:s} {3:s} WHERE {1:s} = {2:d}""".format(self.squeue, self.SQ_ID, row_id, join_text)
        return self.sql_conn.get_dataframe(sql_select)

    def finish(self, row_id, finish_status=STATUS_COMPLETED):
        """
        Sets the status to complete to let the queue know the work has been completed.

        :param row_id: The squeue ID that was returned from the get function
        :param finish_status: Whether the row from the queue finished without exception or not.
        """
        # Sets the row in the queue to completed so the queue manager knows that we are finished.
        sql_update = """UPDATE {0:s} SET {1:s} = now(),
                                         {2:s} = '{3:s}'
                        WHERE {4:s} = {5:d}""".format(self.squeue,
                                                      self.SQ_FINISH_TIME,
                                                      self.SQ_STATUS,
                                                      finish_status,
                                                      self.SQ_ID,
                                                      row_id)
        self.sql_conn.execute_sql(sql_update)

    def get_status(self, row_id):
        """
        :param row_id: The squeue ID that was returned from the get function
        :return: Returns the status corresponding to the row ID.
        """
        status_df = self.sql_conn.get_dataframe('SELECT {0:s} FROM {1:s} WHERE {2:s} = {3:d}'.format(self.SQ_STATUS,
                                                                                                     self.squeue,
                                                                                                     self.SQ_ID,
                                                                                                     row_id))
        if len(status_df) > 0:
            return status_df.loc[0, self.SQ_STATUS]
        return self.STATUS_NOEXIST

    def get_hostname_status(self, sq_id_list, conditional_claim=None, join_text=''):
        """
        :param sq_id_list: A list of the squeue IDs for which we want to find the status
        :type sq_id_list: list
        :param join_text: If the pricing_queue needs to be joined to another table for some data points
        :type join_text: text
        :return: Returns a dataframe with the columns, sq_id and sq_status
        :rtype: pd.DataFrame
        """
        if not conditional_claim:
            conditional_claim = ''
        else:
            conditional_claim = 'WHERE ' + conditional_claim

        if len(sq_id_list) > 0:
            get_hostname_text = "'" + "', '".join([f'vm-d4-vpwrkr-{str(int(sq_id))}' for sq_id in sq_id_list]) + "'"
            get_sq_id_text = ','.join([str(sq_id) for sq_id in sq_id_list])

            status_df = self.sql_conn.get_dataframe(f"""
                    SELECT start_id as sq_id, 
                           {self.SQ_STATUS}
                    FROM {self.squeue}
                    JOIN (SELECT SPLIT_PART({self.SQ_GET_HOSTNAME}, '-', 4)::INT AS start_id, 
                                 MAX({self.SQ_ID}) AS max_sq_id
                          FROM {self.squeue}
                          WHERE {self.SQ_GET_HOSTNAME} IN ({get_hostname_text})
                          GROUP BY {self.SQ_GET_HOSTNAME}
                          UNION
                          SELECT {self.SQ_ID} AS start_id,
                                 {self.SQ_ID} AS max_sq_id
                          FROM {self.squeue}
                          WHERE {self.SQ_ID} IN ({get_sq_id_text})
                          AND {self.SQ_STATUS} = '{self.STATUS_DESTROYED}') max_ids
                        ON {self.squeue}.sq_id = max_ids.max_sq_id
                    WHERE '{self.STATUS_AVAILABLE}' NOT IN (SELECT DISTINCT({self.SQ_STATUS}) 
                                              FROM {self.squeue}
                                              {join_text}
                                              JOIN run_id rid ON rid.run_id = pricing_queue.run_id
                                              {conditional_claim}
                                              AND date(rid.post_time) >= date(now() - interval '1 day'))""")
            return status_df
        else:
            return pandas.DataFrame(columns=['sq_id', 'sq_status'])

    def get_work_in_progress(self):
        """
        :return: Returns a dataframe containing all of the work that is currently in progress.
        """
        return self.sql_conn.get_dataframe("""SELECT * FROM {0:s} WHERE {1:s} = '{2:s}'""".format(self.squeue,
                                                                                                  self.SQ_STATUS,
                                                                                                  self.STATUS_PROGRESS))

    def cleanup_long_running_rows(self, in_progress_timeout_h=8, claimed_timeout_h=1, join_text=''):
        with self.sql_conn.get_engine().connect() as connection:
            timedout_sql = """SELECT * FROM {0:s} 
                              {8:s}
                              WHERE (({1:s} = '{2:s}'
                                     AND {3:s} IS NOT NULL
                                     AND EXTRACT(EPOCH FROM now() - {3:s})/3600 > {4:f}) 
                                    OR ({1:s} = '{5:s}'
                                     AND {6:s} IS NOT NULL
                                     AND EXTRACT(EPOCH FROM now() - {6:s})/3600 > {7:f}))
                              """.format(self.squeue,
                                         SQLQueue.SQ_STATUS,
                                         SQLQueue.STATUS_PROGRESS,
                                         SQLQueue.SQ_GET_TIME,
                                         float(in_progress_timeout_h),
                                         SQLQueue.STATUS_CLAIMED,
                                         SQLQueue.SQ_CLAIM_TIME,
                                         float(claimed_timeout_h),
                                         join_text)
            in_progress_df = pandas.read_sql(timedout_sql, connection)
            if len(in_progress_df) > 0:
                id_list = ', '.join([str(x) for x in list(in_progress_df[SQLQueue.SQ_ID].unique())])
                connection.execute("""UPDATE {0:s} SET {1:s} = '{2:s}' 
                                        WHERE {3:s} IN ({4:s})""".format(self.squeue,
                                                                         SQLQueue.SQ_STATUS,
                                                                         SQLQueue.STATUS_DESTROYED,
                                                                         SQLQueue.SQ_ID,
                                                                         id_list))
        return in_progress_df

    def set_destroyed(self, status_list):
        """
        Everything that is currently in progress gets set to killed status. We then copy the row but put the status
        to available and insert back into the queue with new squeue ids.

        :return A list of the sq_ids that were set to destroyed
        """
        in_progress_df = pandas.DataFrame()
        with self.sql_conn.get_engine().connect() as connection:
            connection.execute('BEGIN WORK;'
                               'LOCK TABLE {0:s} IN ACCESS EXCLUSIVE MODE;'.format(self.squeue))
            in_progress_df = pandas.read_sql("""SELECT * FROM {0:s}
                                                  WHERE {1:s} in ('{2:s}')""".format(self.squeue,
                                                                                     self.SQ_STATUS,
                                                                                     "', '".join(status_list)),
                                             connection)
            connection.execute("""UPDATE {0:s} SET {1:s} = '{3:s}' 
                                    WHERE {1:s} in ('{2:s}')""".format(self.squeue,
                                                                       SQLQueue.SQ_STATUS,
                                                                       "', '".join(status_list),
                                                                       SQLQueue.STATUS_DESTROYED))
            connection.execute('COMMIT WORK;')
        if not in_progress_df.empty:
            return list(in_progress_df[SQLQueue.SQ_ID])
        return []

    def available_count(self, conditional_claim=None, join_text=''):
        if not conditional_claim:
            conditional_claim = ''
        else:
            conditional_claim = 'and ' + conditional_claim
        unclaimed_count_df = self.sql_conn.get_dataframe("""SELECT COUNT(*) count FROM {0:s}
                                                              {4:s}
                                                              WHERE {1:s} = '{2:s}' 
                                                              {3:s}""".format(self.squeue,
                                                                              self.SQ_STATUS,
                                                                              self.STATUS_AVAILABLE,
                                                                              conditional_claim,
                                                                              join_text))
        if len(unclaimed_count_df) > 0:
            return unclaimed_count_df.loc[0, 'count']
        else:
            return 0

    @staticmethod
    def create_table(squeue_name, column_descriptions, sql_conn):
        """
        Uses the column descriptions and the sql connection to create a corresponding queue that also has the necessary
        meta columns for the queue organization.

        :param squeue_name: The name of the SQL Queue that will be visible in the database
        :param column_descriptions: Should be a dictionary where the names are the column names and the values are the
         sql supported column types
        :param sql_conn: A SQLConn connection object to the database where the table should be created.
        """
        if sql_conn.sql_params.type == SQLConn.POSTGRES:
            sql = """create table if not exists {0:s} (""".format(squeue_name)
            column_descriptions[SQLQueue.SQ_STATUS] = 'varchar(20)'
            column_descriptions[SQLQueue.SQ_PRIORITY] = 'int'
            column_descriptions[SQLQueue.SQ_PUT_TIME] = 'timestamp default now()'
            column_descriptions[SQLQueue.SQ_CLAIM_TIME] = 'timestamp'
            column_descriptions[SQLQueue.SQ_GET_TIME] = 'timestamp'
            column_descriptions[SQLQueue.SQ_FINISH_TIME] = 'timestamp'
            column_descriptions[SQLQueue.SQ_PUT_HOSTNAME] = 'text'
            column_descriptions[SQLQueue.SQ_GET_HOSTNAME] = 'text'
            column_descriptions[SQLQueue.SQ_ID] = 'serial primary key'

            columns_list = [x + ' ' + column_descriptions[x] for x in column_descriptions]
            columns_string = ', '.join(columns_list)
            sql = sql + columns_string + ')'
            sql_conn.execute_sql(sql)
        else:
            raise TypeError('We do not support {0:s} type for creating SQL tables'.format(sql_conn.sql_params.type))
