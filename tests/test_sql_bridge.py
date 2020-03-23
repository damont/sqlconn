from sqlconn import SQLConn
from sqlconn.postgresbridge import PostgresBridge
from sqlconn.mssqlbridge import MsSQLBridge
from sqlconn.snowflakebridge import SnowflakeBridge
from sqlconn.sqllitebridge import SQLLiteBridge
import pandas as pd


init_df = pd.DataFrame({
    'test': [1,2,3,4,5,6,7,8,9,10],
    'load': ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'],
    'bulk_c': [True, False, False, True, True, False, False, True, True, False]
})


def _test_bridge(load_df, sql_conn, sql_bridge, test_table_name, **kwargs):

    sql_conn.append_to_table(table_name=test_table_name, data_to_append=load_df)
    columns = sql_bridge.get_columns(table_name=test_table_name.split('.')[-1],
                                     schema_name=test_table_name.split('.')[-2])
    assert len(columns) == 3, f'Was looking for 3 columns, but returned {len(columns)}'
    columns = [x.lower() for x in columns]  # We do this because snowflake returns capital columns
    for col in load_df.columns:
        assert col in columns, f'{col} was not in the list of columns'

    columns = sql_bridge.get_columns(table_name='NOT_HERE',
                                     schema_name=sql_bridge.default_schema())
    assert len(columns) == 0, 'Should return 0 columns'

    table_name, schema_name = sql_conn.get_names(table=test_table_name)

    # Make sure we correctly create a table using bulk load
    sql_conn.execute_sql(f'DROP TABLE IF EXISTS {schema_name}.{table_name}')
    sql_bridge.bulk_load(bulk_df=load_df,
                         table_name=table_name,
                         schema_name=schema_name,
                         table_exists=False,
                         **kwargs)
    assert sql_conn.get_dataframe(f'SELECT count(1) t_count FROM {schema_name}.{table_name}').loc[0, 't_count'] == len(load_df)

    # Make sure we correctly create a table using append to table
    sql_conn.execute_sql(f'DROP TABLE IF EXISTS {schema_name}.{table_name}')
    sql_conn.append_to_table(table_name=test_table_name,
                             data_to_append=load_df,
                             if_exists='append',
                             bulk_copy=SQLConn.BULK_FORCE,
                             **kwargs)
    assert sql_conn.get_dataframe(f'SELECT count(1) t_count FROM {schema_name}.{table_name}').loc[0, 't_count'] == len(load_df)

    # Make sure we correctly replace a table using append to table
    sql_conn.append_to_table(table_name=test_table_name,
                             data_to_append=load_df,
                             if_exists='replace',
                             bulk_copy=SQLConn.BULK_FORCE,
                             **kwargs)
    assert sql_conn.get_dataframe(f'SELECT count(1) t_count FROM {schema_name}.{table_name}').loc[0, 't_count'] == len(load_df)

    # Make sure we correctly append to a table using append to table (don't want to recreate, just add to what is there)
    sql_conn.append_to_table(table_name=test_table_name,
                             data_to_append=load_df,
                             if_exists='append',
                             bulk_copy=SQLConn.BULK_FORCE,
                             **kwargs)
    assert sql_conn.get_dataframe(f'SELECT count(1) t_count FROM {schema_name}.{table_name}').loc[0, 't_count'] == len(load_df) * 2

    sql_bridge.bulk_load(bulk_df=load_df,
                         table_name=table_name,
                         schema_name=schema_name,
                         table_exists=True,
                         if_exists='append',
                         **kwargs)
    assert sql_conn.get_dataframe(f'SELECT count(1) t_count FROM {schema_name}.{table_name}').loc[0, 't_count'] == len(load_df) * 3


def test_postgres_db():
    _test_bridge(load_df=init_df.copy(deep=True),
                 sql_conn=SQLConn.get_connection('devpg'),
                 sql_bridge=PostgresBridge(SQLConn.get_connection('devpg')),
                 test_table_name='tmp.test_load_bulk')


def test_sqlserver_db():
    sql_conn = SQLConn.get_connection('devvmart')
    try:
        sql_conn.execute_sql(f'CREATE SCHEMA test;')
    except:
        pass
    sqlserver_df = init_df.copy(deep=True)
    sqlserver_df['bulk_c'] = sqlserver_df['bulk_c'].astype(int)
    _test_bridge(load_df=sqlserver_df,
                 sql_conn=sql_conn,
                 sql_bridge=MsSQLBridge(SQLConn.get_connection('devvmart')),
                 test_table_name='test.test_load_bulk',
                 tmp_dir='/tmp')


def test_snowflake_db():
    sql_conn = SQLConn.get_connection('devvmartsnow')
    sql_conn.execute_sql(f'CREATE SCHEMA IF NOT EXISTS test;')
    _test_bridge(load_df=init_df.copy(deep=True),
                 sql_conn=sql_conn,
                 sql_bridge=SnowflakeBridge(SQLConn.get_connection('devvmartsnow')),
                 test_table_name='test.test_load_bulk',
                 tmp_dir='/tmp')
