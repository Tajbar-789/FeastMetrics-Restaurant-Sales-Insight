from sql_session import connect
from config import load_config
def create_db():
    conn=connect()
    cursor=conn.cursor()
    config=load_config()
    sql_statement="""SELECT ‘CREATE DATABASE {0}‘ WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = ‘{0}‘)""".format(config['database'])
    cursor.execute(sql_statement)
    cursor.close()
    conn.close()