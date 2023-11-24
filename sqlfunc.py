from .gvar import *
import mysql.connector

def open_conn():
    config = {
    'user': USER,
    'password': PASS,
    'host': HOST,
    'database' : DATABASE,
}
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        return conn,cursor
    except:
        try:
            crear_database()
            conn = mysql.connector.connect(**config)
            cursor = conn.cursor()
            return conn,cursor
        except:
            print('Error de conexión a base de datos')
            raise SystemExit
        
def crear_database():
    config = {
    'user': USER,
    'password': PASS,
    'host': HOST,
}
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    query_db = f'CREATE DATABASE IF NOT EXISTS {DATABASE}'
    cursor.execute(query_db)
    conn.commit()
    cursor.close()
    conn.close()

def insert_sql_dic(tabla,data,cursor,conn):
    queryin = "INSERT IGNORE INTO {} ({}) VALUES ({})".format(
                             ''.join(tabla),
                            ', '.join(data.keys()),
                            ', '.join(['%s'] * len(data))
                    )
                   
    cursor.execute(queryin,tuple(data.values()))
    conn.commit()