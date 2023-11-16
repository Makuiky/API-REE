import requests, json
import mysql.connector
from dateutil.parser import parse
from datetime import datetime, timedelta
#electric-data-server
#admin
def open_conn():
    config = {
    'user': 'root',
    'password': 'admin',
    'host': 'localhost',
    'database' : 'Datos_electricos_esp',
}

    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    return conn,cursor

def fecha_ini(tabla=None):
    conn, cursor = open_conn()
    consfecha = f'SELECT MAX(fecha_reg) AS ult_fecha FROM {tabla}'
    try:
        cursor.execute(consfecha)
        ultfecha=cursor.fetchone()
    except mysql.connector.Error as err:
        ultfecha=[]
        ultfecha.append(None)
    cursor.close()
    conn.close()
    if ultfecha[0]:
        fechaini = str(ultfecha[0])+'T00:00'
        return fechaini
    else:
        fechaini=str(datetime.now().date()-timedelta(days=365))+'T00:00'
        return fechaini
    
def insert_sql_dic(tabla,data,cursor,conn):
    queryin = "INSERT IGNORE INTO {} ({}) VALUES ({})".format(
                             ''.join(tabla),
                            ', '.join(data.keys()),
                            ', '.join(['%s'] * len(data))
                    )
                   
    cursor.execute(queryin,tuple(data.values()))
    conn.commit()



def estructura_generacion(fechaini,fechafin,cortetiempo='day',limitgeo='peninsular'):
    conn, cursor = open_conn()
    crear_tabla = """
    CREATE TABLE IF NOT EXISTS datos_generacion (
    Fuente_energia VARCHAR(255),
    Tipo_energia VARCHAR(255),
    generacion FLOAT,
    porcentaje FLOAT,
    fecha_reg DATE,
    PRIMARY KEY reg_por_fuente (Fuente_energia, fecha_reg)
    )
    """
    cursor.execute(crear_tabla)
    conn.commit()
    url = 'https://apidatos.ree.es/es/datos/generacion/estructura-generacion'

    query ={
    'start_date' : fechaini,
    'end_date' : fechafin,
    'time_trunc' : cortetiempo,
    'geo_limit' : limitgeo,
    }

    response = requests.get(url=url, params=query)

    if response.status_code== requests.codes.ok: 
        jsonresponse = response.json()
        
        for energia in jsonresponse['included']:
            if energia['type']=='Generación total':
                
                pass
            else:
                datatosql = dict()
                for dato in energia['attributes']['values']:
                    fecha=parse(dato['datetime']).date()
                    
            
                    datatosql = {
                        'Fuente_energia' : energia['attributes']['title'],
                        'Tipo_energia' : energia['attributes']['type'],
                        'generacion' : dato['value'],
                        'porcentaje' : dato['percentage'],
                        'fecha_reg' : fecha

                    }
                    insert_sql_dic('datos_generacion',datatosql,cursor,conn)
    
    cursor.close()
    conn.close()

def precio_energia(fecha='2023-11-14',cortetiempo='hour',limitgeo='peninsular', category='mercados',widget='precios-mercados-tiempo-real',dropjson=False):

    conn, cursor = open_conn()
    widget_mod=widget.replace('-','_')
    tabla= f'{category}_{widget_mod}'
    crear_tabla = f"""CREATE TABLE IF NOT EXISTS {tabla}(
    fecha_reg DATE PRIMARY KEY,
    precio_spot_dia FLOAT,
    precio_PVPC_dia FLOAT
    )
    """

    cursor.execute(crear_tabla)
    conn.commit()

    #Formateo de fecha
    fechaini = f'{fecha}T00:00'
    fechafin = f'{fecha}T23:59'
    url = f'https://apidatos.ree.es/es/datos/{category}/{widget}'

    query ={
    'start_date' : fechaini,
    'end_date' : fechafin,
    'time_trunc' : cortetiempo,
    'geo_limit' : limitgeo,
    }
    response = requests.get(url=url, params=query)

    if response.status_code== requests.codes.ok: 
        jsonresponse = response.json()

        if dropjson:

            with open(f'{category}_{widget}.json', 'w') as archivo_json:
                json.dump(jsonresponse, archivo_json)
        else:
            for precios in jsonresponse['included']:
                print()
                preciototal=0.
                for precio in precios['attributes']['values']:
                    preciototal+=precio['value']
                    
                if precios['type'] == 'PVPC (\u20ac/MWh)':
                    preciomedioPVPC = preciototal/len(precios['attributes']['values'])
                else:
                    preciomedioSPOT = preciototal/len(precios['attributes']['values'])
            
            fecha=parse(fechaini).date()
            datatosql = {'fecha_reg' : fecha,
                         'precio_spot_dia' : preciomedioSPOT,
                         'precio_PVPC_dia' : preciomedioPVPC,

                }
            
            insert_sql_dic(tabla,datatosql,cursor,conn)
    else:
        print(response.json())
    conn.close()
    cursor.close()







                    


def balance_electrico():
    url = 'https://apidatos.ree.es/es/datos/balance/balance-electrico'

    query ={
    'start_date' : '2023-10-26T00:00',
    'end_date' : '2023-11-02T00:00',
    'time_trunc' : 'day',
    'geo_limit' : 'peninsular',
    }

    response = requests.get(url=url, params=query)

    if response.status_code== requests.codes.ok:
        """
    with open('datos.json', 'w') as archivo_json:
        json.dump(response.json(), archivo_json)
    """
        jsonresponse = response.json()
        print(jsonresponse['data']['attributes']['title'])
        print(jsonresponse['data']['attributes']['description'])

        for fuentes in jsonresponse['included']:
            for energias in fuentes['attributes']['content']:
                print(energias['groupId'])
                print(energias['attributes']['title'])
                print(energias['attributes']['values'][0]['value'])

    else:
        response.raise_for_status()

    conn.close()

#Ejecuciones
inicio = fecha_ini(tabla='datos_generacion')
ayer=str(datetime.now().date()-timedelta(days=1))+'T23:59'
#estructura_generacion(fechaini=inicio,fechafin=ayer)
precio_energia()

if __name__ == '__main__':
    config = {
    'user': 'root',
    'password': 'admin',
    'host': 'localhost',
}
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    nombre_db = 'Datos_electricos_esp'
    query_db = f'CREATE DATABASE IF NOT EXISTS {nombre_db}'
    cursor.execute(query_db)
    conn.commit()
    cursor.close()
    conn.close()