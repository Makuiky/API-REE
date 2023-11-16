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
        fechaini = str(ultfecha[0])
        return fechaini
    else:
        fechaini=str(datetime.now().date()-timedelta(days=365))
        return fechaini
    
def insert_sql_dic(tabla,data,cursor,conn):
    queryin = "INSERT IGNORE INTO {} ({}) VALUES ({})".format(
                             ''.join(tabla),
                            ', '.join(data.keys()),
                            ', '.join(['%s'] * len(data))
                    )
                   
    cursor.execute(queryin,tuple(data.values()))
    conn.commit()



def estructura_generacion(fechaini=None,fechafin=None,cortetiempo='day',limitgeo='peninsular'):
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

    if fechaini:
        pass
    else:
        fechaini=fecha_ini(tabla='datos_generacion')
    
    if fechafin:
        pass
    else:
        fechafin=str(datetime.now().date()-timedelta(days=1))

    fechainiformat=f'{fechaini}T00:00'
    fechafinformat=f'{fechafin}T23:59'
    query ={
    'start_date' : fechainiformat,
    'end_date' : fechafinformat,
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
        print(f'Terminado generación: {fecha}')
    
    cursor.close()
    conn.close()

def precio_energia_medio_dia(fecha='2022-11-09',cortetiempo='hour',limitgeo='peninsular', category='mercados',widget='precios-mercados-tiempo-real',dropjson=False):

    conn, cursor = open_conn()
    widget_mod=widget.replace('-','_')
    tabla= f'{category}_{widget_mod}'
    crear_tabla = f"""CREATE TABLE IF NOT EXISTS {tabla}(
    fecha_reg DATE PRIMARY KEY,
    precio_spot_dia FLOAT,
    precio_PVPC_dia FLOAT
    )
    """

    

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
            cursor.execute(crear_tabla)
            conn.commit()

            for precios in jsonresponse['included']:
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
    print(f'terminado precio: {fecha}')

def precio_energia_periodo(fechaini=None,fechafin=None):
    if fechaini:
        pass
    else:
        fechaini=fecha_ini(tabla='mercados_precios_mercados_tiempo_real')
    
    if fechafin:
        pass
    else:
        fechafin=str(datetime.now().date()-timedelta(days=1))

    fechaini=parse(fechaini).date()
    fechafin=parse(fechafin).date()
    
    diasperiodo= (fechafin-fechaini).days+1
    
    for dia in range(diasperiodo):
        precio_energia_medio_dia(fechaini+timedelta(days=dia))
        #print(fechaini+timedelta(days=dia))
        


#Ejecuciones

#estructura_generacion()

#precio_energia_periodo()

precio_energia_medio_dia(category='demanda',widget='demanda-tiempo-real',dropjson=True,cortetiempo='day')


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