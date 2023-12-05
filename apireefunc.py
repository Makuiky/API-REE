import requests, json
from dateutil.parser import parse
from datetime import datetime, timedelta
try:
    from .sqlfunc import *
except:
    from sqlfunc import *

#electric-data-server
#admin


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
def datos_energia_periodo(fechaini=None,fechafin=None):
    if fechaini:
        pass
    else:
        fechaini1=fecha_ini(tabla='mercados_precios_mercados_tiempo_real')
        fechaini2=fecha_ini(tabla='demanda_demanda_tiempo_real')
    
    if fechafin:
        pass
    else:
        fechafin=str(datetime.now().date()-timedelta(days=1))

    fechaini1=parse(fechaini1).date()
    fechaini2=parse(fechaini2).date()
    fechafin=parse(fechafin).date()
    
    diasperiodo1= (fechafin-fechaini1).days+1
    diasperiodo2= (fechafin-fechaini2).days+1
    
    for dia in range(diasperiodo1):
        dato_media_dia(fechaini1+timedelta(days=dia),category='mercados',widget='precios-mercados-tiempo-real')
    
    for dia in range(diasperiodo2):
        dato_media_dia(fechaini2+timedelta(days=dia))
def dato_media_dia(fecha='2022-11-09',cortetiempo='hour',limitgeo='peninsular', category='demanda',widget='demanda-tiempo-real',dropjson=False):
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
            conn, cursor = open_conn()
            widget_mod=widget.replace('-','_')

            tabla= f'{category}_{widget_mod}'
            crear_tabla = '''CREATE TABLE IF NOT EXISTS {}(
            fecha_reg DATE PRIMARY KEY,
            {} FLOAT
             )'''.format(
                 ''.join(tabla),
                 ' FLOAT, '.join([tipo['type'].replace(' ','_').replace('(€/MWh)','EMWH') for tipo in jsonresponse['included']])
                 )
            cursor.execute(crear_tabla)
            conn.commit()

            diccarga=dict()

            for registros in jsonresponse['included']:
                valortotal=0.
                for valor in registros['attributes']['values']:
                    valortotal+=valor['value']

                diccarga.update({registros['type'].replace(' ','_').replace('(€/MWh)','EMWH') : valortotal/len(registros['attributes']['values'])})
                
            fecha=parse(fechaini).date()
            diccarga['fecha_reg']= fecha
                
            
            insert_sql_dic(tabla,diccarga,cursor,conn)
            print(f'terminado recogida dato {category} para fecha: {fecha}')

            conn.close()
            cursor.close()





           
#Ejecuciones


