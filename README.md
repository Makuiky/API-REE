# API para consumo de datos de REE

Funciones para descargar datos suministrados por REE y almacenarlos en una base de datos Mysql.

El proyecto se puede emplear tanto con el main incluido en el mismo como mediante un main de nivel superior, por si se quieren agrupar ejecuciones de varias APIs en un mismo script, manteniendo todos los proyectos independientes.

## Creación de gvar.py
Es muy importante para que funcione este proyecto crear el documento gvar.py que contrendá variables de conexión a bbdd y datos de la api key con la siguiente estructura:

USER = 'usuario server'
PASS = 'password server'
HOST = 'direccion server'
DATABASE = 'Datos_electricos_esp' #se podría cambiar ya que la bbdd se crea en la primera ejecución

Este archivo no se han incluido para evitar filtrar estos datos en futuras actualizaciones del proyecto.

## Creación del servidor
Se puede emplear cualquier servidor que se disponga o levantar uno para la API. En mi caso he utilizado la herramienta docker para levantar el servidor necesario.

## funciones creadas en el proyecto
### fecha_ini()
Esta función sirve para gestionar la lógica de fechas para las descargas de la API. Lo que comprueba es la fecha de último registro dentro de la bbdd y si no encuentra nada toma las fechas de un año antes. De esta forma en ejecuciones consecutivas solo descargaremos datos nuevos.

### Esctructura_generacion()
Toma datos diarios sobre las distintas fuentes de energía y la cantidad generada por cada una de ellas. Además se especifica si la fuente de energía es renovable o no renovable.

### datos_energia_periodo()

Toma datos intradía tanto de precio como de demanda y los promedia para disponer de un dato medio diario. 