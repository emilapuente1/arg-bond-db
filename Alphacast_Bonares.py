
import sqlite3
import pandas as pd  
import datetime as dt
import AlphacastKey as key
from alphacast import Alphacast


##Paso 1 Conectar a la DB y traer el ultimo registro
##Eso nos permitira saber que fechas faltan en nuestra DB
#Path de la base SQLite (cambiar segun corresponda)
db_path = 'H:/Documentos/GitHub/Bonos-Arg/'
db_file = "Bonosusd.db"

#Me conecto a la base, la conexion queda dentro de la variable
#La variable luego la uso para interactuar con la base, por ejemplo escribir ahi
conn = sqlite3.connect(db_path + db_file)
ultima_fecha=None


query = 'SELECT Date FROM Bonares ORDER BY Date DESC LIMIT 1'


#try "intenta" conectarse a la base de datos
#si hay un error imprimira un error, sino ejecutara lo de adentro de try
#de una manera u otra, luego la base se cerrara en finally
try:
    
 
    last_date_df = pd.read_sql(query, conn)

    #La DB puede estar vacia (es nueva or ejemplo)
    #de no ser asi, queremos extraer la fecha como un texto y guardarla en una variable
    #no necesitamos el dataframe entero con su estructura de filas y columnas para 
    #un solo dato
    if not last_date_df.empty:
        ultima_fecha=last_date_df.iloc[0]['Date']
        print("La ultima fecha en 'Bonares' es: ", ultima_fecha)
    else:
        #Si no pudo leer la ultima fecha, la tabla esta vacia
        #la crearemos mas abajo
        print("No hay datos, intentando bajar todo el historial")

except Exception as e:
        print("La tabla no existe todavia,la crearemos")

finally:
    conn.close()


#Paso 2 : Importar datos de Alphacast

#obviamente modificar esto con TU apikey (esta en el profile)
alphacast = Alphacast(key.Alphacast)


#Utilizamos el dataset de Bonos soberanos 
dataset = alphacast.datasets.dataset(5359)


# Vamos a necesitar convertir la fecha de string a datetime
# Conversion
# Explicar ISO format 
# "2023-12-03"
# "2023-12-03T10:15:30"
# ejemplos de crear fechas como referencia
# hoy=dt.date.today()
# ayer=hoy-dt.timedelta(days=1)

#Si la tabla esta vacia (es la primera vez que ejecutamos esto por ejemplo)
#esta variable va a valer None (ver arriba)
#Con lo cual intentar un casting podria darnos un error, y lo evitamos
#solo convirtiendo si la variable tiene asignado un valor (la ultima fecha en la tabla)
if ultima_fecha!=None:
    ultima_fecha = dt.datetime.fromisoformat(ultima_fecha)


#Bajar los datos de Alphacast
# e importar a pandas
# pero solo desde la ultima fecha que esta en la DB
# o sea la ultima fecha de actualizacion
# y hasta el ultimo dato disponible al momento de la consulta
df = dataset.download_data(format = "pandas", 
                           startDate=ultima_fecha,
                           endDate=None,
                           filterVariables = [], 
                           filterEntities = {})

#Si el dataframe viene vacio desde Alphacast
#Puede ser que no haya actualizaciones disponibles
#Por ejemplo si lo ejecutamos consecutivamente con el mercado cerrado
if df.empty:
    print ("DB al dia, no hay datos actualizados")
else: 
    # Paso 3 : Exportar a DB
    # Solo caemos aca si realmente hay datos actualizados para grabar en la DB

    #Conectar a DB
    #Agregar el path segun corresponda
    conn = sqlite3.connect('Bonosusd.db')

    df.to_sql('Bonares', conn, if_exists='append', index=False)

    #cerrar conexion y terminar
    conn.close()



