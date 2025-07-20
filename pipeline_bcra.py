# %%
#Importar librerias
import requests #Para conectarse  a la API
import pandas as pd #Para manipulación de datos con Pandas
from datetime import datetime #Para trabajar con fechas
import time #Para trabajar con fechas
import unicodedata #Para corregir errores dentro del dataframe
import mysql.connector #Para conectarme con mysql
from sqlalchemy import create_engine #Para conectarme con mysql
import os
import logging #Para crear logs

# %% --- Configurar logging ---
log_dir = "c:/Users/galim/Desktop/Bootcamp Ingenieria de Datos/Proyecto Final/logs"
os.makedirs(log_dir, exist_ok=True)

#Nombre único para cada ejecución 
log_filename = os.path.join(log_dir, f"pipeline_bcra_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")

# Remover handlers anteriores
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Configurar logging
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)

logging.info("🚀 Inicio de ejecución del pipeline BCRA")


# %%
try:
    url = 'https://api.bcra.gob.ar/estadisticas/v3.0/monetarias'
    response = requests.get(url, verify=False)

    if response.status_code == 200:
        data = response.json()
        variables_info = data['results']
    
    # Creamos un diccionario: id -> descripcion
        descripciones = {v['idVariable']: v['descripcion'] for v in variables_info}
    else:
        logging.warning("⚠️ No se pudo obtener la lista de variables")
        descripciones = {}
except Exception as e:
    logging.error("❌ Error al obtener la lista de variables", exc_info=True)
    descripciones = {}
# %%
    ids = [1, 4, 5, 14, 15, 16, 27, 28,32,35]
    fecha_desde = '2020-01-01'
    fecha_hasta = datetime.now().strftime('%Y-%m-%d')

    todas_las_series = []

# %%
max_retries = 3
retry_delay = 1  

for id_variable in ids:
    url = f'https://api.bcra.gob.ar/estadisticas/v3.0/monetarias/{id_variable}?desde={fecha_desde}&hasta={fecha_hasta}'
    attempt = 0
    success = False

    while attempt < max_retries and not success:
        try:
            response = requests.get(url, verify=False, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if 'results' in data and data['results']:
                    df_variable = pd.json_normalize(data['results'])
                    df_variable['fecha'] = pd.to_datetime(df_variable['fecha'])
                    df_variable['descripcion'] = descripciones.get(id_variable, 'Desconocido')
                    todas_las_series.append(df_variable)
                    logging.info(f"✅ Datos extraídos para variable ID {id_variable} (intento {attempt + 1})")
                    success = True
                else:
                    logging.error(f"❌ No hay datos para la variable ID {id_variable} (intento {attempt + 1})")
                    # No tiene sentido reintentar si no hay datos
                    break
            else:
                logging.error(f"❌ Error en la solicitud para variable ID {id_variable}: {response.status_code} (intento {attempt + 1})")
        except requests.exceptions.Timeout:
            logging.error(f"❌ Timeout en la solicitud para variable ID {id_variable} (intento {attempt + 1})")
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Error en la solicitud para variable ID {id_variable}: {e} (intento {attempt + 1})")

        attempt += 1
        if not success and attempt < max_retries:
            time.sleep(retry_delay)  # Espera antes de reintentar

    if not success:
        logging.error(f"❌ No se pudo descargar la variable ID {id_variable} tras {max_retries} intentos.")

# %%
if todas_las_series:
    # Concatenar todos los DataFrames en uno solo
    df_final = pd.concat(todas_las_series, ignore_index=True)
    
    # Ordenar por ID de variable y fecha
    df_final = df_final.sort_values(by=['idVariable', 'fecha'])
    logging.info("🧱 Consolidación de datos completada")
# %%
# Renombrar columnas
    df_final = df_final.rename(columns={
    'idVariable': 'id',
    'descripcion': 'variable'
})

# %%
# Reordenar columnas
    df_final = df_final[['id', 'variable', 'fecha', 'valor']]

# %%
def quitar_acentos(texto):
    # Normaliza y elimina caracteres especiales
    texto_normalizado = unicodedata.normalize('NFKD', texto)
    texto_sin_acentos = texto_normalizado.encode('ASCII', 'ignore').decode('utf-8')
    return texto_sin_acentos

# Aplicar a toda la columna
df_final['variable'] = df_final['variable'].apply(quitar_acentos)



# %%
df_final.to_csv('c:/Users/galim/Desktop/Bootcamp Ingenieria de Datos/Proyecto Final/datos_bcra_monetarias2.csv',index=False)
logging.info("💾 CSV exportado correctamente")


# %%
#-----------------------------Crear base de datos BCRA en mysql----------------------------------

# %%
# Parámetros de conexión
usuario = "root"
contraseña = "12345"
host = "localhost"
puerto = 3306
base_de_datos = "bcra2"

# %%
# Crear la base de datos BCRA en MySQL


# Conectar a MySQL
cnx = mysql.connector.connect(
    host=host, 
    port=puerto, 
    user=usuario, 
    password=contraseña
)

# Crear cursor
cur = cnx.cursor()

# Crear la base de datos si no existe
cur.execute("CREATE DATABASE IF NOT EXISTS BCRA2")

# Confirmar cambios
cnx.commit()

# Cerrar cursor y conexión inicial
cur.close()
cnx.close()
logging.info("✅ Base de datos creada o ya existente")
# %%
#-----------------------------------------------Crear tablas-------------------------------

# %%
# Conexión a la base de datos
conexion = mysql.connector.connect(
    host= host,
    user= usuario,
    password=contraseña,
    database=base_de_datos
)
cursor = conexion.cursor()


# Crear tabla de dimensión de variable
cursor.execute("""
CREATE TABLE IF NOT EXISTS dim_variable (
    id INT PRIMARY KEY,
    variable VARCHAR(255)
)
""")

# Crear tabla de dimensión de tiempo
cursor.execute("""
CREATE TABLE IF NOT EXISTS dim_tiempo (
    fecha DATE PRIMARY KEY,
    año INT,
    mes INT,
    dia INT
)
""")

# Crear tabla de hechos
cursor.execute("""
CREATE TABLE IF NOT EXISTS hechos_monetarios (
    id INT,
    fecha DATE,
    valor FLOAT,
    PRIMARY KEY (id, fecha),
    FOREIGN KEY (id) REFERENCES dim_variable(id),
    FOREIGN KEY (fecha) REFERENCES dim_tiempo(fecha)
)
""")

conexion.commit()
cursor.close()
conexion.close()
logging.info("✅ Tablas creadas correctamente")

# %%
# Conexion con SQLAlchemy
engine = create_engine(f"mysql+mysqlconnector://{usuario}:{contraseña}@{host}:{puerto}/{base_de_datos}")

# %%
#--------------------------------------Carga incremental en cada tabla----------------------------------

# %%
# Cargar datos en las tablas creadas

# --- dim_variable ---
df_dim_variable = df_final[['id', 'variable']].drop_duplicates()
ids_existentes = pd.read_sql("SELECT id FROM dim_variable", con=engine)
df_nuevas_variables = df_dim_variable[~df_dim_variable['id'].isin(ids_existentes['id'])]

if not df_nuevas_variables.empty:
    df_nuevas_variables.to_sql('dim_variable', con=engine, if_exists='append', index=False)
    logging.info(f"✅ {len(df_nuevas_variables)} nuevas variables insertadas en dim_variable")
else:
    logging.info("ℹ️ No hay nuevas variables para insertar en dim_variable")

# %%
# Dimensión tiempo
df_dim_tiempo = df_final[['fecha']].drop_duplicates()
df_dim_tiempo['año'] = df_dim_tiempo['fecha'].dt.year
df_dim_tiempo['mes'] = df_dim_tiempo['fecha'].dt.month
df_dim_tiempo['dia'] = df_dim_tiempo['fecha'].dt.day

fechas_existentes = pd.read_sql("SELECT fecha FROM dim_tiempo", con=engine)
fechas_existentes['fecha'] = pd.to_datetime(fechas_existentes['fecha'])
df_nuevas_fechas = df_dim_tiempo[~df_dim_tiempo['fecha'].isin(fechas_existentes['fecha'])]

if not df_nuevas_fechas.empty:
    df_nuevas_fechas.to_sql('dim_tiempo', con=engine, if_exists='append', index=False)
    logging.info(f"✅ {len(df_nuevas_fechas)} nuevas fechas insertadas en dim_tiempo")
else:
    logging.info("ℹ️ No hay nuevas fechas para insertar en dim_tiempo")

# %%
#--------------------------------------Tabla Hechos Monetarios--------------------------

# %%
# Obtener la última fecha registrada por variable desde la base de datos
query = """
SELECT id, MAX(fecha) as max_fecha
FROM hechos_monetarios
GROUP BY id
"""
# %%
# Leer desde MySQL
df_existente = pd.read_sql(query, con=engine)


# %%
# Asegurar que las columnas de fecha sean datetime
df_final['fecha'] = pd.to_datetime(df_final['fecha'])
df_existente['max_fecha'] = pd.to_datetime(df_existente['max_fecha'], errors='coerce')


# %%
# Eliminar duplicados en df_final por si acaso
df_final = df_final.drop_duplicates(subset=['id', 'fecha'])



# %%
# Unir con df_final para saber qué datos ya existen
df_incremental = pd.merge(
    df_final,
    df_existente,
    on='id',
    how='left'
)

# %%
# Filtrar: mantener solo los datos nuevos (fecha > max_fecha o sin registros previos)
df_incremental = df_incremental[
    (df_incremental['max_fecha'].isna()) | 
    (df_incremental['fecha'] > df_incremental['max_fecha'])
].drop(columns=['max_fecha'])

# %%
# Eliminar duplicados en df_incremental por si acaso
df_incremental = df_incremental.drop_duplicates(subset=['id', 'fecha'])

# %%
# Preparar datos para tabla de hechos
df_hechos = df_incremental[['id', 'fecha', 'valor']]

# %%
df_hechos = df_hechos.drop_duplicates(subset=['id', 'fecha'])

# Insertar solo los nuevos datos
df_hechos.to_sql('hechos_monetarios', con=engine, if_exists='append', index=False)

logging.info(f"📊 {len(df_hechos)} nuevos registros insertados en hechos_monetarios")


logging.info("🏁 Fin de ejecución del pipeline BCRA")
logging.shutdown()





# %%
