# **Extracción automática de variables del BCRA**
## Principales variables del BCRA. (01/01/2020 a la actualidad)

Este proyecto resuelve la necesidad de acceder de forma automatizada y estructurada a datos públicos del BCRA, que suelen presentarse en formatos poco prácticos para análisis.
Me interesó desarrollarlo como parte de un proyecto de ingeniería de datos, aplicando ETL de datos reales del sistema financiero argentino.
El pipeline extrae variables monetarias históricas (como inflación, tipo de cambio, tasas de interés, entre otras), que pueden utilizarse para análisis económico, reportes financieros o visualización de tendencias en herramientas como Power BI.
Además, fue programado con el Programador de Tareas de Windows para ejecutarse durante la semana a las 10:00 AM, asegurando la actualización automática de los datos.

## Tecnologías utilizadas
- Python
- MySQL
- Power Bi

## Flujo del Pipeline
El pipeline automatiza la extracción de datos desde la API del BCRA utilizando Python, los procesa y almacena en una base de datos MySQL . Finalmente, estos datos son utilizados por Power BI para su visualización y análisis.

## Proceso de ETL en el pipeline del BCRA
Extracción:

Se realiza una solicitud a la API pública del BCRA para obtener datos históricos de las variables de interés.

La extracción es dinámica y automatizada usando Python y la librería requests.

Transformación :

Se limpian y normalizan los datos extraídos, incluyendo:

Conversión de fechas al formato adecuado.

Homogeneización de nombres de variables.

Control de duplicados y registros vacíos.

Se prepara el modelo estrella con tablas de dimensión (dim_variable, dim_tiempo) y hechos (hechos_monetarios).

Carga:

Los datos transformados se cargan en una base de datos MySQL local.

La carga se realiza de forma incremental, insertando solo los nuevos datos no presentes en la base. El script incluye lógica para detectar la última fecha disponible por variable y continuar desde allí.

## Logging y trazabilidad:

Se implementó un sistema de logging con la librería logging de Python.

Los registros de ejecución se guardan en archivos .log con detalles de fechas, errores y pasos realizados. Esto permite identificar problemas, asegurar la trazabilidad y mantener control del historial de ejecuciones.

## Automatización del pipeline:

El script Python fue diseñado para ejecutarse automáticamente usando el Programador de tareas de Windows. Esto permite que el pipeline corra diariamente, garantizando datos siempre actualizados.

## Visualización con Power BI:

Los datos cargados en MySQL se conectan a Power BI para crear dashboards interactivos.

Se muestra información como:

Últimos valores por variable.

Variaciones mensuales.

Tendencias en el tiempo.

Esto transforma datos crudos en información útil para el análisis económico y financiero.
