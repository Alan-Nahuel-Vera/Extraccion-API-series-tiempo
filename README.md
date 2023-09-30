# Extraccion-API-series-tiempo

Tecnologías utilizadas: Python, Docker y Apache Airflow

Para la temática del presente trabajo se creó un proceso ETL desde cero, configurando un pipeline que extrae datos de la API gubernamental "Series de Tiempo". La información extraída incluye valores como la cotización del dólar, Indices de Precios al Consumidor en distintos rubros, así como la variación porcentual del Indice de Precios al consumidor a nivel general.

Esto se realizó a través de un DAG escrito en Python, el cual fue dockerizado y ejecutado a través de Apache Airflow. El DAG cuenta con distintas tasks, las cuales se encargan de extraer y cargar los datos en una tabla alojada en un Database SQL, y además cuenta con una task final que realiza una búsqueda de valores anómalos de cotizaciones de dólar en la tabla ya cargada, generando una alerta si la misma supera un monto determinado, indicando en qué mes ocurrió dicho valor anómalo. El DAG está configurado para correr automáticamente una vez por mes, lo que simplificaría la tarea de extracción y carga de datos mediante dicha automatización.

En un contexto de incertidumbre y vaivenes económicos constantes, este proceso aportaría valor a análisis de datos relacionados con las variables analizadas, sumado a la simplicidad que ofrecería el hecho de que dichos datos sean extraídos y actualizados automáticamente.
