# Extraccion-API-series-tiempo

Tecnologías utilizadas: Python, Docker y Apache Airflow

Para la temática del presente trabajo se creó un proceso ETL desde cero, configurando un pipeline que extrae datos de la API Series de Tiempo. La información extraída incluye valores como la cotización del dólar, Indices de Precios al Consumidor en distintos rubros, así como la variación porcentual del Indice de Precios al consumidor a nivel general.

Esto se realizó a través de un DAG escrito en Python, el cual fue dockerizado y ejecutado a través de Apache Airflow. El mismo cuenta con distintas tasks las cuales se encargan de extraer y cargar los datos en tablas alojadas en un Database SQL, y además cuenta con una task final que realiza una búsqueda de valores anómalos de cotizaciones de dólar en la tabla ya cargada, generando una alerta si la misma supera un monto determinado.

