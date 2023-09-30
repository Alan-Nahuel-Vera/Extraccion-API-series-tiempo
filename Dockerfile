FROM apache/airflow:2.7.0
USER root
RUN apt-get update
USER airflow
ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/"
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

RUN airflow db init && \
airflow users create --username admin --password admin \
--firstname Anonymous --lastname Admin \
--role Admin --email admin@example.org

ENTRYPOINT airflow scheduler & airflow webserver
