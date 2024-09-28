FROM apache/airflow:2.10.1-python3.9

ADD requirements.txt .

RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

# Set the Airflow user
USER airflow