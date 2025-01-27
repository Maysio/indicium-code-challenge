pip install apache-airflow
airflow db init
airflow users create \
    --username admin \
    --firstname Maysio \
    --lastname Saboia \
    --role Admin \
    --email maysio.saboia@hotmail.com
airflow webserver --port 8080
airflow scheduler
