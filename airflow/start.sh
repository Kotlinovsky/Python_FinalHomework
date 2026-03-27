#!/bin/bash

# Установим провайдер для работы PostgreSQL.
pip install apache-airflow-providers-postgres

# Создаем пользователя Airflow.
airflow db migrate
airflow users create \
  --username python_user \
  --password python_password \
  --firstname python_user \
  --lastname python_user \
  --role Admin \
  --email python@example.local || true

# Создаём соединение к PostgreSQL
airflow connections add postgres_default \
  --conn-type postgres \
  --conn-host postgres \
  --conn-schema python_database \
  --conn-login python_user \
  --conn-password python_password \
  --conn-port 5432 || true

# Запускаем сам Airflow (планировщик и веб-сервер).
airflow triggerer &
airflow scheduler &
airflow dag-processor &
airflow api-server
