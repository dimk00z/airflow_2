# airflow_2

## Вторая домашняя работа

Скрипт [data_collector_dag.py](https://github.com/dimk00z/airflow_2/blob/master/dags/data_collector_dag.py) содержит четыре таска:

1. `load_csv_op` загружает данные по заказам

2. `load_transactions_operations_op` загружает данные по транзакциям из json

3. `load_from_postgres_op` подгружает данные о пользователях и товарах

4. `save_data_op` собирает финальный датасет и загружает его в базу

Промежуточные данные храняться в data в виде csv файлов.
