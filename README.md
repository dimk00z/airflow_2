# Airflow 101

## Challenge 2

Скрипт [data_collector_dag.py](https://github.com/dimk00z/airflow_2/blob/master/dags/data_collector_dag.py) содержит четыре таска:

1. `load_csv_op` загружает данные по заказам

2. `load_transactions_operations_op` загружает данные по транзакциям из json

3. `load_from_postgres_op` подгружает данные о пользователях и товарах

4. `save_data_op` собирает финальный датасет и загружает его в базу

Промежуточные данные храняться в data в виде csv файлов.

### Как установить

Для корректной работы скрипта должны быть установлены зависимости:

```
pip install -r requirements.txt
```

### Цель проекта

Код написан в образовательных целях на онлайн-курсе [Airflow 101](https://airflow101.python-jitsu.club/).
