import csv
import requests
import datetime
import os
import logging

from collections import OrderedDict

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

TRANSACTIONS_FILE_NAME = 'transactions.csv'
ORDERS_FILE_NAME = 'orders.csv'

default_args = {
    'owner': 'dimk',
    'start_date': datetime.datetime(2020, 6, 1),
}

dag = DAG(dag_id='data_collector',
          schedule_interval='0 * * * * *',
          default_args=default_args)


def load_orders_csv():
    url = 'https://airflow101.python-jitsu.club/orders.csv'
    temp_file_name = 'temp.csv'
    response = requests.get(url)
    response.raise_for_status()

    with open(temp_file_name, 'wb') as file:
        file.write(response.content)

    with open(temp_file_name, 'r') as in_file, open(ORDERS_FILE_NAME, 'w') as out_file:
        seen = set()
        for line in in_file:
            if line in seen:
                continue
            seen.add(line)
            out_file.write(line)

    os.remove(temp_file_name)


load_csv_op = PythonOperator(
    task_id='load_csv_op',
    python_callable=load_orders_csv,
    dag=dag,
)


def load_transactions_operations():

    LOGGER = logging.getLogger("airflow.task")
    LOGGER.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1")
    print('oppop')

    url = 'https://api.jsonbin.io/b/5ed7391379382f568bd22822'
    response = requests.get(url)
    response.raise_for_status()
    transaction_json = response.json()
    seen_transactions = []
    for transaction, transaction_data in transaction_json.items():
        if transaction and transaction_data:
            status = 'Successful operation' if transaction_data[
                'success'] else f'Error: {". ".join(transaction_data["errors"])}'

            seen_transactions.append({
                'transaction_uuid': transaction,
                'transaction_status': status
            })
    with open(TRANSACTIONS_FILE_NAME, 'w+',  newline="", encoding='utf-8') as file:
        columns = ['transaction_uuid', 'transaction_status']
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(OrderedDict((frozenset(transaction.items()), transaction)
                                     for transaction in seen_transactions).values())


load_transactions_operations_op = PythonOperator(
    task_id='load_transactions_operations_op',
    python_callable=load_transactions_operations,
    dag=dag,
)


load_csv_op >> load_transactions_operations_op
