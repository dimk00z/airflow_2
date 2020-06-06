import csv
import requests
import datetime
import os
import logging
from pathlib import Path

from psycopg2.extras import DictCursor

from collections import OrderedDict

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


DIR_FOR_CSV_FILES = Path.joinpath(Path.cwd(), 'data')
TRANSACTIONS_FILE_NAME = Path.joinpath(DIR_FOR_CSV_FILES, 'transactions.csv')
ORDERS_FILE_NAME = Path.joinpath(DIR_FOR_CSV_FILES, 'orders.csv')
GOODS_FILE_NAME = Path.joinpath(DIR_FOR_CSV_FILES, 'goods.csv')
CUSTOMERS_FILE_NAME = Path.joinpath(DIR_FOR_CSV_FILES, 'customers.csv')


default_args = {
    'owner': 'dimk',
    'start_date': datetime.datetime(2020, 6, 1),
}

dag = DAG(dag_id='data_collector',
          schedule_interval='0 * * * * *',
          default_args=default_args)


def write_csv(table_headers, table_data, file_name):
    with open(file_name, 'w+',  newline="", encoding='utf-8') as file:
        columns = table_headers
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(OrderedDict((frozenset(row.items()), row)
                                     for row in table_data).values())


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
    write_csv(['transaction_uuid', 'transaction_status'],
              seen_transactions, TRANSACTIONS_FILE_NAME)


load_transactions_operations_op = PythonOperator(
    task_id='load_transactions_operations_op',
    python_callable=load_transactions_operations,
    dag=dag,
)


def get_table_data(table):
    result_table = []
    headers = list(table[0].keys())
    for raw in table:
        cell = {}
        for field in headers:
            if raw[field] is None or raw[field] == '':
                break
            cell[field] = raw[field]
        if len(cell) == len(headers):
            result_table.append(cell)
    return headers, result_table


def load_postgres_data(table_name, file_name):
    request = f'SELECT * FROM {table_name}'
    with PostgresHook(
            postgres_conn_id='postgres_goods_customers',
            schema='postgres').get_conn() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(request)
            headers, result_table = get_table_data(cursor.fetchall())
            write_csv(headers, result_table, file_name)


def load_goods():
    load_postgres_data('goods', GOODS_FILE_NAME)


load_goods_op = PythonOperator(
    task_id='load_goods_op',
    python_callable=load_goods,
    dag=dag,
)


def load_customers():
    load_postgres_data('customers', CUSTOMERS_FILE_NAME)


load_customers_op = PythonOperator(
    task_id='load_customers_op',
    python_callable=load_customers,
    dag=dag,
)


load_csv_op >> load_transactions_operations_op >> load_goods_op >> load_customers_op
