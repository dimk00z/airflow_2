import csv
import requests
import datetime
import os
import logging
from dateutil import relativedelta
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
FINAL_DATASET_FILE_NAME = Path.joinpath(DIR_FOR_CSV_FILES, 'final_dataset.csv')


default_args = {
    'owner': 'dimk',
    'start_date': datetime.datetime(2020, 6, 1),
}

dag = DAG(dag_id='data_collector',
          schedule_interval='0 12 * * * *',
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


def load_postgres_table(table_name, file_name):
    request = f'SELECT * FROM {table_name}'
    with PostgresHook(
            postgres_conn_id='postgres_goods_customers',
            schema='postgres').get_conn() as connection:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(request)
            headers, result_table = get_table_data(cursor.fetchall())
            write_csv(headers, result_table, file_name)


def load_from_postgres():
    load_postgres_table('goods', GOODS_FILE_NAME)
    load_postgres_table('customers', CUSTOMERS_FILE_NAME)


def csv_dict_reader(file_name, key_field):
    result_table = {}
    with open(file_name) as file_obj:
        reader = csv.DictReader(file_obj, delimiter=',')
        for line in reader:
            result_table[line[key_field]] = line
    return result_table


def create_final_dataset():

    transactions = csv_dict_reader(TRANSACTIONS_FILE_NAME, 'transaction_uuid')
    goods = csv_dict_reader(GOODS_FILE_NAME, 'name')
    orders = csv_dict_reader(ORDERS_FILE_NAME, 'uuid заказа')
    customers = csv_dict_reader(CUSTOMERS_FILE_NAME, 'email')

    today = datetime.datetime.today()
    headers = ['uuid', 'name', 'age', 'good_title', 'date',
               'payment_status', 'total_price', 'amount', 'last_modified_at']
    result_data_set = []
    for uuid_order, order in orders.items():
        row = {}
        customer_email = order['email']
        row['uuid'] = uuid_order
        if customer_email in customers:
            customer_birth_date = customers[customer_email]['birth_date']
            row['age'] = relativedelta.relativedelta(
                today, datetime.datetime.strptime(customer_birth_date, '%Y-%m-%d')).years
            row['name'] = customers[customer_email]['name']
        else:
            row['age'] = None
            row['name'] = order['ФИО']
        #row['last_modified_at'] = int(datetime.datetime.now().timestamp())
        row['last_modified_at'] = datetime.datetime.now()

        row['good_title'] = order['название товара']
        row['date'] = order['дата заказа']
        row['amount'] = int(order['количество'])
        row['total_price'] = round(
            row['amount'] * float(goods[order['название товара']]['price']), 2)
        row['payment_status'] = transactions[uuid_order]['transaction_status']
        result_data_set.append(row)
    write_csv(headers, result_data_set, FINAL_DATASET_FILE_NAME)


def save_data():
    create_final_dataset()
    table_name = 'home_work_2_data_set'
    check_request = f"""
            SELECT *
            FROM information_schema.tables
            WHERE table_name='{table_name}'"""
    create_request = f"""
                CREATE TABLE public.{table_name} (
                    uuid UUID PRIMARY KEY,
                    name varchar(255),
                    age int,
                    good_title varchar(255),
                    date timestamp,
                    payment_status text,
                    total_price numeric(10,2),
                    amount int,
                    last_modified_at timestamp)"""
    #drop_table_request = f'DROP TABLE public.{table_name}'
    with PostgresHook(
            postgres_conn_id='postgres_final_data_set',
            schema='dimk_smith').get_conn() as connection:
        connection.autocommit = True
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            # cursor.execute(drop_table_request)
            check_table = cursor.execute(check_request)
            if not bool(cursor.rowcount):
                cursor.execute(create_request)
            with open(FINAL_DATASET_FILE_NAME, "r") as f:
                reader = csv.reader(f)
                next(reader)
                columns = (
                    'uuid', 'name', 'age', 'good_title', 'date',
                    'payment_status', 'total_price', 'amount', 'last_modified_at',
                )
                cursor.copy_from(
                    f, table_name, columns=columns, sep=",", null='')


load_csv_op = PythonOperator(
    task_id='load_csv_op',
    provide_context=True,
    python_callable=load_orders_csv,
    dag=dag,
)

load_transactions_operations_op = PythonOperator(
    task_id='load_transactions_operations_op',
    provide_context=True,
    python_callable=load_transactions_operations,
    dag=dag,
)

load_from_postgres_op = PythonOperator(
    task_id='load_from_postgres_op',
    provide_context=True,
    python_callable=load_from_postgres,
    dag=dag,
)

save_data_op = PythonOperator(
    task_id='save_data_op',
    provide_context=True,
    python_callable=save_data,
    dag=dag,
)

load_csv_op >> load_transactions_operations_op >> load_from_postgres_op >> save_data_op
