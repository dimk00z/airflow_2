python3.7 -m venv env

export AIRFLOW_HOME=~/Python/airflow_2
source env/bin/activate

airflow test dag op 2020-06-4

postgres
sudo apt install libpq-dev python3.7-dev
pip install psycopg2
