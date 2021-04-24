from airflow.utils.dates import days_ago
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Airflow',
    'start_date': days_ago(0, hour=1, minute=0),
}

dag = DAG(
    dag_id='example_variables',
    default_args=args,
    schedule_interval="0 * * * *",
    tags=['example'],
    catchup=False
)

def print_variable():
    print("================================================")
    print("Variable Contoh adalah:", Variable.get("Contoh"))
    print("================================================")

cetak_variable = PythonOperator(
    task_id='cetak_variable',
    python_callable=print_variable,
    dag=dag,
)

cetak_variable
