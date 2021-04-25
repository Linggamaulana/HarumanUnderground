from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

from pprint import pprint
import pandas as pd

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='example_email_operators',
    default_args=args,
    schedule_interval=None,
    tags=['example']
)

def print_context(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

def webscrap():
    #scraping table from url
    url = 'https://id.wikipedia.org/wiki/Daftar_orang_terkaya_di_Indonesia'
    dfs = pd.read_html(url)

    #call specified table
    df = dfs[7]

    #export to csv file
    df.to_csv('yourcsv_file.csv', index=False)
webscrap()


cetak_context = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)

send_email = EmailOperator(
        task_id='send_email',
        to='your@email.com',
        subject='tes kirim dengan attaachment02',
        html_content=""" <h3>saya mengumpulkan tugas untuk python airflow</h3> """,
        #add attachment
        files=['yourcsv_file.csv'],
        dag=dag
)

cetak_context >> send_email
