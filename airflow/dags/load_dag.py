import sys
sys.path.append('.') # add path to package
from datetime import datetime, timedelta
from dag_IaC import iac
from src.warehouse.upsert_data_warehouse import UpsertDataWarehouse
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.external_task_operator import ExternalTaskSensor 


default_args = {
    'owner': 'tweet_api_etl',
    'depends_on_past': True,
    'start_date' :datetime.date.today() ,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
    'catchup': True
}


dag = DAG("load_twitter_etl",
          
          default_args=default_args,
          description='load twitter data to redshift',
          schedule_interval='@daily',
          max_active_runs = 1
        )

start = DummyOperator(task_id='Begin_execution',  dag=dag)

wait_for_transform = ExternalTaskSensor(
    task_id='wait for transform IaC',
    external_dag_id='transform_twitter_etl',
    external_task_id='end_etl',
    dag=dag,
)

load = UpsertDataWarehouse()
connect_to_data_warehouse = PythonOperator(task_id='connect to data warehouse', 
                                           python_callable = load.connect_to_data_warehouse,
                                           op_kwargs={'iac': iac},
                                           dag = dag 
) 

insert_data = PythonOperator(task_id='insert data to data warehouse',
                             python_callable = load.insert_data,
                             op_kwargs={'iac': iac},
                             dag = dag 
) 

clean_s3_bucket = PythonOperator(task_id='clean s3 bucket',
                                python_callable = iac.clean_bucket,
                                dag = dag
)

end = DummyOperator(task_id='end_etl',  dag=dag)                       

start >> wait_for_transform >> connect_to_data_warehouse >> insert_data >> clean_s3_bucket >> end