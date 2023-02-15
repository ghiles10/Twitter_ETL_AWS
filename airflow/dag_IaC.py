import sys
sys.path.append('.') # add path to package 
from src.IaC import IaC 
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator



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


dag = DAG("IaC_twitter_etl",
          
          default_args=default_args,
          description='IaC for aws infrastructure and etl twitter pipeline',
          schedule_interval='@daily',
          max_active_runs = 1
        )

iac = IaC()

start = DummyOperator(task_id='Begin_execution',  dag=dag)

create_bucket = PythonOperator( task_id='create_bucket',
                                python_callable=iac.create_bucket,
                                dag=dag 
)

create_cluster = PythonOperator( task_id='create_cluster',
                                python_callable=iac.create_cluster,
                                dag=dag 
) 

open_port = PythonOperator( task_id='open_port',
                           python_callable=iac.open_port, 
                           dag = dag 
) 

verify_cluster_status = PythonOperator( task_id='verify_cluster_status',
                           python_callable=iac.verify_cluster_status, 
                           dag = dag 
)                       

end = DummyOperator(task_id='Stop_execution',  dag=dag) 

start >> create_bucket >> create_cluster >> open_port >> verify_cluster_status >> end