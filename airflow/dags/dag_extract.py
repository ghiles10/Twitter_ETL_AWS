 from src.extract import Extract 
from dag_IaC import iac
from datetime import datetime, timedelta
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


dag = DAG("extract_twitter_etl",
          
          default_args=default_args,
          description='extract twitter data and send it to s3',
          schedule_interval='@daily',
          max_active_runs = 1
        )

extract = Extract()


start = DummyOperator(task_id='Begin_execution',  dag=dag)


wait_for_IaC = ExternalTaskSensor(
    task_id='wait_for_IaC',
    external_dag_id='IaC_twitter_etl',
    external_task_id='Stop_execution',
    dag=dag,
)


extract_from_api = PythonOperator( task_id='extract_data from API',
                                python_callable=extract.extract_tweet,
                                dag=dag 
)


tweet_info_to_csv_s3 = PythonOperator( task_id='tweet info to csv in s3',
                                python_callable=extract.tweet_info_to_csv_s3,
                                op_kwargs={'s3': iac}, 
                                dag=dag 
)

user_info_to_csv_s3 = PythonOperator( task_id='user info to csv in s3',
                           python_callable=extract.user_info_to_csv_s3, 
                           op_kwargs={'s3': iac}, 
                           dag = dag 
)                       

user_activity_to_csv_s3 = PythonOperator( task_id='user activity to csv in s3',
                           python_callable=extract.user_activity_to_csv_s3, 
                           op_kwargs={'s3': iac}, 
                           dag = dag 
)                       



end = DummyOperator(task_id='Stop_execution',  dag=dag) 

start >> wait_for_IaC >> extract_from_api >> tweet_info_to_csv_s3 >> user_info_to_csv_s3 >> user_activity_to_csv_s3 >> end
 
 