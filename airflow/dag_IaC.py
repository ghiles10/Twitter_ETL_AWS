import sys
sys.path.append('.') # add path to package 
from datetime import datetime, timedelta
from pytz import utc
from airflow import DAG


default_args = {
    'owner': 'tweet_api_etl',
    'depends_on_past': True,
    'start_date' :datetime.now(tz=utc) ,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
    'catchup': True
}


dag = DAG("tweeter_etl",
          
          default_args=default_args,
          description='IaC for aws infrastructure and etl twitter pipeline',
          #schedule_interval=None,
          schedule_interval='*/10 * * * *',
          max_active_runs = 1
        )