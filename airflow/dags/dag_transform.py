import sys
sys.path.append('.') # add path to package
from pathlib import Path
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, to_date, col,  max
from dag_IaC import iac
from src.transform import Transform
from dag_extract import extract
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator,  BranchPythonOperator
from airflow.operators.external_task_operator import ExternalTaskSensor 


def get_last_date(ti, iac) : 
    
    """ this function is used to get the last date from the tweets table in redshift"""
    
    try : 
        # connection a red shift pour recuperer la date la plus recente 
        conn = iac._conn
        cur = conn.cursor()
        # grab the latest date from the tweets creation
        sql = "SELECT date_creation FROM ghiles.tweets ORDER BY date_creation DESC LIMIT 1;"

        cur.execute(sql)
        # convert the date from datetime to string
        date_fetched = cur.fetchall()
        last_date = date_fetched[0][0].strftime("%Y-%m-%d")

    except:
        
        # if the date is empty, we set the date to yesterday 
        last_date = datetime.date.today() - datetime.timedelta(days=1)
    

    finally : 
        
        cur.close()
        conn.close() 
        
    # push the task instance (key, value format) to an xcom
    ti.xcom_push(key='last_date', value=last_date) 
    
def read_api_data_compare_date(ti, spark) : 
    
    """ this function is used to compare the last date from the tweets table in redshift with the last date from the tweets table in s3 bukcet for tweet api """
    
    
    tweet_df = spark.read.csv( f"{Path(__file__).parent.parent }/src/data/raw_data" + '/TWEET_INFO.csv', header=True,inferSchema=True)
    tweet_df = tweet_df.select("date_creation") 
    tweet_df = tweet_df.withColumn("date_creation", to_date(to_timestamp(col("date_creation"), "yyyy-MM-dd HH:mm:ss+00:00")))
    max_date = tweet_df.select(max("date_creation")).collect()[0][0]

    last_date = ti.xcom_pull(key='last_date', task_ids=['get_last_date_redshift'])
    
    if max_date > last_date : 
        return 'download_data_from_s3'

    else : 
        return 'end_etl'


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


dag = DAG("transform_twitter_etl",
          
          default_args=default_args,
          description='transform twitter data and send it to s3',
          schedule_interval='@daily',
          max_active_runs = 1
        )


start = DummyOperator(task_id='Begin_execution',  dag=dag)

wait_for_extract = ExternalTaskSensor(
    task_id='wait_for_extract',
    external_dag_id='extract_twitter_etl',
    external_task_id='Stop_execution',
    dag=dag,
)

get_last_date_processed = PythonOperator(
    task_id='get_last_date_redshift',
    op_kwargs={'iac': iac}, 
    python_callable=get_last_date
)

# initialize spark session 
spark = SparkSession.builder.appName("data-ghiles").getOrCreate() 

# initialize the transform class
transform = Transform(spark)

compare_date =  BranchPythonOperator(
    task_id='compare last date of tweets in redshift and the new date of tweets in s3 comming from api',
    op_kwargs={'iac': spark}, 
    python_callable=get_last_date
)


download_data = PythonOperator(task_id='download_data_from_s3',
                               python_callable = transform.download_data ,
                               op_kwargs={'iac': iac}, 
                               dag=dag 
)


transform_tweet_info = PythonOperator(task_id='transform tweet info file', 
                                      pythonCallable =transform.transform_tweet_info,
                                      dag = dag
)

transform_user_info = PythonOperator(task_id='transform user info file', 
                                    python_callable = transform.transform_user_info,
                                    op_kwargs={'user': extract}, 
                                    dag = dag 
) 

                                   
send_to_s3 = PythonOperator(task_id='send data processed to s3', 
                            python_callable = transform.send_to_s3,	
                            op_kwargs={'iac': iac},
                            dag = dag 
)

delete_local_data = PythonOperator(task_id='delete local data',
                                   python_callable = transform.delete_local_data,
                                   dag = dag 
)

end = DummyOperator(task_id='end_etl',  dag=dag)                       

start >> wait_for_extract >> get_last_date_processed >> compare_date >> [download_data, end] 
download_data >> transform_tweet_info >> transform_user_info >> send_to_s3 >> delete_local_data >> end   




