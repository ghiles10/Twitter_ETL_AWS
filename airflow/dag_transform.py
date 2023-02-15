import sys
sys.path.append('.') # add path to package
import datetime 
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, to_date, col,  max


from src import IaC 
from src.warehouse.upsert_data_warehouse import iac



def get_last_date(ti) : 
    
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
    
    

def read_api_data_compare_date(ti) : 
    
    """ this function is used to compare the last date from the tweets table in redshift with the last date from the tweets table in s3 bukcet for tweet api """
    
    spark = SparkSession.builder.appName("data-ghiles").getOrCreate() 
    tweet_df = spark.read.csv( f"{Path(__file__).parent.parent }/src/data/raw_data" + '/TWEET_INFO.csv', header=True,inferSchema=True)
    tweet_df = tweet_df.select("date_creation") 
    tweet_df = tweet_df.withColumn("date_creation", to_date(to_timestamp(col("date_creation"), "yyyy-MM-dd HH:mm:ss+00:00")))
    max_date = tweet_df.select(max("date_creation")).collect()[0][0]

    last_date = ti.xcom_pull(key='last_date', task_ids=['Get_last_date'])
    
    if max_date > last_date : 
        return 'transform_load_data'

    else : 
        return 'end_etl'

read_api_data(ti)