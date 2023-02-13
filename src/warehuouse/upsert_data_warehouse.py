import sys
sys.path.append('.') # add path to package
from log_config import logger
from src.IaC import IaC 
from data_warehouse_query import create_tweet_table, create_user_table



class upsert_data_warehouse():
    
    
    """ Class to upsert data warehouse"""
    
    def connect_to_data_warehouse(self, iac):
        
        """ Connect to data warehouse and create tables"""
        
        # connect to data warehouse
        conn = iac._conn
        cur = conn.cursor()
        logger.debug("Connection to data warehouse")
        
        # create schema
        cur.execute("CREATE SCHEMA IF NOT EXISTS ghiles")
        logger.debug("Schema ghiles created")
        
        # create  tables
        cur.execute(create_tweet_table) 
        logger.debug("Table tweet created") 
        
        cur.execute(create_user_table)
        logger.debug("Table user created") 
        
        conn.commit()
         

iac = IaC()
iac.create_bucket()
iac.create_cluster()
iac.open_port()
iac.verify_cluster_status()
  
upsert = upsert_data_warehouse()  
upsert.connect_to_data_warehouse(iac) 

        