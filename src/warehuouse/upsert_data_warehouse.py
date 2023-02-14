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
        
    def insert_data(self, iac): 
        
        """ Insert data into data warehouse"""
        
        # connect to data warehouse
        conn = iac._conn
        cur = conn.cursor()
        logger.debug("Connection to data warehouse")
        
        # Copier les données
        cur.execute(f"""
        COPY ghiles.tweets (text, favorite_count, date_creation, retweet_count,  date)
        FROM 's3://{iac._bucket_name}/processed_data/tweet.csv' 
        IAM_ROLE '{iac._iam_role}'
        CSV
        DELIMITER ','
        IGNOREHEADER 1
        ;
        """)
        
        logger.debug("Data tweets inserted") 
        
        cur.execute(f"""
        COPY ghiles.users ("user", description, following, followers, favorite_count, retweet_count, date_creation, date)
        FROM 's3://{iac._bucket_name}/processed_data/user.csv' 
        IAM_ROLE '{iac._iam_role}'
        CSV
        DELIMITER ','
        IGNOREHEADER 1
        ;
        """)       
         
        logger.debug("Data users inserted") 
        
        logger.debug("verif data inserted")

        # Exécuter la requête SELECT pour vérifier si les données ont été insérées
        cur.execute("SELECT * FROM ghiles.users LIMIT 10;")

        # Afficher les résultats
        rows = cur.fetchall()
        if len(rows) > 0:
            logger.debug("Data inserted in user ")
            
        cur.execute("SELECT * FROM ghiles.tweets LIMIT 10;")
        rows = cur.fetchall()
        if len(rows) > 0:
            logger.debug("Data inserted in tweet ")
        for row in rows: 
            print(row)

iac = IaC()
iac.create_bucket()
iac.create_cluster()
iac.open_port()
iac.verify_cluster_status()
  
upsert = upsert_data_warehouse()  
upsert.connect_to_data_warehouse(iac) 
upsert.insert_data(iac)

        