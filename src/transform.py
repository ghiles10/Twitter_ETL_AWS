import configparser
from pathlib import Path
import log_config
from pyspark.sql.functions import regexp_replace, substring, lower, to_date, current_timestamp, lit
import src.extract 
from pyspark.sql import SparkSession
import os 
from src.IaC import IaC
import shutil

# creating folder for storign data
if not os.path.exists("src/data"):
    
    os.makedirs("src/data/raw_data")
    os.makedirs("src/data/processed_data")

# récupération des paramètres de configuration dans le fichier config.cfg
config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parent}/config.cfg"))

# logging
logger = log_config.logger 

class Transform : 
    
    def __init__(self, spark):
        
        self._spark = spark
        self._load_path = "s3://ghiles-data-foot/raw_data"
        self._save_path = "s3://ghiles-data-foot/processed_data"
    
    
    def download_data(self,iac) :   
        """ this function is used to download the data from s3"""
        
        # copie des fichiers s3 
        my_bucket = iac._s3.Bucket("ghiles-data-foot")
        
        for o in my_bucket.objects.filter(Prefix='raw_data'):
            file_name = os.path.join("src/data/raw_data", o.key.split("/")[-1])
            my_bucket.download_file(o.key, file_name)
    
    
    def transform_tweet_info(self) : 
        
        """  transformation du fichier TWEET_INFO.csv  """
        
        # chargement du fichier TWEET_INFO.csv et suppression des caractères spéciaux
        tweet_df = self._spark.read \
            .option("header", "true") \
            .option("delimiter", "|") \
            .option("inferSchema", "true") \
            .csv(f"{Path(__file__).parent}/data/raw_data/TWEET_INFO.csv")

        tweet_df = tweet_df.withColumn("text", regexp_replace("text", "[^a-zA-Z\\s]", "")) \
               .withColumn("text", regexp_replace("text", "^@\\w+", "")) 
        
        # Conversion en minuscules
        tweet_df = tweet_df.withColumn("text", lower(tweet_df["text"])) 
        
        # Suppression des URL
        tweet_df = tweet_df.withColumn("text", regexp_replace("text", "http\\S+", "")) 
        
        # Séparation de la colonne date et heure en deux colonnes
        tweet_df = tweet_df.withColumn("date", substring(tweet_df["date_creation"], 0, 10)) 
            # .withColumn("time", substring(tweet_df["date_creation"], 11, 19))
        
        # Transformation de la colonne "date" en format de date
        tweet_df = tweet_df.withColumn("date", to_date(tweet_df["date"], "yyyy-MM-dd"))

        # Transformation de la colonne "time" en format d'heure
        # tweet_df = tweet_df.withColumn("time", to_timestamp(tweet_df["time"], "HH:mm:ss"))
        
        logger.debug("Transformation du fichier TWEET_INFO.csv effectuée")
        
        # Enregistrement du DataFrame csv
        tweet_df.toPandas().to_csv(f"{Path(__file__).parent}/data/processed_data/tweet.csv", index=False, sep='|')
        # tweet_df.coalesce(1).write.csv( f"{Path(__file__).parent}/data/processed_data/tweet", sep = ',', header=True, mode ="overwrite") 
        
        logger.debug("Enregistrement du DataFrame tweet au format csv effectué") 

            
    def transform_user_info(self, user) : 
        
        """ transformation du fichier USER_INFO.csv """
        
        
        # chargement du fichier USER_INFO.csv et suppression des caractères spéciaux
        user_df = self._spark.read.csv( f"{Path(__file__).parent}/data/raw_data" + '/USER_INFO.csv', header=True, inferSchema=True, sep="|") 
        user_activity = self._spark.read.csv( "src/data/raw_data"  + '/USER_ACTIVITY.csv', header=True, inferSchema=True, sep = "|")   
           
        # transformation date
        user_df = user_df.withColumn("date_creation", substring(user_df["date_creation"], 0, 10))
        # user_df = user_df.withColumn("date_creation", to_date(user_df["date_creation"], "yyyy-MM-dd"))
        
        # ajouter la date du jour afin de faciliter le suivi des utilisateurs
        user_df = user_df.withColumn("date", to_date(current_timestamp())) 
                
        # join des deux dataframes 
        
        df_pandas = user_activity.toPandas()
        retweet_count = int(df_pandas["retweet_count"].values[0])
        favorite_count = int(df_pandas["favorite_count"].values[0])
        
        user_df = user_df.withColumn("retweet_count", lit(retweet_count))
        user_df = user_df.withColumn("favorite_count", lit(favorite_count))
        
        # ajout colonne user 
        user_df = user_df.withColumn("user", lit(user.user_name))
        
        logger.debug("Transformation du fichier USER_INFO.csv effectuée") 
        
        #enregistrement du dataframe au format csv
        user_df.toPandas().to_csv(f"{Path(__file__).parent}/data/processed_data/user.csv", index=False, sep='|')
        # user_df.coalesce(1).write.csv( f"{Path(__file__).parent}/data/processed_data/user", sep = ',', header=True, mode ="overwrite" )
        
        logger.debug("Enregistrement du DataFrame au format user csv  effectué")
        
        
    def send_to_s3(self, iac) : 
        
        """ this function is used to send the processed data to s3 """
        
        csv_files = []
        for _, _, files in os.walk(f"{Path(__file__).parent}/data/processed_data"):
            for file in files:
                if file.endswith(".csv"):
                    csv_files.append(file)
                          
        # upload the file to S3
        for file in csv_files: 
            iac._s3.Bucket( iac._bucket_name ).put_object(Key="processed_data/" + file, Body=open(f"{Path(__file__).parent}/data/processed_data/" + file, 'rb'))
            
            logger.debug(f"upload {file} file to s3 for user infos")


    def delete_data(self) : 
        
        """ this function is used to delete the data local """
        
        shutil.rmtree(f"{Path(__file__).parent}/data")
        
if __name__ == "__main__" : 
    
    # pass
    spark = SparkSession.builder.appName("data-ghiles").getOrCreate()

    # instanciation de la classe Transform
    transform = Transform(spark)
    
    # telechargement des fichiers 
    iac = IaC()
    transform.download_data(iac)
    
    # instanciation de la classe Extract
    extract = extract.Extract()
    
    # transformation des données 
    transform.transform_tweet_info()
    transform.transform_user_info(extract) 

    # envoi des données au format csv vers s3
    transform.send_to_s3(iac)