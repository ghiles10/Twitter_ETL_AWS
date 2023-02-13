import configparser
from pathlib import Path
import log_config
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.functions import regexp_replace, substring, lower, to_date, to_timestamp, current_timestamp
import extract 
from pyspark.sql import SparkSession

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


    def transform_tweet_info(self) : 
        
        """  transformation du fichier TWEET_INFO.csv  """
        
        # chargement du fichier TWEET_INFO.csv et suppression des caractères spéciaux
        tweet_df = self._spark.read.csv( self._load_path + '/TWEET_INFO.csv', header=True,inferSchema=True)
        tweet_df = tweet_df.withColumn("text", regexp_replace("text", "[^a-zA-Z\\s]", "")) \
               .withColumn("text", regexp_replace("text", "^@\\w+", ""))
        
        # conversion en minuscule 
        # Conversion en minuscules
        tweet_df = tweet_df.withColumn("text", lower(tweet_df["text"])) 
    
        # Suppression des stop words
        remover = StopWordsRemover(inputCol="text", outputCol="text")
        tweet_df = remover.transform(tweet_df)
        
        # Suppression des URL
        tweet_df = tweet_df.withColumn("text", regexp_replace("text", "http\\S+", "")) 
        
        # Séparation de la colonne date et heure en deux colonnes
        tweet_df = tweet_df.withColumn("date", substring(tweet_df["date_creation"], 0, 10)) \
            .withColumn("time", substring(tweet_df["date_creation"], 11, 19))
        
        # Transformation de la colonne "date" en format de date
        tweet_df = tweet_df.withColumn("date", to_date(tweet_df["date"], "yyyy-MM-dd"))

        # Transformation de la colonne "time" en format d'heure
        tweet_df = tweet_df.withColumn("time", to_timestamp(tweet_df["time"], "HH:mm:ss"))
        
        logger.debug("Transformation du fichier TWEET_INFO.csv effectuée")
        
        # Enregistrement du DataFrame au format Parquet sur S3 
        tweet_df.write.parquet( self._save_path + "/TWEET_DF.parquet", mode="overwrite", compression="snappy", partitionBy=["date"]) 
        
        logger.debug("Enregistrement du DataFrame au format Parquet sur S3 effectué") 
          
            
    def transform_user_info(self, user) : 
        
        """ transformation du fichier USER_INFO.csv """
        
        # chargement du fichier USER_INFO.csv et suppression des caractères spéciaux
        user_df = self._spark.read.csv( self._load_path + '/TWEET_INFO.csv', header=True, inferSchema=True) 
        user_activity = self._spark.read.csv( self._load_path + '/USER_ACTIVITY.csv', header=True, inferSchema=True)   
           
        # transformation date
        user_df = user_df.withColumn("date_creation", substring(user_df["date_creation"], 0, 10))
        user_df = user_df.withColumn("date_creation", to_date(user_df["date_creation"], "yyyy-MM-dd"))
        
        # ajouter la date du jour afin de faciliter le suivi des utilisateurs
        user_df = user_df.withColumn("date", to_date(current_timestamp())) 
                
        # join des deux dataframes 
        user_df = user_df.withColumn("favorite_count", user_activity["favorite_count"])
        user_df = user_df.withColumn("retweet_count", user_activity["retweet_count"])
        
        # ajout colonne user 
        user_df = user_df.withColumn("user", user._user_name)
        
        logger.debug("Transformation du fichier USER_INFO.csv effectuée") 
        
        #enregistrement du dataframe au format parquet sur S3 
        user_df.write.parquet( self._save_path + "/USER_DF.parquet", mode="overwrite", compression="snappy", partitionBy=["date"])
        
        logger.debug("Enregistrement du DataFrame au format Parquet sur S3 effectué")
        
if __name__ == "__main__" : 
    
    spark = SparkSession.builder \
    .appName("ReadFromS3") \
    .config("spark.hadoop.aws.access.key", config.get('AWS','ACCESS')) \
    .config("spark.hadoop.aws.secret.key", config.get('AWS','KEY')) \
    .getOrCreate()

    # instanciation de la classe Transform
    transform = Transform(spark)
    
    # instanciation de la classe Extract
    extract = extract.Extract()
    
    # transformation des données 
    transform.transform_tweet_info()
    transform.transform_user_info(extract._user)
