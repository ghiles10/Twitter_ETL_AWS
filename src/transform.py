import configparser
from pathlib import Path
import log_config
from IaC import IaC 
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.functions import regexp_replace, split, substring, lower,regexp_replace

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
            
        logger.debug("Transformation du fichier TWEET_INFO.csv effectuée")
        
        # Enregistrement du DataFrame au format Parquet sur S3 avec compression Snappy, partitionnement sur la colonne "date" et 8 tâches
        tweet_df.write.parquet( self._save_path + "TWEET_DF", mode="overwrite", compression="snappy", partitionBy=["date"]) 
        
    
        
        
        
        