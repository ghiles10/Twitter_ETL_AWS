import configparser
from pathlib import Path
import log_config
from IaC import IaC 

# récupération des paramètres de configuration dans le fichier config.cfg
config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parent}/config.cfg"))

# logging
logger = log_config.logger 

class Transform : 
    
    def __init__(self, spark):
        
        self._spark = spark
        self._load_path = "s3://ghiles-data-foot/"

    def transform_tweet_info(self) : 
        tweet_info_df = 