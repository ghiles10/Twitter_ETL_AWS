import configparser
from pathlib import Path
import tweepy
import log_config
import csv
import io
from IaC import IaC 

# récupération des paramètres de configuration dans le fichier config.cfg
config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parent}/config.cfg"))

# logging
logger = log_config.logger

class Extract : 
    
    """ this class allows you to extract data from the Twitter API and send it to s3 in csv format """
    
    def __init__(self) :
        
        self._infos = None 
        self.user_name = None 
    
    def extract_tweet(self)  : 

        """ allows you to retrieve all the data related to the user account and to store the data as a dictionary """

        # get user name
        self.user_name = config.get('TWITTER','USER_NAME')
        
        # get twitter credentials
        access_key = config.get('TWITTER','ACCES_KEY')
        access_secret = config.get('TWITTER','ACCES_SECRET')
        consumer_key = config.get('TWITTER','CONSUMER_KEY')
        consumer_secret = config.get('TWITTER','CONSUMER_SECRET')

        # Twitter authentication
        auth = tweepy.OAuthHandler(access_key, access_secret)   
        auth.set_access_token(consumer_key, consumer_secret) 
        logger.debug("Twitter authentication")
        
        # Creating an API object 
        api = tweepy.API(auth)

        # store info

        self._infos = {
            self.user_name: {
                "TWEET_INFO": {
                    "text": [] ,
                    "favorite_count":[] ,
                    "retweet_count": [],
                    "created_at":[]
                },
                "USER_INFO": {
                    "followers_count": '' ,
                    "following_count": '',
                    "created_at":'' ,
                    "description":'' ,
                },
                "USER_ACTIVITY": {
                    "favoris_count":'' ,
                    "retweet_count":'' ,
                }
            }
        }

        # get user tweets 
        tweets = api.user_timeline(screen_name='@'+str(self.user_name), 
                                count=50,
                                include_rts = False,
                                tweet_mode = 'extended') 
        
        # retrieves tweets
        for tweet in tweets : 
            self._infos[self.user_name]["TWEET_INFO"]['text'].append(tweet._json["full_text"])
            self._infos[self.user_name]["TWEET_INFO"]['favorite_count'].append(tweet.favorite_count)
            self._infos[self.user_name]["TWEET_INFO"]['retweet_count'].append(tweet.retweet_count)
            self._infos[self.user_name]["TWEET_INFO"]['created_at'].append(tweet.created_at)
        
        logger.debug("get user tweets")
    
        # Retrieving account information
        user = api.get_user( screen_name = self.user_name)
        
        self._infos[self.user_name]["USER_INFO"]['followers_count'] =  user.followers_count
        self._infos[self.user_name]["USER_INFO"]['following_count'] = user.friends_count
        self._infos[self.user_name]["USER_INFO"]['created_at'] = user.created_at
        self._infos[self.user_name]["USER_INFO"]['description'] = user.description

        self._infos[self.user_name]["USER_ACTIVITY"]['favoris_count'] =  user.favourites_count
        self._infos[self.user_name]["USER_ACTIVITY"]['retweet_count'] =  user.statuses_count
        
        logger.debug("get user info")
               
    def tweet_info_to_csv_s3(self, s3)  : 

        """ allows to transform the data into a csv file and to send it to s3 for tweets infos""" 
        
        data = self._infos[self.user_name]['TWEET_INFO']

        column_names = ["text", "favorite_count", "date_creation", "retweet_count"]
        table_name = 'TWEET_INFO' 
        table_data = [] 
        
        # create table data
        for col1, col2, col3, col4 in zip (data['text'], data['favorite_count'] , data['created_at'] , data['retweet_count'] ) : 

            table_data.append([col1,col2,col3,col4])         

        logger.debug("retrieve data in a table list for tweets infos")
            
        # create a file-like buffer to receive CSV data
        csv_file = io.StringIO()

        # create the csv writer
        writer = csv.writer(csv_file)
        # write the header
        writer.writerow(column_names) 
        # write data 
        writer.writerows(table_data)
        
        logger.debug("create csv file for tweets infos") 
        
        # upload the file to S3
        s3._s3.Bucket( s3._bucket_name ).put_object(Key=table_name + '.csv', Body=csv_file.getvalue().encode('UTF-8'))
        logger.debug("upload csv file to s3 for tweets infos") 

    def user_info_to_csv_s3(self, s3)  : 

        """ allows to transform the data into a csv file and to send it to s3 for user infos """ 
            
        data = self._infos[self.user_name]['USER_INFO']

        column_names = ["followers", "following"  ,"date_creation" , "description" ]
        table_name = 'USER_INFO'
        table_data = [] 
        
        # create table data
        table_data.append((data['followers_count'] ,data['following_count'] , data['created_at']  , data['description']) )     

        logger.debug("retrieve data in a table list for user infos") 
            
        # create a file-like buffer to receive CSV data
        csv_file = io.StringIO()
        # create the csv writer
        writer = csv.writer(csv_file)
        # write the header
        writer.writerow(column_names)      
        # write data 
        writer.writerows(table_data)
        
        logger.debug("create csv file for user infos")
        
        # upload the file to S3
        s3._s3.Bucket( s3._bucket_name ).put_object(Key=table_name + '.csv', Body=csv_file.getvalue().encode('UTF-8'))
        logger.debug("upload csv file to s3 for user infos")

    def user_activity_to_csv_s3(self, s3)  : 

        """ allows to transform the data into a csv file and to send it to s3 for user infos """ 
            
        data = self._infos[self.user_name]['USER_ACTIVITY']

        column_names = ["favorite_count", "retweet_count" ]
        table_name = 'USER_ACTIVITY'
        table_data = [] 
        
        # create table data
        table_data.append( (data['favoris_count'] ,data['retweet_count'] ))         

        logger.debug("retrieve data in a table list for user infos for user activity") 
            
        # create a file-like buffer to receive CSV data
        csv_file = io.StringIO()
        # create the csv writer
        writer = csv.writer(csv_file)
        # write the header
        writer.writerow(column_names)      
        # write data 
        writer.writerows(table_data)
        
        logger.debug("create csv file for user infos for user activity")
        
        # upload the file to S3
        s3._s3.Bucket( s3._bucket_name ).put_object(Key=table_name + '.csv', Body=csv_file.getvalue().encode('UTF-8'))
        logger.debug("upload csv file to s3 for user infos for user activity")
      
if __name__ == '__main__' : 
    
    # create instance of class Extract
    extract = Extract() 
    extract.extract_tweet()
    iac= IaC()
    extract.tweet_info_to_csv_s3(iac)
    extract.user_info_to_csv_s3(iac)
    extract.user_activity_to_csv_s3(iac) 
    
    
    






















