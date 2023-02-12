import configparser
import boto3
import json
import psycopg2
from pathlib import Path
import botocore.exceptions


# récupération des paramètres de configuration dans le fichier config.cfg

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parent}/config.cfg"))

class IaC:

    def __init__(self) -> None:
        
        # Création des clients pour accéder aux services AWS
        self.KEY = config.get('AWS','KEY')
        self.SECRET = config.get('AWS','ACCESS')
        
        # Création des clients pour accéder aux services AWS

        self._ec2 = boto3.resource('ec2',
                            region_name="eu-west-3",
                            aws_access_key_id = self.KEY,
                            aws_secret_access_key=self.SECRET
                            )

        self._s3 = boto3.resource('s3',
                            region_name="eu-west-3",
                            aws_access_key_id=self.KEY,
                            aws_secret_access_key=self.SECRET
                        )

        self._iam = boto3.client('iam',aws_access_key_id=self.KEY,
                            aws_secret_access_key=self.SECRET,
                            region_name='eu-west-3'
                        )

        self._redshift = boto3.client('redshift',
                        region_name="eu-west-3",
                        aws_access_key_id=self.KEY,
                        aws_secret_access_key=self.SECRET
                        )
        
        self._myClusterProps = None

    # création d'une bucket afin de stocker les données 
    def create_bucket(self) : 
        
        """ this function is used to create a bucket"""
        try: 
            self._s3.create_bucket(Bucket='ghiles-data-foot', CreateBucketConfiguration={'LocationConstraint': 'eu-west-3'} )
            
        except (botocore.exceptions.BotoCoreError, botocore.exceptions.ClientError) as e:
            error_code = e.response['Error']['Code']
            if error_code == 'BucketAlreadyOwnedByYou':
                # Traitement de l'erreur
                print("Le bucket existe déjà et vous en êtes propriétaire.")
            pass
            

            
    def create_cluster(self) : 
        
        """ this function is used to create a cluster"""
        
        # création d'un role IAM pour accéder à Redshift
        try : 

            print("1.1 Creating a new IAM Role") 
            self._iam.create_role(
                Path='/',
                RoleName=config.get("DWH", "DWH_IAM_ROLE_NAME"),
                Description = "Allows Redshift clusters to call AWS services on your behalf.",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}}],
                    'Version': '2012-10-17'})
                )    
        except Exception as e : 
            pass
                
            print("1.2 Attaching Policy")

        try : 
            self._iam.attach_role_policy(RoleName=config.get("DWH", "DWH_IAM_ROLE_NAME"),
                                PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                                )

            print("1.3 Get the IAM role ARN")
            roleArn = self._iam.get_role(RoleName=config.get("DWH", "DWH_IAM_ROLE_NAME"))['Role']['Arn']
            
        except Exception as e : 
            pass

    # création du cluster Redshift 
    
        try : 
            
            self._redshift.create_cluster(        
                
                ClusterType=config.get("DWH","DWH_CLUSTER_TYPE"),
                NodeType=config.get("DWH","DWH_NODE_TYPE"),
                NumberOfNodes=int(config.get("DWH","DWH_NUM_NODES")),

                #Identifiers & Credentials
                DBName=config.get("DWH","DWH_DB"),
                ClusterIdentifier=config.get("DWH","DWH_CLUSTER_IDENTIFIER"),
                MasterUsername=config.get("DWH","DWH_DB_USER"),
                MasterUserPassword=config.get("DWH","DWH_DB_PASSWORD"),
                
                #Roles (for s3 access)
                IamRoles=[roleArn]  
                )
            
        except Exception as e :
            
            print('erreur lors de la création du cluster')
            print(e)
            
                

    def open_port(self) : 
        
        """ this function is used to open the port 5439 to access the cluster"""
        
        # ouverture du port 5439 pour accéder au cluster
        try : 
            
            self._myClusterProps = self._redshift.describe_clusters(ClusterIdentifier=config.get("DWH","DWH_CLUSTER_IDENTIFIER"))['Clusters'][0]
            vpc = self._ec2.Vpc(id = self._myClusterProps['VpcId'])
            defaultSg = list(vpc.security_groups.all())[0]
            
            print(defaultSg)
            defaultSg.authorize_ingress(
                GroupName=defaultSg.group_name,
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(config.get("DWH","DWH_PORT")),
                ToPort=int(config.get("DWH","DWH_PORT"))
            )

        except  Exception as e :
            print(e)
            pass

    # s'assurer que le cluster est bien créé 
    def verify_cluster_status(self ): 
        
        """ this function is used to verify the status of the cluster """
        self._myClusterProps = self._redshift.describe_clusters(ClusterIdentifier=config.get("DWH","DWH_CLUSTER_IDENTIFIER"))['Clusters'][0]
        DWH_ENDPOINT = self._myClusterProps['Endpoint']['Address']
        
        conn = psycopg2.connect(
            host= DWH_ENDPOINT,
            database= config.get("DWH","DWH_DB"),
            user= config.get("DWH","DWH_DB_USER"),
            password= config.get("DWH","DWH_DB_PASSWORD"),
            port= config.get("DWH","DWH_PORT")
        )

        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        print('------------------------------------------ test ------------------------------------------')
        print(cursor.fetchone())
        conn.close()


if __name__ == '__main__' : 
    
    iac = IaC()
    iac.create_bucket()
    iac.create_cluster()
    iac.open_port()
    iac.verify_cluster_status()
    
    
    