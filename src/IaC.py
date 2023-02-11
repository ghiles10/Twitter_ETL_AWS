import configparser
import boto3
import json
import pandas as pd
import psycopg2

# récupération des paramètres de configuration dans le fichier config.cfg 

config = configparser.ConfigParser()
config.read_file(open('config.cfg'))

# KEY                    = config.get('AWS','KEY')
# SECRET                 = config.get('AWS','SECRET')

# DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
# DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
# DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

# DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
# DWH_DB                 = config.get("DWH","DWH_DB")
# DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
# DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
# DWH_PORT               = config.get("DWH","DWH_PORT")

# DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")



class Iac:

    def __init__(self) -> None:
        
    # Création des clients pour accéder aux services AWS

        self._ec2 = boto3.resource('ec2',
                            region_name="eu-west-3",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )

        self._s3 = boto3.resource('s3',
                            region_name="eu-west-3",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                        )

        self._iam = boto3.client('iam',aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET,
                            region_name='eu-west-3'
                        )

        self._redshift = boto3.client('redshift',
                        region_name="eu-west-3",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )


    # création d'une bucket afin de stocker les données 
    def create_bucket(self) : 
        
        """ this function is used to create a bucket"""
        
        self._s3.create_bucket(Bucket='ghiles_football_etl', CreateBucketConfiguration={'LocationConstraint': 'eu-west-3'} )

    def create_cluster(self) : 
        
        """ this function is used to create a cluster"""
        
        # création d'un role IAM pour accéder à Redshift

        try:
            
            print("1.1 Creating a new IAM Role") 
            self._iam.create_role(
                Path='/',
                RoleName=DWH_IAM_ROLE_NAME,
                Description = "Allows Redshift clusters to call AWS services on your behalf.",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}}],
                    'Version': '2012-10-17'})
            )    
            
        except Exception as e:
            print(e)
            
            
        print("1.2 Attaching Policy")

        self._iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                            )

        print("1.3 Get the IAM role ARN")
        roleArn = self._iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

        # création du cluster Redshift 
        try:
            self._redshift.create_cluster(        
                
                ClusterType=DWH_CLUSTER_TYPE,
                NodeType=DWH_NODE_TYPE,
                NumberOfNodes=int(DWH_NUM_NODES),

                #Identifiers & Credentials
                DBName=DWH_DB,
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                MasterUsername=DWH_DB_USER,
                MasterUserPassword=DWH_DB_PASSWORD,
                
                #Roles (for s3 access)
                IamRoles=[roleArn]  
            )
        except Exception as e:
            print(e)


    # fonction pour afficher les propriétés du cluster
    def pretty_redshift_props(self, props):
        
        """ this function is used to display the properties of the cluster """
        
        pd.set_option('display.max_colwidth', -1) 
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in props.items() if k in keysToShow]
        
        return pd.DataFrame(data=x, columns=["Key", "Value"])

    def describe_cluster(self) : 
        
        """ this function is used to describe the cluster"""
        

    # récupération des propriétés du cluster
    myClusterProps = self._redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']

    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
    
    return DWH_ENDPOINT, DWH_ROLE_ARN


# ouverture du port 5439 pour accéder au cluster

try:
    
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)

# s'assurer que le cluster est bien créé 

def verify_cluster_status(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB ): 
    
    """ this function is used to verify the status of the cluster """
    
    conn = psycopg2.connect(
        host= DWH_ENDPOINT,
        database= DWH_DB,
        user= DWH_DB_USER,
        password= DWH_DB_PASSWORD,
        port= DWH_PORT
    )

    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    print(cursor.fetchone())
    conn.close()

