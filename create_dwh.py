import pandas as pd
import boto3
import json
import configparser
from botocore.exceptions import ClientError

def create_clients(KEY, SECRET):
    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

    s3 = boto3.resource('s3',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                       )
    redshift = boto3.client('redshift',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                           )

    iam = boto3.client('iam',aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                         region_name='us-west-2'
                      )
    return ec2, s3, redshift, iam

def create_dwh_role(DWH_IAM_ROLE_NAME, iam_client):
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam_client.create_role(
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
    
    print("Attaching S3 Policy")
    try:
        iam_client.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                              )['ResponseMetadata']['HTTPStatusCode']
        print('Policy Attached')
    except Exception as e:
            print(e)
    
    print("Attaching Redshift Policy")
    try:
        iam_client.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                               PolicyArn="arn:aws:iam::aws:policy/AmazonRedshiftFullAccess"
                              )['ResponseMetadata']['HTTPStatusCode']
        print('Policy Attached')
    except Exception as e:
            print(e)
            
    print("1.3 Get the IAM role ARN")
    roleArn = iam_client.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    print(roleArn)
    return roleArn
    
def create_cluster(DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, roleArn, redshift):
    try:
        response = redshift.create_cluster(        
        #HW
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
    
def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")
    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
    DWH_IAM_ARN            = config.get("DWH", "DWH_IAM_ARN")
    
    clients = create_clients(KEY, SECRET)
    ec2 = clients[0]
    s3 = clients[1]
    redshift = clients[2]
    iam_client = clients[3]
    
    roleArn = create_dwh_role(DWH_IAM_ROLE_NAME, iam_client)
    create_cluster(DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, roleArn, redshift)
    print('Cluster Created')
if __name__ == "__main__":
    main()