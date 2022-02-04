import pandas as pd
import boto3
import json
import configparser
from botocore.exceptions import ClientError

def create_clients(KEY, SECRET):
    redshift = boto3.client('redshift',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                           )

    return redshift

def delete_cluster(redshift, DWH_CLUSTER_IDENTIFIER):
    try:
        redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
        print("Cluster deleted")
    except Exception as e:
        print(e)
    
    
def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

    redshift = create_clients(KEY, SECRET)
    
    delete_cluster(redshift, DWH_CLUSTER_IDENTIFIER)
    
if __name__ == "__main__":
    main()