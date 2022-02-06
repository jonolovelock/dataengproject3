import configparser
import psycopg2
import boto3
import pandas as pd
from create_dwh import create_clients
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function applies each query in the drop_table_queries list (from sql.queries.py)

    INPUTS:
    * cur: the cursor variable
    * conn: the connection to the Redshift DWH.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function applies each query in the create_table_queries list (from sql.queries.py)

    INPUTS:
    * cur: the cursor variable
    * conn: the connection to the Redshift DWH.
    """
    for query in create_table_queries:
        print("Now executing: " + query)
        cur.execute(query)
        conn.commit()
        
def dwh_properties(cluster):
    """
    This function takes in a Redshift cluster object and returns a dataframe containing proerties specific to a Redshift datawarehouse

    INPUTS:
    * cluster: the current Redshift cluster object
    """
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in cluster.items() if k in keysToShow]
    properties = pd.DataFrame(data=x, columns=["Key", "Value"])
    return properties 

def getClusterInfo(redshift_client, cluster_Id):
    """
    This function takes in a Redshift SDK client and a Redshift cluster ID and returns properties specific to a Redshift cluster 

    INPUTS:
    * redshift_client: a Redshift SDK object 
    * cluster_id: the Redshift cluster ID
    """
    #This only works for this project where we only have 1 cluster, in future needs to be re-factored to handle multi-cluster enironments
    cluster_properties = redshift_client.describe_clusters(ClusterIdentifier=cluster_Id)['Clusters'][0]
    dwh_info = dwh_properties(cluster_properties)
    return dwh_info

def main():
    """
    This runs the 'create_tables.py' module in order to create staging and analytics tables for the sparkify database
    This will only work if the 'create_dwh.py script has alread been run and the Redhift cluster status is equal to Available'
    """
    #Get required Config
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    DWH_DB_USER = config.get('DWH', 'DWH_DB_USER')
    DWH_DB_PASSWORD = config.get('DWH', 'DWH_DB_PASSWORD')
    DWH_PORT = config.get('DWH', 'DWH_PORT')
    DWH_DB= config.get('DWH', 'DWH_DB')
    DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
    
    redshift_client = create_clients(KEY, SECRET)[2]
    dwh_info = getClusterInfo(redshift_client, DWH_CLUSTER_IDENTIFIER)
#     print(dwh_info)
    # get Endpoint from newly provisioned cluster
    # Note: this code should be refactered, must be neater way to do this
    DWH_ENDPOINT = dwh_info.iat[5,1]['Address']
    print(DWH_ENDPOINT)
    
    # Connect
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    cur = conn.cursor()
    print("Connected to: " + DWH_DB)
    
    # Run Queries
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()