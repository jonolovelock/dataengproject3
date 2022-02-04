import configparser
import psycopg2
import boto3
from create_dwh import create_clients
from create_tables import getClusterInfo
from sql_queries import create_table_queries, drop_table_queries, copy_table_queries, insert_table_queries



def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        print("Copy Query Completed:" + query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        print("Insert Query Completed:" + query)
        conn.commit()


def main():
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
    
    #get Endpoint from newly provisioned cluster
    DWH_ENDPOINT = dwh_info.loc[5,'Value']['Address']
    
    # Connect
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    cur = conn.cursor()
    print("Connected to: " + DWH_DB)
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()