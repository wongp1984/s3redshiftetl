import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, retrieve_counts, loaded_tables, topN_query_tuples


def load_staging_tables(cur, conn):
    """A function to load data into staging tables from S3 by calling the queries

    Args:
        cur (cursor): cursor object of a db connection
        conn (connection): connection object of a dbconnection
    """
    for query in copy_table_queries:
        print(f"Load staging table using query: {query}")
        cur.execute(query)
        conn.commit()
        

def insert_tables(cur, conn):
    """A function to insert data into dimension and fact tables from staging tables by calling the queries

    Args:
        cur (cursor): cursor object of a db connection
        conn (connection): connection object of a dbconnection
    """
    for query in insert_table_queries:
        print(f"Inserting table using query: {query}")
        cur.execute(query)
        conn.commit()
    
#################################################
# def load_staging_tables_test(cur, conn, config):
#     query = copy_table_queries[0]
#     print(f"Load staging table using query: {query}")
#     cur.execute(query)
#     conn.commit()
    
#     print("Finish loading! Next is ")
    
#     query =  ("""
#             copy staging_songs_table
#             from '{}A/B/'
#             iam_role '{}' 
#             json 'auto ignorecase';    
#             """).format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])
    
#     print(f"Load staging table using query: {query}")
#     cur.execute(query)
#     conn.commit()
    
#     query =  ("""
#             copy staging_songs_table
#             from '{}B/B/'
#             iam_role '{}' 
#             json 'auto ignorecase';    
#             """).format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])
    
#     print(f"Load staging table using query: {query}")
#     cur.execute(query)
#     conn.commit()
    
#     print("Done loading!")
#################################################

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # load data from S3 to staging tables
    load_staging_tables(cur, conn)
    
    # insert data into dimension and fact tables
    insert_tables(cur, conn)
    
    # check the count of each loaded table
    for table in loaded_tables:
        retrieve_counts(cur, conn, table_name=table)
        
    # check the top N queries
    for qt in topN_query_tuples:
        retrieve_counts(cur, conn, query_tuple=qt)
    
    conn.close()
    


if __name__ == "__main__":
    main()