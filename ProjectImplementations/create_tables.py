import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """A function to drop tables by calling the queries

    Args:
        cur (cursor): cursor object of a db connection
        conn (connection): connection object of a dbconnection
    """
    for query in drop_table_queries:
        print(f"Drop table using query: {query}")
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """A function to create tables by calling the queries

    Args:
        cur (cursor): cursor object of a db connection
        conn (connection): connection object of a dbconnection
    """
    for query in create_table_queries:
        print(f"Create table using query: {query}")
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()