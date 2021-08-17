import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Copy staging tables from files stored in S3.
    Print error if any.
    """
    try:
        for query in copy_table_queries:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        print(query, e)        



def insert_tables(cur, conn):
    """
    Inserts records from staging tables into fact/dimension tables.
    Print error if any.
    """
    try:
        for query in insert_table_queries:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        print(query, e)

def main():
    """
    Connects to AWS Redshift cluster.
    Set up database and tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()