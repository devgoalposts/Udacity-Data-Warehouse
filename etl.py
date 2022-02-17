import configparser
import psycopg2
from sql_queries import create_table_queries, copy_table_queries, insert_table_queries

def create_tables(cur, conn):
    print("Creating tables")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print("Done creating tables\n")

def load_staging_tables(cur, conn):
    print("Copying to staging tables")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print("Done copying to staging tables")


def insert_tables(cur, conn):
    print("Inserting into tables")
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    print("Done inserting into tables\n")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    create_tables(cur, conn)
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()