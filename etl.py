import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """
    Description: This function is used to load the staging tables
                 defined in the list object 'copy_table_queries'
    Arguments:
        cur == the cursor object
        conn == object of the connection to the database
    Returns:
        None
    """
    try:
        from time import time
        load_times = []
        for table, query in copy_table_queries.items():
            print("======= LOADING TABLE: {} =======".format(table))
            t0 = time()
            cur.execute(query)
            conn.commit()

            load_time = time()-t0
            load_times.append(load_time)

            print("=== DONE IN: {0:.2f} sec\n".format(load_time))
    except psycopg2.Error as e:
        print(e)


def insert_tables(cur, conn):
    """
    Description: This function is used to insert the fact and dimension tables
                 defined in the list object 'insert_table_queries'
    Arguments:
        cur == the cursor object
        conn == object of the connection to the database
    Returns:
        None
    """
    try:
        from time import time
        load_times = []
        for table, query in insert_table_queries.items():
            print("======= LOADING TABLE: {} =======".format(table))
            t0 = time()
            cur.execute(query)
            conn.commit()
            load_time = time()-t0
            load_times.append(load_time)

            print("=== DONE IN: {0:.2f} sec\n".format(load_time))
    except psycopg2.Error as e:
        print(e)


def main():
    """
    Description: This function initially creates a connection object
                 called 'conn' to the database using the credentials
                 in the config file as well as a cursor object to run
                 queries called 'cur'. Next, it runs the
                 'load_staging_tables' function to load the
                 staging tables in the database then runs the
                 'insert_tables' function to then insert
                 the fact and dimension the tables.
                 Last, it closes the connection to the database.
    Arguments:
        cur == the cursor object
        conn == object of the connection to the database
    Returns:
        None
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
