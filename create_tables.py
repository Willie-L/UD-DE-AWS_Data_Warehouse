import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    Description: This function is used to drop the tables
                 defined in the list object 'drop_table_querie'
    Arguments:
        cur == the cursor object
        conn == object of the connection to the database
    Returns:
        None
    """
    try:
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
    except psycopg2.Error as e:
        print(e)


def create_tables(cur, conn):
    """
    Description: This function is used to create the tables
                 defined in the list object 'create_table_queries'
    Arguments:
        cur == the cursor object
        conn == object of the connection to the database
    Returns:
        None
    """
    try:
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
    except psycopg2.Error as e:
        print(e)


def main():
    """
    Description: This function initially creates a connection object called
                 'conn' to the database using the credentials in
                 the config file as well as a cursor object to run queries
                 called 'cur'. Next, it runs the 'drop_tables' function to
                 clear any tables that may already exist in the database
                 then runs the 'create_tables' function to then create the
                 tables. Last, it closes the connection to the database.
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
