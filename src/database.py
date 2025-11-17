import psycopg2
from dotenv import load_dotenv
import os


def get_connection():
    """

    This function establishes an connection with the postgres database and returns the connection

    """
    dotenv_path=os.path.join(os.path.dirname(__file__),'../configs/postgres/.env')
    load_dotenv(dotenv_path)
    conn=psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=int(os.getenv("POSTGRES_PORT=5432",5432))
    )
    return conn


def get_all_raw(conn=None):
    """

    This functions returns everything from the Raw_readings table
    Also if there is no connection given then it creates it on his own and also closes it.
    If there is a connection given then it will make the request and only return the data

    """

    own_conn = False
    if conn is None:
        conn = get_connection()
        own_conn = True

    
    cursor=conn.cursor()
    cursor.execute("""SELECT * FROM public."Raw_readings" """)
    data=cursor.fetchall()

    if own_conn:
        conn.close()
    
    return data
