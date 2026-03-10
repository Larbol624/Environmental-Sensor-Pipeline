import psycopg2
from dotenv import load_dotenv
import os


def get_connection():
    """

    This function establishes an connection with the postgres database and returns the connection

    """
    dotenv_path=os.path.join(os.path.dirname(__file__),'../../configs/postgres/.env')
    load_dotenv(dotenv_path)
    
    conn=psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=int(os.getenv("POSTGRES_PORT"),5432)
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
    cursor.execute("""SELECT * FROM public."Raw_readings"; """)
    data=cursor.fetchall()

    if own_conn:
        conn.close()
    
    return data

def get_all_aggregated(conn=None):
    """

    This functions returns everything from the Aggreted_metrics table
    Also if there is no connection given then it creates it on his own and also closes it.
    If there is a connection given then it will make the request and only return the data

    """

    own_conn = False
    if conn is None:
        conn = get_connection()
        own_conn = True

    
    cursor=conn.cursor()
    cursor.execute("""SELECT * FROM public."Aggregated_metrics" """)
    data=cursor.fetchall()

    if own_conn:
        conn.close()
    
    return data

def get_all_alerts(conn=None):
    """

    This functions returns everything from the Alerts table
    Also if there is no connection given then it creates it on his own and also closes it.
    If there is a connection given then it will make the request and only return the data

    """

    own_conn = False
    if conn is None:
        conn = get_connection()
        own_conn = True

    
    cursor=conn.cursor()
    cursor.execute("""SELECT * FROM public."Alerts" """)
    data=cursor.fetchall()

    if own_conn:
        conn.close()
    
    return data

def insert_into_raw(sensor_id , Timestamp, Temperature, Humidity, Co2, conn=None):
    """
    Insert values into the raw_readings tabel
    Also if there is no connection given then it creates it on his own and also closes it.
    If there is a connection given then it will make the request and only return the data
    """
    
    own_conn = False
    if conn is None:
        conn = get_connection()
        own_conn = True

    cursor=conn.cursor()

    query="""INSERT INTO public."Raw_readings" ("Sensor_id", "TimeStamp", "Temperature", "Humidity", "Co2") VALUES (%s, %s, %s, %s, %s);"""
    cursor.execute(query,(sensor_id,Timestamp,Temperature,Humidity,Co2))

    conn.commit()
    if own_conn:
        conn.close()
    return

def insert_into_aggregated(sensor_id , Timestamp, avgTemperature, avgHumidity, avgCo2, conn=None):
    """
    Insert values into the aggregated metrics tabel
    Also if there is no connection given then it creates it on his own and also closes it.
    If there is a connection given then it will make the request and only return the data
    """
    
    own_conn = False
    if conn is None:
        conn = get_connection()
        own_conn = True

    cursor=conn.cursor()

    query="""INSERT INTO public."Aggregated_metrics" ("Sensor_id", "Window_start", "avg_Temp", "avg_Humidity", "avg_Co2") VALUES (%s, %s, %s, %s, %s);"""
    cursor.execute(query,(sensor_id,Timestamp,avgTemperature,avgHumidity,avgCo2))

    conn.commit()
    if own_conn:
        conn.close()
    return   

def insert_into_alerts(sensor_id , Timestamp, type, problem_message,conn=None):
    """
    Insert alert into the alert tabel
    Also if there is no connection given then it creates it on his own and also closes it.
    If there is a connection given then it will make the request and only return the data
    """
    
    own_conn = False
    if conn is None:
        conn = get_connection()
        own_conn = True

    cursor=conn.cursor()

    query="""INSERT INTO public."Alerts" ("Sensor_id", "TimeStamp", "error_Type", "Problem_message" ) VALUES (%s, %s, %s, %s);"""
    cursor.execute(query,(sensor_id,Timestamp,type, problem_message))

    conn.commit()
    if own_conn:
        conn.close()
    return   
