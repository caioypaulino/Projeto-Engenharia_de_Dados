import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace criado com sucesso!")

def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT
    """)

    print("Tabela criada com sucesso!")

def insert_data(session, **kwargs):
    print("Inserindo dados...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    post_code = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, post_code, 
                                                    email, username, registered_date, phone, picture)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address, post_code,
              email, username, registered_date, phone, picture))

        logging.info(f"Dados inseridos para {first_name} {last_name}")

    except Exception as ex:
        logging.error(f"Não foi possível inserir dados por conta da exception {ex}")



def create_spark_connection():
    spark_conn = None

    try:
        spark_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark-cassandra-connector_2.13:3.41", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection criada com sucesso!")

    except Exception as ex:
        logging.error(f"Não foi possível criar a spark session por conta da exception {ex}")

    return spark_conn

def connect_to_kafka(spark_conn):
    spark_df = None

    try:
        spark_df = spark_conn. readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Dataframe kafka criado com sucesso!")

    except Exception as ex:
        logging.warning(f"Dataframe kafka não pode ser criado por conta da exception {ex}")

    return spark_df

def create_cassandra_connection():
    session = None

    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        session = cluster.connect()

        return session

    except Exception as ex:
        logging.error(f"Não foi possível criar a cassandra connection por conta da exception {ex}")

        return None

if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            insert_data(session)