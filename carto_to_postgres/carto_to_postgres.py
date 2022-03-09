import json
import os

import geopandas as gpd
import pandas as pd
import psycopg2
from tqdm import tqdm
from carto.auth import APIKeyAuthClient
from carto.sql import CopySQLClient
from cartoframes.utils import decode_geometry
from shapely.geometry.base import BaseGeometry
from sqlalchemy import create_engine

COLLISION_STRATEGIES = ['fail', 'replace']


def download_carto_dataset(username, api_key, table_name):
    """
    Function to download a CARTO dataset as a CSV file using COPY to command through CARTO's SQL API

    Returns name of the downloaded file (file_name, str)
    args:
        username: CARTO account username (str)
        api_key: CARTO API key with access to the dataset (str)
        table_name: Target CARTO dataset
    """

    file_name = f"{table_name}.csv"

    print(f"Downloading dataset {table_name}")

    base_url = f"https://{username}.carto.com"

    copy_client = CopySQLClient(APIKeyAuthClient(base_url, api_key))

    to_query = f"COPY {table_name} TO stdout WITH (FORMAT csv, HEADER true)"

    copy_client.copyto_file_path(to_query, f'{table_name}.csv')

    print(f"Dataset {table_name} downloaded")

    return file_name


def connect_database(
        host,
        database,
        user,
        password,
        port=None,
        sslmode=None,
        sslrootcert=None,
        sslcert=None,
        sslkey=None):
    """
    Function to connect to a PostgreSQL database

    Returns SQLAlchemy and psycopg2 connection objects
    args:
        host: database server corresponding host (str)
        database: database name (str)
        user: database target user (str)
        password: user's corresponding password (str)
    """

    args = {
        "host": host,
        "user": user,
        "port": port or 5432,
        "password": password,
        "database": database,
        "sslcert": sslcert or None,
        "sslkey": sslkey or None,
        "sslrootcert": sslrootcert or None,
        "sslmode": sslmode or 'prefer'
    }

    engine = create_engine(
        f"postgresql+psycopg2://{host}:{port}:{user}@{password}/{database}",
        connect_args=args)

    con = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        sslmode=sslmode,
        sslrootcert=sslrootcert,
        sslcert=sslcert,
        sslkey=sslkey)

    return engine, con


def check_table_name_length(table_name):
    """
    Function to check if a table name length is below PostgreSQL 63 byte limit

    Returns table name below this limit, truncating original name if necessary (table_name, str)
    args:
        host: database server corresponding host (str)
        database: database name (str)
        user: database target user (str)
        password: user's corresponding password (str)
    """

    if len(table_name) >= 63:
        table_name = table_name[:62]

        print(f"Table name too large, truncating to {table_name}")

    return table_name


def create_table_postgis(
        file_name,
        table_name,
        schema,
        engine,
        con,
        if_exists='replace'):
    """
    Function that given a CSV file path, creates a table inside the Postgres database
    with the correct data structure

    Depends on function check_table_length

    This function reads the CSV file, creates a GeoDataFrame, formats the data to upload to PostgreSQL
    and creates a table with the desired data types and column order.

    Returns value to indicate if COPY process should happen (proceed_copy, bool)
    and psycopg2 cursor object (cursor)
    args:
        file_name: path to csv file (str)
        table_name: desired database table name (str)
        schema: target database schema (str)
        host: connection host (str)
        database: target database (str)
        user: connection user (str)
        password: password for user (str)
        if_exists: defines how to behave if the table already exists {'fail', 'replace'}, default 'replace'
                   - fail: Raise a ValueError
                   - replace: Drop the table before inserting new values
    """

    if if_exists not in COLLISION_STRATEGIES:
        raise ValueError(
            "if_exists was not in available options, please try 'fail' or 'replace'")

    proceed_copy = True

    cursor = con.cursor()

    value = BaseGeometry()

    table_name = check_table_name_length(table_name)

    df = pd.read_csv(file_name, nrows=10)

    columns_ordered = [col if (col != 'the_geom')
                       else 'geometry' for col in df.columns.values]

    gdf = gpd.GeoDataFrame(
        df,
        crs='EPSG:4326',
        geometry=decode_geometry(
            df['the_geom']))

    gdf['geometry'].fillna(value, inplace=True)

    gdf.drop('the_geom', axis=1, inplace=True)

    gdf = gdf.reindex(columns=columns_ordered)

    try:
        gdf.astype(object).to_postgis(
            name=table_name,
            schema=schema,
            con=engine,
            if_exists=if_exists)

        cursor.execute(f'truncate table "{schema}".{table_name};')

        cursor.execute(
            f'alter table "{schema}".{table_name} rename column geometry to the_geom;')

    except Exception as e:
        proceed_copy = False
        print(f"Some error ocurred creating table {e}")

    return proceed_copy, cursor


def dataset_to_postgis(
        file_name,
        table_name,
        schema,
        host,
        database,
        user,
        password,
        sslmode=None,
        sslrootcert=None,
        sslcert=None,
        sslkey=None,
        if_exists='replace'):
    """
    Function that uploads a dataset (CSV file) to a Postgres database.

    Depends on function create_table_postgis
    Once the table is created, it uses PostgreSQL COPY from command to upload the data
    args:
        file_name: path to csv file (str)
        table_name: desired database table name (str)
        schema: target database schema (str)
        host: connection host (str)
        database: target database (str)
        user: connection user (str)
        password: password for user (str)
    """

    engine, con = connect_database(host=host, database=database,
                                   user=user, password=password,
                                   sslmode=sslmode, sslrootcert=sslrootcert,
                                   sslcert=sslcert, sslkey=sslkey)

    proceed_copy, cursor = create_table_postgis(
        file_name=file_name, table_name=table_name, schema=schema, engine=engine, con=con, if_exists=if_exists)

    if proceed_copy:

        copy_sql = f"""
               COPY "{schema}".{table_name} FROM stdin WITH CSV HEADER
               DELIMITER as ','
               """
        print(f"Copying dataset {table_name} to postgres")

        with open(file_name, 'r') as f:

            try:
                cursor.copy_expert(sql=copy_sql, file=f)
                con.commit()
                print(f"Dataset {table_name} copied to postgres")
            except (Exception, psycopg2.DatabaseError) as error:
                print(f"Some error ocurred copying dataset {error}")
                con.rollback()
            cursor.close()


def carto_to_postgis(
        username,
        table_name,
        api_key,
        schema,
        host,
        database,
        user,
        password,
        sslmode=None,
        sslrootcert=None,
        sslcert=None,
        sslkey=None,
        if_exists='replace'):
    """
    Function that downloads a CARTO dataset and uploads it to a PostgreSQL database.

    Depends on download_carto_dataset and dataset_to_postgis.
    args:
        username: CARTO account username (str)
        api_key: CARTO API key with access to the dataset (str)
        table_name: desired database table name (str)
        schema: target database schema (str)
        host: connection host (str)
        database: target database (str)
        user: connection user (str)
        password: password for user (str)
    """

    file_name = download_carto_dataset(username=username, api_key=api_key,
                                       table_name=table_name)

    dataset_to_postgis(
        file_name=file_name,
        table_name=table_name,
        schema=schema,
        host=host,
        database=database,
        user=user,
        password=password,
        sslmode=sslmode,
        sslrootcert=sslrootcert,
        sslcert=sslcert,
        sslkey=sslkey,
        if_exists=if_exists)

    os.remove(file_name)


with open("config.json") as config:
    config = json.load(config)

username = config.get('username')
api_key = config.get('api_key')
schema = config.get('schema')
if_exists = config.get('if_exists')
table_list = config.get('table_list')

param_dict = {
    "host": config.get('host'),
    "database": config.get('database'),
    "port": config.get('port'),
    "user": config.get('user'),
    "password": config.get('password'),
    "sslcert": config.get('sslcert'),
    "sslkey": config.get('sslkey'),
    "sslrootcert": config.get('sslrootcert'),
    "sslmode": config.get('sslmode')
}

for table_name in tqdm(table_list):

    carto_to_postgis(username=username, table_name=table_name, api_key=api_key,
                     schema=schema, **param_dict, if_exists=if_exists)
