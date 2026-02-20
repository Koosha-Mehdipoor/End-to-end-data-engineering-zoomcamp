
import pandas as pd
import sqlalchemy
from sqlalchemy import URL
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')

def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table):
    url_object = URL.create(
        "postgresql+psycopg",
        username= pg_user,
        password=pg_pass,  # plain (unescaped) text
        host=pg_host,
        port = pg_port,
        database= pg_db,
    )
    year = 2021
    month = 1
    chunksize = 100000


    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
    filename = f"yellow_tripdata_{year}-{month:02d}.csv.gz"

    dtype = {
        "VendorID": "Int64",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "RatecodeID": "Int64",
        "store_and_fwd_flag": "string",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "payment_type": "Int64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64"
    }

    parse_dates = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime"
    ]


    engine = create_engine(url_object)

    df_iter = pd.read_csv(
        prefix + filename ,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator = True ,
        chunksize = chunksize)


    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(
                name=target_table, 
                con = engine, 
                if_exists = 'replace'
            )
            first = False
            print("Table Created")



        df_chunk.to_sql(
            name= target_table,
            con = engine, 
            if_exists = 'append')
        print("Inserted: " , len(df_chunk))

    pass
if __name__ == '__main__':
    run()
