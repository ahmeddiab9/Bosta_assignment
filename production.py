import logging
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from pymongo import MongoClient
from sqlalchemy import create_engine
# (for schedule)
# import schedule
# import time

# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Add a file handler to log messages to a file
file_handler = logging.FileHandler('etl.log')
file_handler.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


@task(retries=1, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str , num_chunks: int) -> pd.DataFrame:
    try:
        """Read data from web, load it into MongoDB"""
        # Specify data types for columns with mixed types
        dtype_mapping = {6: str}  # Assuming column 6 is the problematic column, change the index if needed

        chunk_size = 1000  # Number of rows per chunk
        
        # Read data in chunks
        df_chunks = pd.read_csv(dataset_url, dtype=dtype_mapping, chunksize=chunk_size)

        # MongoDB connection details
        mongodb_connection_string = "mongodb://fr3on:fr30n@13.38.181.99:27007"
        mongodb_database = "db_taxi"
        mongodb_collection = "yellow_taxi_data_2023"

        # Load dataframe into MongoDB
        client = MongoClient(mongodb_connection_string)
        db = client[mongodb_database]
        collection = db[mongodb_collection]
        
        
        # Insert each chunk separately
        inserted_chunks = 0
        for i, df_chunk in enumerate(df_chunks):
            collection.insert_many(df_chunk.to_dict(orient='records'))
            inserted_chunks += 1
            print(f"Inserted chunk {i + 1} into MongoDB")
            if inserted_chunks >= num_chunks:
                break
        
        # client.close()
        
        # Print message when all chunks are done
        print("All chunks inserted into MongoDB")
    
        # Fetch the complete data from MongoDB and create a DataFrame
        data = list(collection.find())
        df = pd.DataFrame(data)
    
        # Output sample of the DataFrame
        print("Sample of the DataFrame:")
        print(df.head(10))
    
        return df
    except Exception as e:
        logger.error(f"Error occurred while fetching data: {str(e)}")
        return None


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    try:
        """Fix Dtype issues, remove duplicates, and calculate time difference"""
        # Fix Dtype issues
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

        # Remove duplicates
        df = df.drop_duplicates()

        # Calculate time difference
        df['time_difference'] = df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']

        print(df.head(2))
        print(f'columns:  {df.dtypes}')
        return df
    except Exception as e:
        logger.error(f"Error occurred while cleaning data: {str(e)}")
        return None


@task()
def load_into_mysql(df: pd.DataFrame) -> None:
    try:
        """Load the cleaned data into MySQL database"""
        SQLALCHEMY_CONNECTION_STRING = "mysql+pymysql://dbmasteruser:7c!+f;?jGu0W;>{uFT=?}w2Kuk,<B^|z@ls-ae0dfa25335c900a4ba55bc931e1c4df110ba935.cw4xkre3miml.eu-west-1.rds.amazonaws.com:3306"
        SQL_TABLE_NAME = "yellow_tripdata_2023"
    
        
        # Establish SQL connection using SQLAlchemy
        sql_engine = create_engine(SQLALCHEMY_CONNECTION_STRING)

        
        # Create the database if it doesn't exist
        sql_engine.execute("CREATE DATABASE IF NOT EXISTS dataset")
        
         # Select the database
        sql_engine.execute("USE dataset")
        
        # Define the table schema
        table_schema = """
            CREATE TABLE IF NOT EXISTS {} (
                VendorID INT,
                tpep_pickup_datetime DATETIME,
                tpep_dropoff_datetime DATETIME,
                passenger_count FLOAT,
                trip_distance FLOAT,
                RatecodeID FLOAT,
                store_and_fwd_flag VARCHAR(1),
                PULocationID INT,
                DOLocationID INT,
                payment_type INT,
                fare_amount FLOAT,
                extra FLOAT,
                mta_tax FLOAT,
                tip_amount FLOAT,
                tolls_amount FLOAT,
                improvement_surcharge FLOAT,
                total_amount FLOAT,
                congestion_surcharge FLOAT,
                airport_fee FLOAT,
                time_difference FLOAT
            )
        """.format(SQL_TABLE_NAME)
        
        # Create the table in the database
        sql_engine.execute(table_schema)

        # Insert the cleaned data into the MySQL table
        insert_query = """
            INSERT INTO {} (
                VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance,
                RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type,
                fare_amount, extra, mta_tax, tip_amount, tolls_amount,
                improvement_surcharge, total_amount, congestion_surcharge, airport_fee, time_difference
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """.format(SQL_TABLE_NAME)

        # Convert the DataFrame to a list of tuples
        rows = [tuple(row) for row in df.itertuples(index=False)]
        # Insert the data using batch inserts
        sql_engine.execute(insert_query, rows)


        print("Data loaded into MySQL successfully!")
    except Exception as e:
        logger.error(f"Error occurred while loading data into MySQL: {str(e)}")


@flow()
def etl_mongodb_to_mysql(color: str, year: int, month: int, num_chunks: int) -> None:
    """The main ETL function"""
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

    df = fetch(dataset_url , num_chunks)
    if df is not None:
        df_clean = clean(df)
        if df_clean is not None:
            load_into_mysql(df_clean)


@flow()
def etl_parent_flow(months: list[int] = [1, 2], year: str = '2021', color: str = 'yellow' , num_chunks: int = 1):
    for month in months:
        etl_mongodb_to_mysql(color, year, month , num_chunks)


if __name__ == '__main__':
    color = 'yellow'
    months = [1,2]
    year = '2021'
    etl_parent_flow(months, year, color)


# if i want to schedule this job in every day at 3 am 
# Define the ETL job function
# def run_etl_job():
#     color = 'yellow'
#     months = [1, 2]
#     year = '2021'
#     etl_parent_flow(months, year, color)


# # Schedule the job to run every day at 3 am
# schedule.every().day.at("03:00").do(run_etl_job)

# # Continuously run the scheduler
# while True:
#     schedule.run_pending()
#     time.sleep(1)