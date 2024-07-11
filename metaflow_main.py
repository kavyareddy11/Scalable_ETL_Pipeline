from metaflow import FlowSpec, step, Flow
from sqlalchemy import create_engine, text
from tenacity import retry, stop_after_attempt, wait_fixed
import pandas as pd
import psycopg2
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AirbnbETLFlow(FlowSpec):
    
    @step
    def start(self):
        logger.info("Starting the flow")
        """
        Start step: Initialize variables and database connection.
        """
        # self.db_url = 'postgresql://postgres@localhost:5432/airbnb'
        # self.engine = create_engine(self.db_url)
        self.csv_path = '/Users/kavyareddy/Downloads/airbnb/AB_NYC_2019.csv' #specify your path here
        self.next(self.create_table)
    
    
    @step
    def create_table(self):
        """
        Create table in PostgreSQL if it does not exist.
        """
        logger.info("Starting create table")
        create_table_query = """
        CREATE TABLE IF NOT EXISTS listings (
            id SERIAL PRIMARY KEY,
            name TEXT,
            host_id INTEGER,
            host_name TEXT,
            neighbourhood_group TEXT,
            neighbourhood TEXT,
            latitude DECIMAL,
            longitude DECIMAL,
            room_type TEXT,
            price INTEGER,
            minimum_nights INTEGER,
            number_of_reviews INTEGER,
            last_review DATE,
            reviews_per_month DECIMAL,
            calculated_host_listings_count INTEGER,
            availability_365 INTEGER
        );
        """
        @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
        def execute_with_retry(engine, query):
            with engine.connect() as conn:
                conn.execute(text(query))

        db_url = 'postgresql://postgres@localhost:5432/airbnb'
        engine = create_engine(db_url)
        try:
            execute_with_retry(engine, create_table_query)
            print("Table created successfully or already exists.")
        except Exception as e:
            print(f"Failed to create table after 3 attempts. Error: {e}")
            raise
        # with engine.connect() as conn:
        #     conn.execute(text(create_table_query))
        logger.info("Created table")
        self.next(self.load_data)
        
    
    @step
    def load_data(self):
        """
        Load data from CSV file into a DataFrame.
        """
        logger.info("Starting loading data")
        try:
            self.df = pd.read_csv(self.csv_path)
        except FileNotFoundError:
            print(f"CSV file not found at {self.csv_path}")
            raise
        except pd.errors.EmptyDataError:
            print("The CSV file is empty")
            raise
        logger.info("Loaded data")
        self.next(self.transform_data)
        
    
    @step
    def transform_data(self):
        """
        Transform the data.
        """
        logger.info("Starting data transformation")
        df = self.df
        # Normalizing text fields
        df['host_name'] = df['host_name'].str.title()
        df['neighbourhood'] = df['neighbourhood'].str.title()
        df['neighbourhood_group'] = df['neighbourhood_group'].str.title()
        # Separate date and time components
        df['last_review_date'] = pd.to_datetime(df['last_review']).dt.date
        df['last_review_time'] = pd.to_datetime(df['last_review']).dt.time
        df['last_review_year'] = pd.to_datetime(df['last_review']).dt.year
        df['last_review_month'] = pd.to_datetime(df['last_review']).dt.month
        df['last_review_day'] = pd.to_datetime(df['last_review']).dt.day
        df['last_review_day_of_week'] = pd.to_datetime(df['last_review']).dt.dayofweek
        # Normalize room types
        df['room_type'] = df['room_type'].replace({'Entire home/apt': 'Entire Home/Apartment', 'Private room': 'Private Room', 'Shared room': 'Shared Room'})
        # Handle missing values
        df['reviews_per_month'].fillna(0, inplace=True)
        # Create derived features
        df['price_per_night'] = df['price'] / df['minimum_nights']
        # Round latitude and longitude
        df['latitude'] = df['latitude'].round(3)
        df['longitude'] = df['longitude'].round(3)
        # Encode categorical variables
        df['room_type_code'] = df['room_type'].astype('category').cat.codes
        df['neighbourhood_group_code'] = df['neighbourhood_group'].astype('category').cat.codes
        # Remove duplicates
        df.drop_duplicates(inplace=True)
        # Optimize data types
        df['host_id'] = df['host_id'].astype('int32')
        df['price'] = df['price'].astype('int32')
        df['number_of_reviews'] = df['number_of_reviews'].astype('int32')
        # Calculating average price per neighborhood
        avg_price_per_neighbourhood = df.groupby('neighbourhood')['price'].mean().reset_index()
        avg_price_per_neighbourhood.rename(columns={'price': 'avg_price'}, inplace=True)
        # Calculate average number of reviews per neighborhood
        avg_reviews_per_neighbourhood = df.groupby('neighbourhood')['number_of_reviews'].mean().reset_index()
        avg_reviews_per_neighbourhood.rename(columns={'number_of_reviews': 'avg_reviews'}, inplace=True)
        # Calculate average minimum nights per neighborhood
        avg_minimum_nights_per_neighbourhood = df.groupby('neighbourhood')['minimum_nights'].mean().reset_index()
        avg_minimum_nights_per_neighbourhood.rename(columns={'minimum_nights': 'avg_minimum_nights'}, inplace=True)
        # Calculate total listings per neighborhood
        total_listings_per_neighbourhood = df['neighbourhood'].value_counts().reset_index()
        total_listings_per_neighbourhood.columns = ['neighbourhood', 'total_listings']
        # Merge metrics into a single DataFrame
        metrics_df = avg_price_per_neighbourhood.merge(avg_reviews_per_neighbourhood, on='neighbourhood')
        metrics_df = metrics_df.merge(avg_minimum_nights_per_neighbourhood, on='neighbourhood')
        metrics_df = metrics_df.merge(total_listings_per_neighbourhood, on='neighbourhood')
        self.df = df.merge(metrics_df, on='neighbourhood', how='left')
        logger.info("COmpleted data transformation")
        self.next(self.load_to_db)
        
    
    @step
    def load_to_db(self):
        """
        Load the transformed data back into PostgreSQL.
        """
        logger.info("Starting to load transformed data")
        db_url = 'postgresql://postgres@localhost:5432/airbnb'
        engine = create_engine(db_url)
        self.df.to_sql('transforemed_listings', engine, index=False, if_exists='replace')
        logger.info("Done loading transformed data")
        self.next(self.end)
        
    
    @step
    def end(self):
        """
        End step: Finalize the workflow.
        """
        logger.info("ETL workflow completed successfully.")

if __name__ == '__main__':
    AirbnbETLFlow()

