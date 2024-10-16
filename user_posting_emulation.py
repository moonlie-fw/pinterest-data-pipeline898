import requests
import boto3
import random
import json
import sqlalchemy
import yaml

from time import sleep
from sqlalchemy import inspect, create_engine, text
from urllib.parse import quote_plus
from datetime import datetime

random.seed(100)

class AWSDBConnector:
    """
    A class to handle AWS RDS MySQL database connection, data fetching, and sending the fetched data 
    to an external API in JSON format.

    Attributes:
    ----------
    host : str
        The host address for the database.
    user : str
        The database user name.
    password : str
        The encoded password for the database.
    database : str
        The database name.
    port : int
        The port number to connect to the database.
    """

    def __init__(self, yaml_file: str):
        """
        Initializes the connector by loading the database credentials from a YAML file.

        Parameters:
        ----------
        yaml_file : str
            The path to the YAML file containing the database credentials.
        """
        self.read_db_creds(yaml_file)

    def read_db_creds(self, yaml_file: str) -> dict:
        """
        Reads the database credentials from a YAML file and stores them as instance variables.

        Parameters:
        ----------
        yaml_file : str
            The path to the YAML file containing the database credentials.

        Returns:
        -------
        dict
            The loaded YAML data.
        """
        with open(yaml_file, 'r') as credentials:
            data_loaded = yaml.safe_load(credentials)
            self.host = data_loaded['HOST']
            self.user = data_loaded['USER']
            self.password = quote_plus(data_loaded['PASSWORD'])
            self.database = data_loaded['DATABASE']
            self.port = int(data_loaded['PORT'])

            print("Credentials loaded successfully!!")
            return data_loaded

    def create_db_connector(self):
        """
        Creates and returns a SQLAlchemy engine to connect to the MySQL database.

        Returns:
        -------
        sqlalchemy.engine.Engine
            SQLAlchemy engine connected to the database.
        """
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?charset=utf8mb4")
        return engine

    def list_db_tables(self, engine):
        """
        Lists all tables in the connected database.

        Parameters:
        ----------
        engine : sqlalchemy.engine.Engine
            The SQLAlchemy engine connected to the database.

        Returns:
        -------
        list
            A list of table names in the database.
        """
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        print("Tables in the database:", tables)
        return tables

    def run_infinite_post_data_loop_method(self, engine):
        """
        Continuously fetches random data from the 'pinterest_data', 'geolocation_data', 
        and 'user_data' tables and sends the data to the external API in JSON format.

        Parameters:
        ----------
        engine : sqlalchemy.engine.Engine
            The SQLAlchemy engine connected to the database.
        """
        while True:
            sleep(random.randrange(0, 2))
            random_row = random.randint(0, 11000)

            with engine.connect() as connection:
                # Fetching data from the database tables
                pin_result = self.fetch_row_data(connection, 'pinterest_data', random_row)
                geo_result = self.fetch_row_data(connection, 'geolocation_data', random_row)
                user_result = self.fetch_row_data(connection, 'user_data', random_row)

                print(pin_result)
                print(geo_result)
                print(user_result)

                # Endpoint URL (should be moved to a config/env file for better maintainability)
                invoke_url = "https://pmwzvsclr6.execute-api.us-east-1.amazonaws.com/dev/topics"

                # Send data to API for each table
                self.send_data_to_api(f"{invoke_url}/0afff86fcd1b.pin", pin_result)
                self.send_data_to_api(f"{invoke_url}/0afff86fcd1b.geo", geo_result)
                self.send_data_to_api(f"{invoke_url}/0afff86fcd1b.user", user_result)

    def fetch_row_data(self, connection, table_name: str, random_row: int) -> dict:
        """
        Fetches a single row of data from a specified table in the database.

        Parameters:
        ----------
        connection : sqlalchemy.engine.Connection
            The SQLAlchemy connection to the database.
        table_name : str
            The name of the table to fetch data from.
        random_row : int
            The row number to randomly select data from.

        Returns:
        -------
        dict
            The selected row as a dictionary.
        """
        query = text(f"SELECT * FROM {table_name} LIMIT {random_row}, 1")
        selected_row = connection.execute(query).fetchone()
        return dict(selected_row._mapping) if selected_row else {}

    def send_data_to_api(self, url: str, data: dict):
        """
        Sends a JSON payload to the specified API endpoint.

        Parameters:
        ----------
        url : str
            The API endpoint URL.
        data : dict
            The data to send in the request payload.
        """
        payload = json.dumps({"records": [{"value": data}]}, default=self.json_serial)
        response = requests.post(url, headers={'Content-Type': 'application/vnd.kafka.json.v2+json'}, data=payload)
        print(f"Response status for {url.split('/')[-1]}:", response.status_code)

    @staticmethod
    def json_serial(obj):
        """
        Serializes datetime objects to ISO format for JSON payloads.

        Parameters:
        ----------
        obj : Any
            The object to serialize.

        Returns:
        -------
        str
            The ISO formatted string for datetime objects.
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {obj.__class__.__name__} not serializable")


if __name__ == "__main__":
    yaml_file = 'db_creds.yaml'  # Replace with your actual path to the YAML file
    connector = AWSDBConnector(yaml_file)
    engine = connector.create_db_connector()
    connector.run_infinite_post_data_loop_method(engine)
    print('Working')
