
import requests
from time import sleep
import random
import boto3
import json
import sqlalchemy
from sqlalchemy import create_engine, text
import yaml
from urllib.parse import quote_plus
from datetime import datetime

# Seed for reproducibility
random.seed(100)

class AWSDBConnector:
    def __init__(self, yaml_file):
        self.stream_read_db_creds(yaml_file)

    def stream_read_db_creds(self, yaml_file):
        with open(yaml_file, 'r') as credentials:
            data_loaded = yaml.safe_load(credentials)
            self.host = data_loaded['HOST']
            self.user = data_loaded['USER']
            self.password = quote_plus(data_loaded['PASSWORD'])
            self.database = data_loaded['DATABASE']
            self.port = int(data_loaded['PORT'])

            print("Credentials loaded successfully!!")
            return yaml_file

    def stream_create_db_connector(self):
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?charset=utf8mb4")
        return engine

    def stream_list_db_tables(self, engine):
        inspector = sqlalchemy.inspect(engine)
        tables = inspector.get_table_names()
        print("Tables in the database:", tables)
        return tables
    

    def stream_run_infinite_post_data_loop_method(self, engine):
        streams = {
            'pin': 'streaming-0afff86fcd1b-pin',
            'geo': 'streaming-0afff86fcd1b-geo',
            'user': 'streaming-0afff86fcd1b-user'
        }

        def stream_json_serial(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Type {obj.__class__.__name__} not serializable")

        def stream_send_data_to_stream(stream_name, data):
            base_invoke_url = "https://pmwzvsclr6.execute-api.us-east-1.amazonaws.com/dev/streams"
            invoke_url = f"{base_invoke_url}/{stream_name}/record"
            payload = json.dumps({
                "StreamName": stream_name,
                "Data": data,
                "PartitionKey": "desired-name"
            }, default=stream_json_serial)

            headers = {'Content-Type': 'application/json'}
            response = requests.put(invoke_url, headers=headers, data=payload)

            print(f"Response status for {stream_name}: {response.status_code}")

        while True:
            sleep(random.randrange(0, 2))
            random_row = random.randint(0, 11000)

            with engine.connect() as connection:
                # Fetch Pinterest data
                pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
                pin_selected_row = connection.execute(pin_string)
                pin_result = dict(pin_selected_row.fetchone()._mapping)

                # Fetch Geolocation data
                geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
                geo_selected_row = connection.execute(geo_string)
                geo_result = dict(geo_selected_row.fetchone()._mapping)

                # Fetch User data
                user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
                user_selected_row = connection.execute(user_string)
                user_result = dict(user_selected_row.fetchone()._mapping)

                # Send data to the respective streams
                stream_send_data_to_stream(streams['pin'], pin_result)
                stream_send_data_to_stream(streams['geo'], geo_result)
                stream_send_data_to_stream(streams['user'], user_result)

                print("Pin data:", pin_result)
                print("Geo data:", geo_result)
                print("User data:", user_result)
if __name__ == "__main__":
    # Initialize connector and database engine
    stream_connector_rds = AWSDBConnector(yaml_file='db_creds.yaml')
    stream_engine_rds = stream_connector_rds.stream_create_db_connector()

    # List all tables in the database (optional)
    stream_connector_rds.stream_list_db_tables(stream_engine_rds)

    # Start sending data to streams
    stream_run_infinite_post_data_loop_method(stream_engine_rds)