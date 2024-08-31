import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import inspect
from sqlalchemy import create_engine
from sqlalchemy import text
import yaml
from urllib.parse import quote_plus
from datetime import datetime 

random.seed(100)


class AWSDBConnector:

    def __init__(self, yaml_file):
        self.read_db_creds(yaml_file)

    def read_db_creds (self,yaml_file):
            
        with open(yaml_file, 'r') as credentials:
            data_loaded = yaml.safe_load(credentials)
            self.host= data_loaded['HOST']
            self.user = data_loaded['USER']
            self.password = quote_plus(data_loaded['PASSWORD'])
            self.database = data_loaded['DATABASE']
            self.port = int(data_loaded['PORT'])
            
            print("Credentials loaded sucessfully!!")
            return yaml_file
            
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?charset=utf8mb4")
        return engine
            
    def list_db_tables(self, engine):
        inspector = inspect(engine)
        tables =inspector.get_table_names()
        print("Tables in the database:", tables)
        return tables
    
    def run_infinite_post_data_loop_method(self,engine):
        while True:
            sleep(random.randrange(0, 2))
            random_row = random.randint(0, 11000)

            with engine.connect() as connection:
                pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
                pin_selected_row = connection.execute(pin_string)
                pin_result = dict(pin_selected_row.fetchone()._mapping)
                    
                for row in pin_selected_row:
                    pin_result = dict(row._mapping)

                geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
                geo_selected_row = connection.execute(geo_string)
                geo_result = dict(geo_selected_row.fetchone()._mapping)
                    
                for row in geo_selected_row:
                    geo_result = dict(row._mapping)

                user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
                user_selected_row = connection.execute(user_string)
                user_result = dict(user_selected_row.fetchone()._mapping)
                    
                for row in user_selected_row:
                    user_result = dict(row._mapping)
                    
                print(pin_result)
                print(geo_result)
                print(user_result)

                invoke_url = "https://pmwzvsclr6.execute-api.us-east-1.amazonaws.com/dev/topics"

                def json_serial(obj):
                    if isinstance(obj, datetime):
                        return obj.isoformat()
                    raise TypeError(f"Type {obj.__class__.__name__} not serializable")

                pin_payload = json.dumps({"records": [{"value": pin_result}]}, default=json_serial)
                geo_payload = json.dumps({"records": [{"value": geo_result}]}, default=json_serial)
                user_payload = json.dumps({"records": [{"value": user_result}]}, default=json_serial)

                pin_response = requests.post(f"{invoke_url}/0afff86fcd1b.pin", headers={'Content-Type': 'application/vnd.kafka.json.v2+json'}, data=pin_payload)
                geo_response = requests.post(f"{invoke_url}/0afff86fcd1b.geo", headers={'Content-Type': 'application/vnd.kafka.json.v2+json'}, data=geo_payload)
                user_response = requests.post(f"{invoke_url}/0afff86fcd1b.user", headers={'Content-Type': 'application/vnd.kafka.json.v2+json'}, data=user_payload)

                print("Pin response status:", pin_response.status_code)
                print("Geo response status:", geo_response.status_code)
                print("User response status:", user_response.status_code)



#new_connector = AWSDBConnector() 
def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
                
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
                
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
                
            for row in user_selected_row:
                user_result = dict(row._mapping)
                
            print(pin_result)
            print(geo_result)
            print(user_result)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
        
        


