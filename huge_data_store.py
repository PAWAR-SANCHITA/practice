from azure.cosmos import CosmosClient, PartitionKey
import os
from dotenv import load_dotenv
from uuid import uuid4
import json

# for unit testing we have to make somechanges in this code for unit testing unit_testing.py
# we are converting this code in def functions to make this code compact.
# Replace these with your Azure Cosmos DB credentials.


def load_config():
    load_dotenv('e:/vyas internship/tasks/practice/config.env')
    return {
        "cosmos_connection_string" : os.getenv("AZURE_COSMOS_CONNECTION_STRING"),
        "database_name" : os.getenv("AZURE_COSMOS_DATABASE_NAME"),
        "container_name_cosmos" : os.getenv("AZURE_COSMOS_CONTAINER_NAME"),
        "second_container_name_cosmos" : os.getenv("AZURE_COSMOS_SECOND_CONTAINER_NAME"),
       
    }
        # Initialization 

def initialize_clients(config):
    cosmos_client = CosmosClient.from_connection_string(config["cosmos_connection_string"])
    database = cosmos_client.get_database_client(config["database_name"])
    return {
        "container_cosmos": database.get_container_client(config["container_name_cosmos"]),
        "second_container_cosmos": database.get_container_client(config["second_container_name_cosmos"]),
    }
    

# Example data: Different 'CustomerID' for each item
items = [
    '''{"id":"","ods_date":"2024-10-01T00:00:00Z","data_isdeleted":"No","detail_type":"P","detail_data":{"rec_type":"P","project_id":"APS134001","device_id":"R1387892-1WP","project_status_das":"onboarded","project_active_today":"yes","project_calls_today":27,"project_errors_today":3}}''',
    '''{"id":"","ods_date":"2024-10-01T00:00:00Z","data_isdeleted":"No","detail_type":"P","detail_data":{"rec_type":"P","project_id":"APS160001","device_id":"R1387893-1WP","project_status_das":"onboarded","project_active_today":"yes","project_calls_today":10,"project_errors_today":0}}''',
    '''{"id":"","ods_date":"2024-10-01T00:00:00Z","data_isdeleted":"No","detail_type":"D","detail_data":{"rec_type":"D","project_id":"APS134001","device_id":"R1531768-1WP","device_active_today":"yes","device_calls_today":27,"device_errors_today":3}}''',
    '''{"id":"","ods_date":"2024-10-01T00:00:00Z","data_isdeleted":"No","detail_type":"D","detail_data":{"rec_type":"D","project_id":"APS160001","device_id":"R1387891-1WP","device_active_today":"yes","device_calls_today":10,"device_errors_today":0}}''',
    '''{"id":"","ods_date":"2024-10-01T00:00:00Z","data_isdeleted":"No","detail_type":"P","detail_data":{"rec_type":"P","project_id":"APS302001","device_id":"R1387894-1WP","project_status_das":"onboarded","project_active_today":"yes","project_calls_today":4,"project_errors_today":0}}''',
    ]

def insert_items_into_cosmos(items, clients):
    for single_item in items:
        try:
            # Insert items one by one
            item = json.loads(single_item)

            # to provide unique id value we use uuid library
            item['id'] = str(uuid4())

            # Query to check if an item with the same 'CustomerID' already exists

            query = f"SELECT * FROM c WHERE c.ods_date = '{item['ods_date']}' OR c.detail_type = '{item['detail_type']}' AND c.detail_data.project_id = '{item['detail_data']['project_id']}' AND c.detail_data.device_id = '{item['detail_data']['device_id']}'"

            existing_items = list(clients.query_items(query=query, enable_cross_partition_query=True))
        
            #print(f"Found {len(existing_items)} existing items for this query")

            if not existing_items:
                clients["container_cosmos"].upsert_item(body=item)  # Insert the item
                print(f"Item with ods_date '{item['ods_date']}' and detail_type '{item['detail_type']}' added successfully")
                print("primary")
                

            else:
                clients["second_container_cosmos"].upsert_item(body=item)  # Insert the item
                print(f"Item with ods_date '{item['ods_date']}' and detail_type '{item['detail_type']}' added successfully")           
                print("secondary")

        
        except Exception as e:
            print(f"error blob {single_item}")
            print(f"Error inserting item with ods_date '{item['ods_date']}': {e}")
        






