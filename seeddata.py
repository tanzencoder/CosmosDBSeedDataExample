import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey
import datetime
import os
import json

import config

HOST = config.settings['host']
MASTER_KEY = config.settings['master_key']
DATABASE_ID = config.settings['database_id']
CONTAINER_ID = config.settings['container_id']


def upsert_config_item(container, config_doc):
    """Upserts config item to the appropriate container"""
    try:
        response = container.upsert_item(body=config_doc)
        return response
    
    except exceptions.CosmosHttpResponseError:
        print('\nupsert_item caught an error upserting item {0}'.format(config_doc['id']))


def get_cosmos_client():
    """Gets Cosmos client to connect to the database"""
    return cosmos_client.CosmosClient(HOST, {'masterKey': MASTER_KEY})


def get_cosmos_database(client):
    try:
        db = client.create_database_if_not_exists(id = DATABASE_ID)
        return db

    except exceptions.CosmosHttpResponseError:
        pass


def get_cosmos_container(database, container_id):
    try:
        container = database.create_container_if_not_exists(id = container_id, partition_key=PartitionKey(path='/id', kind='Hash'))
        return container

    except exceptions.CosmosHttpResponseError:
        print('Container with id \'{0}\' was found'.format(container_id))


def process_config_docs():
    client = get_cosmos_client()

    try:
        # setup database for this sample
        db = get_cosmos_database(client)

        config_data_dir = os.scandir('ConfigData')

        for entry in config_data_dir:
            if entry.is_dir():
                print("Iterating files in '{0}' directory.".format(entry.name))
                container = get_cosmos_container(db, entry.name)
                current_dir = os.scandir(entry.path)

                for file_entry in current_dir:
                    if file_entry.is_file:
                        print("Processing file '{0}'". format(file_entry.name))
                        with open(file_entry.path) as json_file:
                            data = json.load(json_file)
                            response = upsert_config_item(container, data)
                            print('Upserted Item\'s Id is {0}'.format(response['id']))

                current_dir.close()
                
        config_data_dir.close()

    except exceptions.CosmosHttpResponseError as e:
        print('\nprocess_config_docs has caught an error. {0}'.format(e.message))

    finally:
        print("\nprocess_config_docs done")


if __name__ == '__main__':
    process_config_docs()