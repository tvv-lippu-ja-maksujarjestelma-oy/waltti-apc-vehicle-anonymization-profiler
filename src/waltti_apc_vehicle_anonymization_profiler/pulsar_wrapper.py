"""Apache Pulsar functions."""
import json

import pulsar


def create_client(logger, client_config, oauth2_config):
    combined_config = client_config | {
        "authentication": pulsar.AuthenticationOauth2(
            json.dumps(oauth2_config)
        ),
        "logger": logger,
    }
    return pulsar.Client(**combined_config)


def create_producer(client, producer_config):
    return client.create_producer(**producer_config)


def create_consumer(client, consumer_config):
    return client.subscribe(**consumer_config)


def create_reader(client, reader_config):
    return client.create_reader(**reader_config)


def create_readers(client, readers_config):
    return {k: create_reader(client, v) for k, v in readers_config.items()}
