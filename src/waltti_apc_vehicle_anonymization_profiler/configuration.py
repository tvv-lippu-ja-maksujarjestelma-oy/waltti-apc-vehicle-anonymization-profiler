"""Configuration management."""

import json
import json.decoder
import os

import pulsar


def get_optional_int_with_default(env_var, default):
    value = default
    string = os.getenv(env_var)
    if string is not None:
        try:
            value = int(string)
        except ValueError as err:
            msg = (
                f"If given, the environment variable {env_var} must be an"
                f" integer. Instead, this was given: {string}"
            )
            raise ValueError(msg) from err
    return value


def get_optional_bool_with_default(env_var, default):
    value = default
    string = os.getenv(env_var)
    if string is not None:
        is_true = string.lower() == "true"
        is_false = string.lower() == "false"
        if not is_true and not is_false:
            msg = (
                f"If given, the environment variable {env_var} must be a"
                " boolean, either true or false. Instead, this was given:"
                f" {string}"
            )
            raise ValueError(msg)
        value = is_true
    return value


def get_string(env_var):
    string = os.getenv(env_var)
    if string is None:
        msg = f"The environment variable {env_var} must be given"
        raise ValueError(msg)
    return string


def get_optional_string_with_default(env_var, default):
    string = os.getenv(env_var)
    if string is None:
        string = default
    return string


def get_health_check_port(env_var):
    port = get_optional_int_with_default(env_var, 8080)
    if port is not None and (port < 1 or port > 65535):
        msg = (
            f"If given, the environment variable {env_var} must be a port"
            " number in the inclusive range [1, 65535]"
        )
        raise ValueError(msg)
    return port


def get_pulsar_compression_type(env_var, default):
    string = os.getenv(env_var)
    if string is None:
        return default
    if string == "NONE":
        return pulsar.CompressionType.NONE
    if string == "LZ4":
        return pulsar.CompressionType.LZ4
    if string == "ZLib":
        return pulsar.CompressionType.ZLib
    if string == "ZSTD":
        return pulsar.CompressionType.ZSTD
    if string == "SNAPPY":
        return pulsar.CompressionType.SNAPPY
    msg = (
        f"If given, the environment variable {env_var} must be set to either"
        ' "NONE", "LZ4", "ZLib", "ZSTD" or "SNAPPY". Instead, this was given:'
        f" {string}"
    )
    raise ValueError(msg)


def get_pulsar_catalogue_readers(env_var):
    string = os.getenv(env_var)
    if string is None:
        msg = f"The environment variable {env_var} is required"
        raise ValueError(msg)
    try:
        lists = json.loads(string)
        return {
            reader_spec["feedPublisherId"]: {
                "topic": reader_spec["topic"],
                "start_message_id": pulsar.MessageId.earliest,
                "reader_name": reader_spec["name"],
            }
            for reader_spec in lists
        }
    except Exception as err:
        msg = (
            f"The environment variable {env_var} must be a JSON stringified"
            ' list of objects where each object has keys "feedPublisherId",'
            f' "topic" and "name". Instead, this was given: {string}'
        )
        raise ValueError(msg) from err


def read_configuration():
    health_check_port = get_health_check_port("HEALTH_CHECK_PORT")
    is_fresh_start = get_optional_bool_with_default("IS_FRESH_START", False)
    pulsar_block_if_queue_full = get_optional_bool_with_default(
        "PULSAR_BLOCK_IF_QUEUE_FULL", True
    )
    pulsar_cache_reader_name = get_string("PULSAR_CACHE_READER_NAME")
    pulsar_catalogue_readers = get_pulsar_catalogue_readers(
        "PULSAR_CATALOGUE_READERS"
    )
    pulsar_compression_type = get_pulsar_compression_type(
        "PULSAR_COMPRESSION_TYPE", pulsar.CompressionType.ZSTD
    )
    pulsar_oauth2_audience = get_string("PULSAR_OAUTH2_AUDIENCE")
    pulsar_oauth2_issuer_url = get_string("PULSAR_OAUTH2_ISSUER_URL")
    pulsar_oauth2_private_key = get_string("PULSAR_OAUTH2_KEY_PATH")
    pulsar_producer_topic = get_string("PULSAR_PRODUCER_TOPIC")
    pulsar_service_url = get_string("PULSAR_SERVICE_URL")
    pulsar_tls_validate_hostname = get_optional_bool_with_default(
        "PULSAR_TLS_VALIDATE_HOSTNAME", True
    )
    return {
        "health_check": {
            "port": health_check_port,
        },
        "processing": {
            "is_fresh_start": is_fresh_start,
        },
        "pulsar": {
            "oauth2": {
                "audience": pulsar_oauth2_audience,
                "issuer_url": pulsar_oauth2_issuer_url,
                "private_key": pulsar_oauth2_private_key,
            },
            "client": {
                "service_url": pulsar_service_url,
                "use_tls": True,
                "tls_validate_hostname": pulsar_tls_validate_hostname,
            },
            "producer": {
                "topic": pulsar_producer_topic,
                "compression_type": pulsar_compression_type,
                "block_if_queue_full": pulsar_block_if_queue_full,
            },
            "cache_reader": {
                "topic": pulsar_producer_topic,
                "start_message_id": pulsar.MessageId.earliest,
                "reader_name": pulsar_cache_reader_name,
            },
            "catalogue_readers": pulsar_catalogue_readers,
        },
    }
