"""Process messages and handle the business logic."""

import json
import pathlib
import tempfile
import time
import traceback

import apc_anonymizer.configuration
import jsonschema
from apc_anonymizer.mechanisms.simple import hyperparameter_optimization

from waltti_apc_vehicle_anonymization_profiler import (
    graceful_exit,
    pulsar_wrapper,
    validators,
)


def get_latest_message(reader):
    message = None
    while reader.has_message_available():
        message = reader.read_next()
    return message


def validate_and_return_message_data(logger, validator, message):
    result = None
    try:
        message_data = message.data()
        to_be_validated = json.loads(message_data)
        validator.validate(to_be_validated)
        result = to_be_validated
    except json.JSONDecodeError as err:
        logger.error(
            "The Pulsar message data is not valid JSON",
            extra={
                "json_fields": {
                    "err": traceback.format_exception(err),
                    "messageDataString": message_data.decode(
                        encoding="utf-8", errors="replace"
                    ),
                    "properties": message.properties(),
                    "messageEventTimestamp": message.event_timestamp(),
                }
            },
        )
    except jsonschema.ValidationError as err:
        logger.error(
            "The Pulsar message data does not validate with the given schema"
            " validator",
            extra={
                "json_fields": {
                    "err": traceback.format_exception(err),
                    "messageData": to_be_validated,
                    "properties": message.properties(),
                    "messageEventTimestamp": message.event_timestamp(),
                }
            },
        )
    except Exception as err:
        logger.critical(
            "Something went wrong while trying to validate the Pulsar message"
            " data",
            extra={
                "json_fields": {
                    "err": traceback.format_exception(err),
                    "pulsarMessage": message,
                }
            },
        )
    return result


def split_model_string_to_tuple(model_string):
    return tuple(map(int, model_string.split(sep="-", maxsplit=1)))


def combine_model_tuple_to_string(model_tuple):
    return "-".join(map(str, model_tuple))


def build_cache(logger, message):
    validator = validators.get_profile_collection_validator()
    vehicle_profiles = validate_and_return_message_data(
        logger, validator, message
    )
    return vehicle_profiles["modelProfiles"]


def get_vehicle_string(vehicle):
    return vehicle["operatorId"] + "_" + vehicle["vehicleShortName"]


def validate_and_return_vehicle_apc_mapping_messages(logger, messages):
    validator = validators.get_vehicle_apc_mapping_validator()
    return {
        feed_publisher_id: validate_and_return_message_data(
            logger, validator, message
        )
        for feed_publisher_id, message in messages.items()
    }


def keep_only_vehicles_with_apc(vehicle_apc_mappings):
    return {
        feed_publisher_id: [
            vehicle
            for vehicle in vehicle_apc
            if any(
                device["type"] == "PASSENGER_COUNTER"
                for device in vehicle["equipment"]
            )
        ]
        for feed_publisher_id, vehicle_apc in vehicle_apc_mappings.items()
    }


def log_if_multiple_apc_devices(logger, all_vehicles, messages):
    all_vehicles_with_multiple_apc = {
        feed_publisher_id: sorted(
            [
                vehicle
                for vehicle in vehicles
                if sum(
                    device["type"] == "PASSENGER_COUNTER"
                    for device in vehicle["equipment"]
                )
                > 1
            ],
            key=get_vehicle_string,
        )
        for feed_publisher_id, vehicles in all_vehicles.items()
    }
    for (
        feed_publisher_id,
        vehicles_with_multiple_apc,
    ) in all_vehicles_with_multiple_apc.items():
        if len(vehicles_with_multiple_apc) > 0:
            logger.info(
                '"Found vehicles with more than one system of type'
                ' "PASSENGER_COUNTER"',
                extra={
                    "json_fields": {
                        "numberOfVehiclesWithMultipleApcSystems": len(
                            vehicles_with_multiple_apc
                        ),
                        "vehiclesWithMultipleApcSystems": (
                            vehicles_with_multiple_apc
                        ),
                        "feedPublisherId": feed_publisher_id,
                        "topic": messages[feed_publisher_id].topic_name(),
                    }
                },
            )


def get_vehicles_to_tuple_models(logger, feed_publisher_id, vehicle_catalogue):
    vehicles_to_tuple_models = {}
    for vehicle in vehicle_catalogue:
        seating_capacity = vehicle.get("seatingCapacity")
        standing_capacity = vehicle.get("standingCapacity")
        if (seating_capacity is None) or (standing_capacity is None):
            logger.error(
                "The vehicle does not have seatingCapacity or standingCapacity"
                " defined so no anonymization profile can be created for it."
                " If either value should be zero, mark it explicitly so in the"
                " vehicle registry.",
                extra={
                    "json_fields": {
                        "vehicle": vehicle,
                        "feedPublisherId": feed_publisher_id,
                    }
                },
            )
        else:
            vehicles_to_tuple_models[
                feed_publisher_id + ":" + get_vehicle_string(vehicle)
            ] = (
                seating_capacity,
                standing_capacity,
            )
    return vehicles_to_tuple_models


def extract_vehicles_to_tuple_models(logger, all_vehicles):
    return [
        get_vehicles_to_tuple_models(logger, feed_publisher_id, vehicles)
        for feed_publisher_id, vehicles in all_vehicles.items()
    ]


def merge_list_of_dicts(list_of_dicts):
    result = {}
    for d in list_of_dicts:
        result.update(d)
    return result


def get_latest_vehicles_to_tuple_models(logger, messages):
    vehicle_apc_mappings = validate_and_return_vehicle_apc_mapping_messages(
        logger, messages
    )
    vehicles_with_apc = keep_only_vehicles_with_apc(vehicle_apc_mappings)
    log_if_multiple_apc_devices(logger, vehicles_with_apc, messages)
    vehicles_to_tuple_models = extract_vehicles_to_tuple_models(
        logger, vehicles_with_apc
    )
    merged_vehicles_to_tuple_models = merge_list_of_dicts(
        vehicles_to_tuple_models
    )
    logger.debug(
        "Got latest vehicle-to-vehicle-model mappings",
        extra={
            "json_fields": {
                "vehicleApcMappingSizes": {
                    f: len(v) for f, v in vehicle_apc_mappings.items()
                },
                "vehiclesWithApcSizes": {
                    f: len(v) for f, v in vehicles_with_apc.items()
                },
                "vehicleToTupleModels": vehicles_with_apc.items(),
                "mergedVehicleToTupleModels": merged_vehicles_to_tuple_models,
            }
        },
    )
    return merged_vehicles_to_tuple_models


def transform_capacity_to_minimum_counts(seating_capacity, standing_capacity):
    """Transform vehicle capacity into minimum category counts.

    These coefficients were decided by experts from Jyväskylä and Kuopio in a
    meeting in early 2023. Any changes should be discussed in advance with and
    documented to stakeholders.
    """
    return {
        "EMPTY": 0,
        "MANY_SEATS_AVAILABLE": round(0.12 * seating_capacity),
        "FEW_SEATS_AVAILABLE": round(0.73 * seating_capacity),
        "STANDING_ROOM_ONLY": round(0.93 * seating_capacity),
        "CRUSHED_STANDING_ROOM_ONLY": round(
            0.95 * seating_capacity + 0.48 * standing_capacity
        ),
        "FULL": round(0.95 * seating_capacity + 0.83 * standing_capacity),
    }


def transform_vehicle_model_to_computation_input(model):
    return {
        # The model names form the CSV filenames from which we parse them back
        # later on.
        "outputFilename": combine_model_tuple_to_string(model) + ".csv",
        "minimumCounts": transform_capacity_to_minimum_counts(*model),
        "maximumCount": sum(model),
    }


def get_computation_configuration(tmp_dir, models):
    return {
        "configurationVersion": "1-0-0",
        "outputDirectory": str(tmp_dir),
        "vehicleModels": list(
            map(transform_vehicle_model_to_computation_input, models)
        ),
        "inference": {
            "mechanism": "simple",
        },
    }


def get_string_models_to_profiles(logger, directory, tuple_models):
    result = {}
    string_models = list(map(combine_model_tuple_to_string, tuple_models))
    dir_path = pathlib.Path(directory)
    for csv_file_path in sorted(dir_path.iterdir()):
        if not csv_file_path.is_file() or csv_file_path.suffix != ".csv":
            logger.error(
                "Something else than a CSV file was unexpectedly found in the"
                " directory",
                extra={
                    "json_fields": {
                        "directory": directory,
                        "filePath": csv_file_path,
                    }
                },
            )
        else:
            stem = csv_file_path.stem
            if stem not in string_models:
                logger.error(
                    "The CSV file does not match any model we asked for",
                    extra={
                        "json_fields": {
                            "csvFile": csv_file_path,
                            "models": string_models,
                        }
                    },
                )
            else:
                result[stem] = csv_file_path.read_text(encoding="utf-8")
    return result


def compute_new_profiles(logger, new_tuple_models):
    new_string_models_to_profiles = {}
    with tempfile.TemporaryDirectory() as tmp_dir:
        logger.debug(
            "Create a configuration for profile computation to refer"
            " to temporary directory",
            extra={
                "json_fields": {
                    "tmpDir": tmp_dir,
                }
            },
        )
        computation_configuration = get_computation_configuration(
            tmp_dir, new_tuple_models
        )
        logger.debug(
            "Validate computation configuration",
            extra={
                "json_fields": {
                    "computationConfiguration": computation_configuration,
                }
            },
        )
        computation_configuration = (
            apc_anonymizer.configuration.reinforce_configuration(
                computation_configuration
            )
        )
        logger.info(
            "Create anonymization profiles for the new vehicle models."
            " This is going to take a while."
        )
        hyperparameter_optimization.run_inference_for_all_vehicle_models(
            computation_configuration
        )
        logger.info("Computing new anonymization profiles has finished")
        new_string_models_to_profiles = get_string_models_to_profiles(
            logger, tmp_dir, new_tuple_models
        )
    return new_string_models_to_profiles


def get_needed_string_models_to_profiles(
    logger,
    new_string_models_to_profiles,
    cached_string_models_to_profiles,
    needed_tuple_models,
):
    available_string_models_to_profiles = (
        new_string_models_to_profiles | cached_string_models_to_profiles
    )
    needed_string_models = list(
        map(combine_model_tuple_to_string, needed_tuple_models)
    )
    needed_string_models_to_profiles = {
        k: v
        for k, v in available_string_models_to_profiles.items()
        if k in needed_string_models
    }
    logger.debug(
        "Figured out which vehicle profiles are needed according to the latest"
        " catalogue message",
        extra={
            "json_fields": {
                "neededStringModelsToProfiles": (
                    needed_string_models_to_profiles
                )
            }
        },
    )
    return needed_string_models_to_profiles


def form_producer_message_data(vehicles_to_models, string_models_to_profiles):
    data = {
        "schemaVersion": "1-0-0",
        "vehicleModels": vehicles_to_models,
        "modelProfiles": string_models_to_profiles,
    }
    validator = validators.get_profile_collection_validator()
    validator.validate(data)
    return json.dumps(data).encode("utf-8")


def generate_message_to_send(
    logger, cached_string_models_to_profiles, latest_messages
):
    producer_message_data = None
    min_event_timestamp = None
    logger.debug("Reformat the cached vehicle models from strings to tuples")
    cached_tuple_models_to_profiles = {
        split_model_string_to_tuple(k): v
        for k, v in cached_string_models_to_profiles.items()
    }
    logger.debug(
        "Map all vehicles from the latest catalogue messages to their vehicle"
        " models in tuple format. Keep it in one dict."
    )
    latest_vehicles_to_tuple_models = get_latest_vehicles_to_tuple_models(
        logger, latest_messages
    )
    needed_tuple_models = set(latest_vehicles_to_tuple_models.values())
    cached_tuple_models = set(cached_tuple_models_to_profiles.keys())
    logger.debug(
        "See if there are any new vehicle models",
        extra={
            "json_fields": {
                "neededTupleModels": list(
                    map(combine_model_tuple_to_string, needed_tuple_models)
                ),
                "cachedTupleModels": list(
                    map(combine_model_tuple_to_string, cached_tuple_models)
                ),
            }
        },
    )
    new_tuple_models = needed_tuple_models.difference(cached_tuple_models)
    if len(new_tuple_models) == 0:
        logger.info("No new vehicle models were found")
    else:
        logger.info(
            "New vehicle models were found",
            extra={
                "json_fields": {
                    "newVehicleModels": list(
                        map(combine_model_tuple_to_string, new_tuple_models)
                    )
                }
            },
        )
        logger.debug("Compute new anonymization profiles")
        new_string_models_to_profiles = compute_new_profiles(
            logger, new_tuple_models
        )
        logger.debug("Read the new anonymization profiles")
        needed_string_models_to_profiles = (
            get_needed_string_models_to_profiles(
                logger,
                new_string_models_to_profiles,
                cached_string_models_to_profiles,
                needed_tuple_models,
            )
        )
        latest_vehicles_to_string_models = {
            k: combine_model_tuple_to_string(v)
            for k, v in latest_vehicles_to_tuple_models.items()
        }
        logger.debug("Form message data to send")
        producer_message_data = form_producer_message_data(
            dict(sorted(latest_vehicles_to_string_models.items())),
            dict(sorted(needed_string_models_to_profiles.items())),
        )
        logger.debug("Extract event timestamp to send")
        event_timestamps = {
            feed_publisher_id: message.event_timestamp()
            for feed_publisher_id, message in latest_messages.items()
        }
        for feed_publisher_id, event_timestamp in event_timestamps.items():
            if event_timestamp is None:
                message = latest_messages[feed_publisher_id]
                logger.critical(
                    "Event timestamp must exist as we have computed new models"
                    " and that requires that a message has been received."
                    " Either we have a logic error or the message is missing"
                    " its event timestamp in the source topic.",
                    extra={
                        "json_fields": {
                            "messageDataString": message.data().decode(
                                encoding="utf-8", errors="replace"
                            ),
                            "feedPublisherId": feed_publisher_id,
                            "topic": message.topic_name(),
                            "properties": message.properties(),
                        }
                    },
                )
        nonempty_event_timestamps = {
            k: v for k, v in event_timestamps.items() if v is not None
        }
        min_event_timestamp = time.time_ns() // 1_000_000
        if len(nonempty_event_timestamps) > 0:
            min_event_timestamp = min(nonempty_event_timestamps.values())
    return producer_message_data, min_event_timestamp


def process_messages(
    logger,
    processing_config,
    pulsar_config,
    resources,
):
    cached_string_models_to_profiles = {}
    if processing_config["is_fresh_start"]:
        logger.info(
            "Skip warming up cache and create all anonymization profiles from"
            " scratch"
        )
    else:
        logger.info("Warm up cache")
        cache_reader = resources["pulsar_cache_reader"]
        latest_cache_message = get_latest_message(cache_reader)
        if latest_cache_message is None:
            logger.info(
                "While warming up the cache, we found no old profiles."
                " Hopefully this is the first time this service runs."
                " Otherwise check the retention on the Pulsar topic or the"
                " state of the vehicle catalogue upstream.",
                extra={"json_fields": {"pulsarTopic": cache_reader.topic()}},
            )
        else:
            cached_string_models_to_profiles = build_cache(
                logger, latest_cache_message
            )

    logger.info("Read latest message from each catalogue topic")
    readers = resources["pulsar_catalogue_readers"]
    latest_messages = {
        feed_publisher_id: get_latest_message(reader)
        for feed_publisher_id, reader in readers.items()
    }
    # FIXME:
    # Due to a known issue we close Pulsar before we use multiprocessing. Once
    # the issue is satisfactorily resolved, do not close and recreate
    # Pulsar resources here and leave it to the responsibility of main().
    # https://github.com/apache/pulsar-client-python/issues/127
    graceful_exit.close_pulsar(resources)
    for feed_publisher_id, latest_message in latest_messages.items():
        if latest_message is None:
            logger.critical(
                "No messages were found from the catalogue topic. Hopefully"
                " the topic will soon get written to.",
                extra={
                    "json_fields": {
                        "feedPublisherId": feed_publisher_id,
                        "topic": readers[feed_publisher_id]["topic"],
                    }
                },
            )
    latest_nonempty_messages = {
        k: v for k, v in latest_messages.items() if v is not None
    }
    if len(latest_nonempty_messages) > 0:
        logger.info(
            "At least one message was found when reading all the catalogue"
            " topics. Try to form a message if there is anything new to send."
        )
        producer_message_data, event_timestamp = generate_message_to_send(
            logger,
            cached_string_models_to_profiles,
            latest_messages,
        )
        if producer_message_data is not None and event_timestamp is not None:
            # FIXME:
            # Due to a known issue we close Pulsar before we use
            # multiprocessing. Once the issue is satisfactorily resolved, do
            # not close and recreate Pulsar resources here and leave it to the
            # responsibility of main().
            # https://github.com/apache/pulsar-client-python/issues/127
            logger.info("Create Pulsar client")
            pulsar_client = pulsar_wrapper.create_client(
                logger, pulsar_config["client"], pulsar_config["oauth2"]
            )
            resources["pulsar_client"] = pulsar_client
            logger.info("Create Pulsar producer")
            pulsar_producer = pulsar_wrapper.create_producer(
                pulsar_client, pulsar_config["producer"]
            )
            resources["pulsar_producer"] = pulsar_producer

            logger.info("Send the profiles")
            pulsar_producer.send(
                producer_message_data, event_timestamp=event_timestamp
            )
