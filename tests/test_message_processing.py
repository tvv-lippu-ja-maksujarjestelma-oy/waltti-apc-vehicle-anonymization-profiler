import json
import logging
import pathlib

import pytest
from waltti_apc_vehicle_anonymization_profiler import message_processing


def test_split_model_string_to_tuple():
    model = "123-00"
    expected_output = (123, 0)
    assert (
        message_processing.split_model_string_to_tuple(model)
        == expected_output
    )


def test_combine_model_tuple_to_string():
    model = (123, 0)
    expected_output = "123-0"
    assert (
        message_processing.combine_model_tuple_to_string(model)
        == expected_output
    )


def test_transform_capacity_to_minimum_counts_zero_capacity():
    seating_capacity = 0
    standing_capacity = 0
    output = message_processing.transform_capacity_to_minimum_counts(
        seating_capacity, standing_capacity
    )
    expected_output = {
        "EMPTY": 0,
        "MANY_SEATS_AVAILABLE": 0,
        "FEW_SEATS_AVAILABLE": 0,
        "STANDING_ROOM_ONLY": 0,
        "CRUSHED_STANDING_ROOM_ONLY": 0,
        "FULL": 0,
    }
    assert output == expected_output


def test_transform_capacity_to_minimum_counts_one_seating():
    seating_capacity = 1
    standing_capacity = 0
    output = message_processing.transform_capacity_to_minimum_counts(
        seating_capacity, standing_capacity
    )
    expected_output = {
        "EMPTY": 0,
        "MANY_SEATS_AVAILABLE": 0,
        "FEW_SEATS_AVAILABLE": 1,
        "STANDING_ROOM_ONLY": 1,
        "CRUSHED_STANDING_ROOM_ONLY": 1,
        "FULL": 1,
    }
    assert output == expected_output


def test_transform_capacity_to_minimum_counts_one_standing():
    seating_capacity = 0
    standing_capacity = 1
    output = message_processing.transform_capacity_to_minimum_counts(
        seating_capacity, standing_capacity
    )
    expected_output = {
        "EMPTY": 0,
        "MANY_SEATS_AVAILABLE": 0,
        "FEW_SEATS_AVAILABLE": 0,
        "STANDING_ROOM_ONLY": 0,
        "CRUSHED_STANDING_ROOM_ONLY": 0,
        "FULL": 1,
    }
    assert output == expected_output


def test_transform_capacity_to_minimum_counts_five_and_five():
    seating_capacity = 5
    standing_capacity = 5
    output = message_processing.transform_capacity_to_minimum_counts(
        seating_capacity, standing_capacity
    )
    expected_output = {
        "EMPTY": 0,
        "MANY_SEATS_AVAILABLE": 1,
        "FEW_SEATS_AVAILABLE": 4,
        "STANDING_ROOM_ONLY": 5,
        "CRUSHED_STANDING_ROOM_ONLY": 7,
        "FULL": 9,
    }
    assert output == expected_output


@pytest.fixture()
def logger():
    logging.basicConfig(
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG,
        format=(
            "%(asctime)s.%(msecs)03d %(levelname)s %(module)s -"
            " %(funcName)s: %(message)s"
        ),
    )
    return logging.getLogger()


@pytest.fixture()
def catalogue_message_kuopio(mocker):
    message = mocker.MagicMock()
    data = [
        {
            "operatorId": "44517",
            "vehicleShortName": "1",
            "vehicleRegistrationNumber": "ILL-607",
            "standingCapacity": 77,
            "seatingCapacity": 49,
            "equipment": [{"type": "LOCATION_PRODUCER", "id": "130716"}],
        },
        {
            "operatorId": "44517",
            "vehicleShortName": "6",
            "vehicleRegistrationNumber": "ILL-602",
            "standingCapacity": 77,
            "seatingCapacity": 49,
            "equipment": [
                {"type": "LOCATION_PRODUCER", "id": "131185"},
                {
                    "type": "PASSENGER_COUNTER",
                    "id": "DL-Mccl1",
                    "apcSystem": "DEAL_COMP",
                },
                {
                    "type": "PASSENGER_COUNTER",
                    "id": "emblica-sc02-apc-device1",
                    "apcSystem": "EMBLICA",
                },
                {
                    "type": "PASSENGER_COUNTER",
                    "id": "KL006-APC",
                    "apcSystem": "TELIA",
                },
            ],
        },
        {
            "operatorId": "44517",
            "vehicleShortName": "160",
            "vehicleRegistrationNumber": "JLJ-160",
            "standingCapacity": 38,
            "seatingCapacity": 39,
            "equipment": [
                {"type": "LOCATION_PRODUCER", "id": "131176"},
                {
                    "type": "PASSENGER_COUNTER",
                    "id": "emblica-sc01-apc-device1",
                    "apcSystem": "EMBLICA",
                },
                {
                    "type": "PASSENGER_COUNTER",
                    "id": "KL160-APC",
                    "apcSystem": "TELIA",
                },
            ],
        },
        {
            "operatorId": "44517",
            "vehicleShortName": "44",
            "vehicleRegistrationNumber": "ERV-344",
            "seatingCapacity": 59,
            "equipment": [{"type": "LOCATION_PRODUCER", "id": "130951"}],
        },
    ]
    message.data.return_value = json.dumps(data).encode("utf-8")
    message.event_timestamp.return_value = 123
    message.topic_name.return_value = (
        "persistent://public/default/catalogue-fi-kuopio"
    )
    return message


@pytest.fixture()
def catalogue_message_jyvaskyla(mocker):
    message = mocker.MagicMock()
    data = [
        {
            "operatorId": "6714",
            "vehicleShortName": "123",
            "vehicleRegistrationNumber": "FOO-000",
            "standingCapacity": 1,
            "seatingCapacity": 2,
            "equipment": [{"type": "LOCATION_PRODUCER", "id": "NIX"}],
        },
        {
            "operatorId": "6714",
            "vehicleShortName": "518",
            "vehicleRegistrationNumber": "BAR-100",
            "standingCapacity": 68,
            "seatingCapacity": 49,
            "equipment": [
                {
                    "type": "PASSENGER_COUNTER",
                    "id": "emblica-sc04-apc-device1",
                    "apcSystem": "EMBLICA",
                },
                {
                    "type": "PASSENGER_COUNTER",
                    "id": "JL518-APC",
                    "apcSystem": "TELIA",
                },
            ],
        },
        {
            "operatorId": "6714",
            "vehicleShortName": "521",
            "vehicleRegistrationNumber": "BAZ-200",
            "standingCapacity": 68,
            "seatingCapacity": 49,
            "equipment": [
                {
                    "type": "PASSENGER_COUNTER",
                    "id": "emblica-sc03-apc-device1",
                    "apcSystem": "EMBLICA",
                },
                {
                    "type": "PASSENGER_COUNTER",
                    "id": "JL521-APC",
                    "apcSystem": "TELIA",
                },
            ],
        },
    ]
    message.data.return_value = json.dumps(data).encode("utf-8")
    message.event_timestamp.return_value = 234
    message.topic_name.return_value = (
        "persistent://public/default/catalogue-fi-jyvaskyla"
    )
    return message


def test_get_latest_vehicles_to_tuple_models(
    logger, catalogue_message_kuopio, catalogue_message_jyvaskyla
):
    messages = {
        "fi:jyvaskyla": catalogue_message_jyvaskyla,
        "fi:kuopio": catalogue_message_kuopio,
    }
    expected_output = {
        "fi:jyvaskyla:6714_518": (49, 68),
        "fi:jyvaskyla:6714_521": (49, 68),
        "fi:kuopio:44517_160": (39, 38),
        "fi:kuopio:44517_6": (49, 77),
    }
    output = message_processing.get_latest_vehicles_to_tuple_models(
        logger, messages
    )
    assert output == expected_output


def test_process_messages(
    mocker, logger, catalogue_message_kuopio, catalogue_message_jyvaskyla
):
    def add_csv_files(config):
        tmp_dir = config["outputDirectory"]
        tmp_path = pathlib.Path(tmp_dir)
        for vm in config["vehicleModels"]:
            for csv_filename in vm["outputFilenames"]:
                csv_path = tmp_path / csv_filename
                with pathlib.Path.open(csv_path, "w") as f:
                    f.write("foo")

    mocker.patch(
        "waltti_apc_vehicle_anonymization_profiler.message_processing.hyperparameter_optimization.run_inference_for_all_vehicle_models",
        side_effect=add_csv_files,
    )

    catalogue_reader_kuopio = mocker.MagicMock()
    catalogue_reader_kuopio.has_message_available.side_effect = [
        True,
        False,
    ]
    catalogue_reader_kuopio.read_next.return_value = catalogue_message_kuopio

    catalogue_reader_jyvaskyla = mocker.MagicMock()
    catalogue_reader_jyvaskyla.has_message_available.side_effect = [
        True,
        False,
    ]
    catalogue_reader_jyvaskyla.read_next.return_value = (
        catalogue_message_jyvaskyla
    )
    catalogue_readers = {
        "fi:jyvaskyla": catalogue_reader_jyvaskyla,
        "fi:kuopio": catalogue_reader_kuopio,
    }
    cache_reader = mocker.MagicMock()
    cache_reader.has_message_available.return_value = False
    resources = {
        "pulsar_cache_reader": cache_reader,
        "pulsar_catalogue_readers": catalogue_readers,
    }
    processing_config = {"is_fresh_start": False}
    pulsar_config = {"client": {}, "oauth2": {}, "producer": {}}
    mocker.patch(
        "waltti_apc_vehicle_anonymization_profiler.message_processing.pulsar_wrapper.create_client"
    )
    producer_mock = mocker.MagicMock()
    mocker.patch(
        "waltti_apc_vehicle_anonymization_profiler.message_processing.pulsar_wrapper.create_producer",
        return_value=producer_mock,
    )
    mocker.patch(
        "waltti_apc_vehicle_anonymization_profiler.message_processing.graceful_exit.close_pulsar"
    )
    expected_producer_message_data = {
        "schemaVersion": "1-0-0",
        "vehicleModels": {
            "fi:jyvaskyla:6714_518": "49-68",
            "fi:jyvaskyla:6714_521": "49-68",
            "fi:kuopio:44517_160": "39-38",
            "fi:kuopio:44517_6": "49-77",
        },
        "modelProfiles": {
            "39-38": "foo",
            "49-68": "foo",
            "49-77": "foo",
        },
    }
    expected_producer_message_data_encoded = json.dumps(
        expected_producer_message_data
    ).encode("utf-8")
    message_processing.process_messages(
        logger, processing_config, pulsar_config, resources
    )
    producer_mock.send.assert_called_with(
        expected_producer_message_data_encoded, event_timestamp=123
    )
