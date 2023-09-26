import json
import logging
import os
import pathlib

import pytest
from waltti_apc_vehicle_anonymization_profiler import main


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


@pytest.fixture()
def fake_csv_string():
    return "foo"


@pytest.fixture()
def expected_producer_message_data(fake_csv_string):
    data = {
        "schemaVersion": "1-0-0",
        "vehicleModels": {
            "fi:jyvaskyla:6714_518": "49-68",
            "fi:jyvaskyla:6714_521": "49-68",
            "fi:kuopio:44517_160": "39-38",
            "fi:kuopio:44517_6": "49-77",
        },
        "modelProfiles": {
            "39-38": fake_csv_string,
            "49-68": fake_csv_string,
            "49-77": fake_csv_string,
        },
    }
    return json.dumps(data).encode("utf-8")


@pytest.fixture()
def expected_producer_message_event_timestamp():
    return 123


def test_main(
    mocker,
    catalogue_message_kuopio,
    catalogue_message_jyvaskyla,
    expected_producer_message_data,
    expected_producer_message_event_timestamp,
    fake_csv_string,
):
    def add_csv_files(config):
        tmp_dir = config["outputDirectory"]
        tmp_path = pathlib.Path(tmp_dir)
        for vm in config["vehicleModels"]:
            for csv_filename in vm["outputFilenames"]:
                csv_path = tmp_path / csv_filename
                with pathlib.Path.open(csv_path, "w") as f:
                    f.write(fake_csv_string)

    # Set up configuration. Pulsar configuration will not be used.
    os.environ["HEALTH_CHECK_SERVER"] = "8080"
    os.environ["IS_FRESH_START"] = "false"
    os.environ["PINO_LOG_LEVEL"] = "debug"
    os.environ["PULSAR_BLOCK_IF_QUEUE_FULL"] = "true"
    os.environ["PULSAR_CACHE_READER_NAME"] = "foo-cache-reader"
    os.environ[
        "PULSAR_CATALOGUE_READERS"
    ] = """
    [
      {
        "feedPublisherId": "fi:kuopio",
        "name": "vehicle-anonymization-profiler-catalogue-reader-fi-kuopio",
        "topic": "persistent://foo/bar/baz-fi-kuopio"
      }
    ]
    """
    os.environ["PULSAR_COMPRESSION_TYPE"] = "ZSTD"
    os.environ["PULSAR_OAUTH2_AUDIENCE"] = "urn:sn:pulsar:waltti:alpha"
    os.environ["PULSAR_OAUTH2_ISSUER_URL"] = "https://foo.bar"
    os.environ["PULSAR_OAUTH2_KEY_PATH"] = "/secrets/foo-key"
    os.environ["PULSAR_PRODUCER_TOPIC"] = "persistent://foo/bar/baz"
    os.environ["PULSAR_SERVICE_URL"] = "pulsar+ssl://foo.bar:6651"
    os.environ["PULSAR_TLS_VALIDATE_HOSTNAME"] = "true"

    mocker.patch(
        "waltti_apc_vehicle_anonymization_profiler.main.pulsar_wrapper.create_client"
    )
    producer_main_mock = mocker.MagicMock()
    mocker.patch(
        "waltti_apc_vehicle_anonymization_profiler.main.pulsar_wrapper.create_producer",
        return_value=producer_main_mock,
    )
    cache_reader = mocker.MagicMock()
    cache_reader.has_message_available.return_value = False
    cache_reader.topic.return_value = "persistent://public/default/cache-topic"
    mocker.patch(
        "waltti_apc_vehicle_anonymization_profiler.main.pulsar_wrapper.create_reader",
        return_value=cache_reader,
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
    mocker.patch(
        "waltti_apc_vehicle_anonymization_profiler.main.pulsar_wrapper.create_readers",
        return_value=catalogue_readers,
    )
    mocker.patch(
        "waltti_apc_vehicle_anonymization_profiler.graceful_exit.close_pulsar"
    )
    mocker.patch(
        "waltti_apc_vehicle_anonymization_profiler.graceful_exit.sys.exit"
    )
    mocker.patch(
        "waltti_apc_vehicle_anonymization_profiler.message_processing.hyperparameter_optimization.run_inference_for_all_vehicle_models",
        side_effect=add_csv_files,
    )
    # FIXME:
    # Due to a known issue we destroy and create Pulsar resources in
    # message_processing.py. Once the issue is satisfactorily resolved, pass
    # just the producer and the readers onwards as usual from main() to
    # message_processing, remove these mocks and replace mock on which
    # the assert happens.
    # https://github.com/apache/pulsar-client-python/issues/127
    producer_mock = mocker.MagicMock()
    mocker.patch(
        "waltti_apc_vehicle_anonymization_profiler.message_processing.pulsar_wrapper.create_producer",
        return_value=producer_mock,
    )
    mocker.patch(
        "waltti_apc_vehicle_anonymization_profiler.message_processing.graceful_exit.close_pulsar"
    )

    main.main()

    producer_mock.send.assert_called_with(
        expected_producer_message_data,
        event_timestamp=expected_producer_message_event_timestamp,
    )
