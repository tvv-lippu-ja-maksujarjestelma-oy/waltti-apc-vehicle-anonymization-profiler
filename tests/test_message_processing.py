import json
import logging
import pathlib

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


def test_process_messages(mocker):
    def add_csv_files(config):
        tmp_dir = config["outputDirectory"]
        tmp_path = pathlib.Path(tmp_dir)
        for vm in config["vehicleModels"]:
            for csv_filename in vm["outputFilenames"]:
                csv_path = tmp_path / csv_filename
                with open(csv_path, "w") as f:
                    f.write("foo")

    mocker.patch(
        "waltti_apc_vehicle_anonymization_profiler.message_processing.hyperparameter_optimization.run_inference_for_all_vehicle_models",
        side_effect=add_csv_files,
    )

    logging.basicConfig(
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG,
        format=(
            "%(asctime)s.%(msecs)03d %(levelname)s %(module)s -"
            " %(funcName)s: %(message)s"
        ),
    )
    logger = logging.getLogger()

    catalogue_data_kuopio = [
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
    event_timestamp_kuopio = 123
    catalogue_message_stub_kuopio = mocker.MagicMock()
    catalogue_message_stub_kuopio.data.return_value = json.dumps(
        catalogue_data_kuopio
    ).encode("utf-8")
    catalogue_message_stub_kuopio.event_timestamp.return_value = (
        event_timestamp_kuopio
    )
    catalogue_message_stub_kuopio.topic_name.return_value = (
        "persistent://public/default/catalogue-fi-kuopio"
    )
    catalogue_reader_mock_kuopio = mocker.MagicMock()
    catalogue_reader_mock_kuopio.has_message_available.side_effect = [
        True,
        False,
    ]
    catalogue_reader_mock_kuopio.read_next.return_value = (
        catalogue_message_stub_kuopio
    )

    catalogue_data_jyvaskyla = [
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
    event_timestamp_jyvaskyla = 234
    catalogue_message_stub_jyvaskyla = mocker.MagicMock()
    catalogue_message_stub_jyvaskyla.data.return_value = json.dumps(
        catalogue_data_jyvaskyla
    ).encode("utf-8")
    catalogue_message_stub_jyvaskyla.event_timestamp.return_value = (
        event_timestamp_jyvaskyla
    )
    catalogue_message_stub_jyvaskyla.topic_name.return_value = (
        "persistent://public/default/catalogue-fi-jyvaskyla"
    )
    catalogue_reader_mock_jyvaskyla = mocker.MagicMock()
    catalogue_reader_mock_jyvaskyla.has_message_available.side_effect = [
        True,
        False,
    ]
    catalogue_reader_mock_jyvaskyla.read_next.return_value = (
        catalogue_message_stub_jyvaskyla
    )

    catalogue_readers = {
        "fi:jyvaskyla": catalogue_reader_mock_jyvaskyla,
        "fi:kuopio": catalogue_reader_mock_kuopio,
    }
    cache_reader_mock = mocker.MagicMock()
    cache_reader_mock.has_message_available.return_value = False
    resources = {
        "pulsar_cache_reader": cache_reader_mock,
        "pulsar_catalogue_readers": catalogue_readers,
    }
    processing_config = {
        "is_fresh_start": False,
        "feed_map": {
            "persistent://public/default/catalogue-fi-kuopio": "fi:kuopio"
        },
    }
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
        expected_producer_message_data_encoded,
        event_timestamp=min(event_timestamp_kuopio, event_timestamp_jyvaskyla),
    )
