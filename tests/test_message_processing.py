import json

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


def test_sanitize_vehicle_apc_mapping_payload_removes_empty_registration():
    payload = [
        {
            "operatorId": "6714",
            "vehicleShortName": "470",
            "vehicleRegistrationNumber": "",
            "standingCapacity": 40,
            "seatingCapacity": 35,
            "equipment": [{"type": "PASSENGER_COUNTER", "id": "JL470-APC"}],
        }
    ]
    result = message_processing.sanitize_vehicle_apc_mapping_payload(payload)
    assert "vehicleRegistrationNumber" not in result[0]


def test_generate_message_to_send_publishes_without_new_models(mocker):
    logger = mocker.MagicMock()
    latest_message = mocker.MagicMock()
    latest_message.event_timestamp.return_value = 123
    latest_messages = {"fi:jyvaskyla": latest_message}
    mocker.patch(
        "waltti_apc_vehicle_anonymization_profiler.message_processing.get_latest_vehicles_to_tuple_models",
        return_value={"fi:jyvaskyla:6714_470": (35, 40)},
    )
    compute_new_profiles = mocker.patch(
        "waltti_apc_vehicle_anonymization_profiler.message_processing.compute_new_profiles"
    )

    (
        producer_message_data,
        event_timestamp,
    ) = message_processing.generate_message_to_send(
        logger,
        {"35-40": "fake-csv"},
        latest_messages,
    )

    compute_new_profiles.assert_not_called()
    assert producer_message_data is not None
    assert event_timestamp == 123
    parsed = json.loads(producer_message_data.decode("utf-8"))
    assert parsed["vehicleModels"] == {"fi:jyvaskyla:6714_470": "35-40"}
    assert parsed["modelProfiles"] == {"35-40": "fake-csv"}


def test_validate_vehicle_catalogue_skips_invalid_feed_publisher(mocker):
    logger = mocker.MagicMock()
    valid_message = mocker.MagicMock()
    valid_message.data.return_value = json.dumps(
        [
            {
                "operatorId": "6714",
                "vehicleShortName": "470",
                "vehicleRegistrationNumber": "",
                "standingCapacity": 40,
                "seatingCapacity": 35,
                "equipment": [
                    {"type": "PASSENGER_COUNTER", "id": "JL470-APC"}
                ],
            }
        ]
    ).encode("utf-8")
    valid_message.topic_name.return_value = "topic-valid"
    valid_message.event_timestamp.return_value = 1
    valid_message.properties.return_value = {}

    invalid_message = mocker.MagicMock()
    invalid_message.data.return_value = b"{"
    invalid_message.topic_name.return_value = "topic-invalid"
    invalid_message.event_timestamp.return_value = 2
    invalid_message.properties.return_value = {}

    result = (
        message_processing.validate_and_return_vehicle_apc_mapping_messages(
            logger,
            {
                "fi:jyvaskyla": valid_message,
                "fi:kuopio": invalid_message,
            },
        )
    )

    assert "fi:jyvaskyla" in result
    assert "fi:kuopio" not in result
