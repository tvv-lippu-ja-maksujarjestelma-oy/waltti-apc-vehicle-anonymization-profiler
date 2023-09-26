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
