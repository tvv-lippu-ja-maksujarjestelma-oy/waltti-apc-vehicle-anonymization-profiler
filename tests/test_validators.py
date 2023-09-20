import jsonschema
import pytest
from waltti_apc_vehicle_anonymization_profiler import validators


def test_get_vehicle_apc_mapping_validator():
    try:
        validators.get_vehicle_apc_mapping_validator()
    except Exception:
        pytest.fail(
            "Getting JSON Schema validator for vehicle-APC mapping failed"
        )


def test_get_profile_collection_validator():
    try:
        validators.get_profile_collection_validator()
    except Exception:
        pytest.fail(
            "Getting JSON Schema validator for profile collection failed"
        )


def test_model_names_follow_schema_regex():
    example_key_value_follow_regex = {
        "vehicleModels": {"fi:kuopio:1234_124": "45-60"},
        "modelProfiles": {"45-60": "foo,bar,baz"},
    }
    example_wrong_key = {
        "vehicleModels": {"fi:kuopio:1234_124": "45-60"},
        "modelProfiles": {"45-60X": "foo,bar,baz"},
    }
    example_wrong_value = {
        "vehicleModels": {"fi:kuopio:1234_124": "45-60X"},
        "modelProfiles": {"45-60": "foo,bar,baz"},
    }
    validator = validators.get_profile_collection_validator()
    try:
        validator.validate(example_key_value_follow_regex)
    except jsonschema.ValidationError:
        pytest.fail("Validating supposedly valid example data failed")
    with pytest.raises(jsonschema.ValidationError):
        validator.validate(example_wrong_key)
    with pytest.raises(jsonschema.ValidationError):
        validator.validate(example_wrong_value)
