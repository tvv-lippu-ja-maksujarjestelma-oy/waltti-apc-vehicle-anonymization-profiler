"""Get validators."""

import importlib.resources
import json

import jsonschema


def get_validator(path):
    """Get a validator for the schema in the given path.

    Raises json.JSONDecodeError if the schema file is not valid JSON.

    Raises jsonschema.SchemaError if the schema is not a valid JSON Schema.

    Raises FileNotFoundError if the path is incorrect.

    As the schema files are distributed within this service instead of loading
    them from the internet, testing all uses of this function means we do not
    need to litter all code with try-except to catch mistakes in the path or
    the schema.
    """
    return jsonschema.Draft202012Validator(
        json.loads(
            importlib.resources.files(
                "waltti_apc_vehicle_anonymization_profiler"
            )
            .joinpath(path)
            .read_text(encoding="utf-8")
        )
    )


def get_vehicle_apc_mapping_validator():
    return get_validator("schemas/vehicle-apc-mapping.schema.json")


def get_profile_collection_validator():
    return get_validator("schemas/profile-collection.schema.json")
