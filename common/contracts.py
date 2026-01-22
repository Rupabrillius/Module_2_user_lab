import json
from jsonschema import Draft7Validator

def load_schema(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def validate(schema: dict, obj: dict) -> list[str]:
    v = Draft7Validator(schema)
    return [e.message for e in v.iter_errors(obj)]
