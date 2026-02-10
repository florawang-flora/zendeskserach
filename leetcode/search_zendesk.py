import json
import argparse
from pathlib import Path
import os

def load_json(path):
    p = Path(path)

    #print(Path.cwd())
    #print( p.resolve())
    if not p.exists():
        raise FileNotFoundError(f"JSON file not found: {path}")

    with p.open("r") as f:
        data = json.load(f)

    # if it doesn't have arrary
    if not isinstance(data, list):
        raise ValueError(f"Expected a JSON array, however: {type(data)}")
    return data
print(load_json("../data_source/users.json"))
