import json
from dataclasses import dataclass


@dataclass
class Center:
    code: str
    name: str
    city: str
    state: str
    lat: float
    lon: float

    def __str__(self) -> str:
        return f"{self.code} - {self.name} ({self.city}, {self.state})"

    @classmethod
    def from_json(cls, json_str):
        return cls(**json.loads(json_str))
