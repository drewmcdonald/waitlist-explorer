import csv
from functools import lru_cache
import json
from time import sleep
from typing import Tuple

import requests


URL = "https://nominatim.openstreetmap.org/search?format=json&city={city}&state={state}"

HEADERS = {"User-Agent": "Drew McDonald/1.0 (github.com/drewmcdonald)"}


@lru_cache(maxsize=500)
def geocode_city(city: str, state: str) -> Tuple[float, float]:
    url = URL.format(city=city, state=state)
    result = requests.get(url, headers=HEADERS)
    json = result.json()
    sleep(0.5)
    return float(json[0]["lat"]), float(json[0]["lon"])


if __name__ == "__main__":
    result = []
    with open("data/centers.txt", "r") as file:
        reader = csv.reader(file, delimiter="\t")
        next(reader)  # Skip header row
        for row in reader:
            try:
                code, name, city, state = row
                lat, lon = geocode_city(city, state)
                result.append(
                    dict(
                        code=code,
                        name=name,
                        city=city,
                        state=state,
                        lat=lat,
                        lon=lon,
                    )
                )
            except Exception as e:
                print(f"failed to read row {row}")
                print(e)

    with open("data/centers_geocoded.jsonl", "w") as file:
        for row in result:
            json.dump(row, file)
            file.write("\n")
