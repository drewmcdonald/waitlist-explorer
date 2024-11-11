import json
import math
from itertools import permutations
from functools import lru_cache


@lru_cache(maxsize=500)
def haversine(lat1, lon1, lat2, lon2):
    R = 3444  # Radius of Earth in nautical miles
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


if __name__ == "__main__":
    with open("data/centers_geocoded.jsonl", "r") as f:
        centers = [json.loads(line) for line in f]

    with open("data/centers_distance.txt", "w") as f:
        f.write("source\ttarget\tdistance_nm\n")
        for center1, center2 in permutations(centers, 2):
            distance_nm = haversine(
                center1["lat"], center1["lon"], center2["lat"], center2["lon"]
            )
            f.write(f"{center1['code']}\t{center2['code']}\t{distance_nm:.2f}\n")
