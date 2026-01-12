import argparse
import json
import math
import os
import time
from datetime import datetime

import geopandas as gpd
import requests
from shapely.geometry import shape
from urllib3.exceptions import InsecureRequestWarning


requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


def fetch_page(session, url, params, retries=3, backoff=1.5):
    for attempt in range(1, retries + 1):
        try:
            response = session.get(url=url, params=params, verify=False, timeout=30)
            if response.status_code == 200:
                return response.json()
            print(f"HTTP {response.status_code} for {response.url}")
        except Exception as exc:
            print(f"Request error (attempt {attempt}/{retries}): {exc}")
        time.sleep(backoff * attempt)
    return None


def point_generator(min_longitude, max_longitude, min_latitude, max_latitude, step):
    current_longitude = min_longitude
    while current_longitude <= max_longitude:
        current_latitude = min_latitude
        while current_latitude <= max_latitude:
            yield current_longitude, current_latitude
            current_latitude += step
        current_longitude += step


def total_points(min_longitude, max_longitude, min_latitude, max_latitude, step):
    lon_count = int(math.floor((max_longitude - min_longitude) / step)) + 1
    lat_count = int(math.floor((max_latitude - min_latitude) / step)) + 1
    return lon_count * lat_count


def load_checkpoint(path):
    if not os.path.exists(path):
        return 0
    with open(path, "r", encoding="utf-8") as handle:
        return int(handle.read().strip() or 0)


def save_checkpoint(path, index):
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(str(index))


def load_seen_ids(path):
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as handle:
        return {line.strip() for line in handle if line.strip()}


def append_seen_id(path, object_id):
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(f"{object_id}\n")


def append_feature(path, feature):
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(feature, ensure_ascii=False) + "\n")


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch Shanghai land use data.")
    parser.add_argument(
        "--url",
        default="https://map4.shanghai-map.net/arcgis/rest/services/planblock/MapServer/identify",
        help="ArcGIS identify URL.",
    )
    parser.add_argument("--min-longitude", type=float, default=120.80012536879065)
    parser.add_argument("--max-longitude", type=float, default=122.27874755895266)
    parser.add_argument("--min-latitude", type=float, default=30.54553222673087)
    parser.add_argument("--max-latitude", type=float, default=31.945383160404504)
    parser.add_argument("--step", type=float, default=0.0001)
    parser.add_argument("--delay", type=float, default=0.1, help="Delay between requests.")
    parser.add_argument("--save-every", type=int, default=500, help="Checkpoint interval.")
    parser.add_argument("--output-dir", default="./shp")
    return parser.parse_args()


def main():
    args = parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    checkpoint_path = os.path.join(args.output_dir, "checkpoint.txt")
    seen_ids_path = os.path.join(args.output_dir, "seen_ids.txt")
    jsonl_path = os.path.join(args.output_dir, "features.jsonl")

    start_index = load_checkpoint(checkpoint_path)
    seen_ids = load_seen_ids(seen_ids_path)

    total = total_points(
        args.min_longitude,
        args.max_longitude,
        args.min_latitude,
        args.max_latitude,
        args.step,
    )

    session = requests.Session()
    collected = 0
    last_log_time = time.time()

    for index, point in enumerate(
        point_generator(
            args.min_longitude,
            args.max_longitude,
            args.min_latitude,
            args.max_latitude,
            args.step,
        ),
        start=0,
    ):
        if index < start_index:
            continue

        min_lon = point[0] - args.step
        max_lon = point[0] + args.step
        min_lat = point[1] - args.step
        max_lat = point[1] + args.step

        geometry_str = json.dumps(
            {"x": point[0], "y": point[1], "spatialReference": {"wkid": 4326}}
        )
        params = {
            "sr": 4326,
            "layers": "all",
            "tolerance": 1,
            "returnGeometry": "true",
            "imageDisplay": "651,852,96",
            "mapExtent": f"{min_lon},{min_lat},{max_lon},{max_lat}",
            "geometry": geometry_str,
            "geometryType": "esriGeometryPoint",
            "f": "json",
        }

        json_response = fetch_page(session, args.url, params)
        time.sleep(args.delay)

        if json_response and "results" in json_response:
            for result in json_response["results"]:
                attributes = result.get("attributes", {})
                object_id = str(
                    attributes.get("OBJECTID")
                    or attributes.get("ObjectID")
                    or attributes.get("objectid")
                    or ""
                )
                if object_id and object_id in seen_ids:
                    continue

                geom = result.get("geometry", {})
                if geom and "rings" in geom:
                    geom_with_type = {
                        "type": "Polygon",
                        "coordinates": geom["rings"],
                    }
                    try:
                        polygon = shape(geom_with_type)
                    except Exception as exc:
                        print(f"Geometry conversion error: {exc}")
                        continue

                    planland_1 = attributes.get("PLANLAND_1", "")
                    if not isinstance(planland_1, str):
                        planland_1 = str(planland_1)

                    feature = {
                        "type": "Feature",
                        "geometry": polygon.__geo_interface__,
                        "properties": {
                            **attributes,
                            "PLANLAND_1": planland_1,
                        },
                    }
                    append_feature(jsonl_path, feature)
                    if object_id:
                        seen_ids.add(object_id)
                        append_seen_id(seen_ids_path, object_id)
                    collected += 1

        if (index + 1) % args.save_every == 0:
            save_checkpoint(checkpoint_path, index + 1)

        now = time.time()
        if now - last_log_time >= 5:
            percent = (index + 1) / total * 100
            print(
                f"[{datetime.now().isoformat(timespec='seconds')}] "
                f"Progress {index + 1}/{total} ({percent:.2f}%), "
                f"collected {collected}"
            )
            last_log_time = now

    save_checkpoint(checkpoint_path, total)

    if os.path.exists(jsonl_path):
        gdf = gpd.read_file(jsonl_path)
        output_path = os.path.join(args.output_dir, "output.shp")
        gdf.to_file(output_path, encoding="utf-8", driver="ESRI Shapefile")
        print(f"Shapefile saved to {output_path}.")
    else:
        print("No valid polygons found.")


if __name__ == "__main__":
    main()
