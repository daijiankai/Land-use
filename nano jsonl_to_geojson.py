import json
import geopandas as gpd
from shapely.geometry import shape

input_file = "output_test/features.jsonl"
output_file = "output_test/land_use.geojson"

features = []
with open(input_file, "r", encoding="utf-8") as f:
    for line in f:
        data = json.loads(line)
        geom = shape(data["geometry"])
        props = data.get("properties", {})
        props["geometry"] = geom
        features.append(props)

gdf = gpd.GeoDataFrame(features, crs="EPSG:4326")
gdf.to_file(output_file, driver="GeoJSON")

print(f"Saved to {output_file}")
