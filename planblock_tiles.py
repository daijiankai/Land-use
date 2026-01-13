import requests
import json
import geopandas as gpd
from shapely.geometry import shape
from urllib3.exceptions import InsecureRequestWarning
import time
import os
import math

# 禁用不安全请求警告
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

URL = "https://map4.shanghai-map.net/arcgis/rest/services/planblock20250110/MapServer/identify"

# 上海市范围（你的）
MIN_LON = 120.80012536879065
MAX_LON = 122.27874755895266
MIN_LAT = 30.54553222673087
MAX_LAT = 31.945383160404504

# ====== 你主要会调这两个参数 ======
STEP = 0.001        # 块内采样精度：0.001 约 100m（你想要的精细）
TILE_SIZE = 0.05    # 分块大小：0.05° 约 5km（建议先用这个）
# =================================

DELAY = 0.1         # 每次请求间隔
OUT_DIR = "./shp_tiles"
os.makedirs(OUT_DIR, exist_ok=True)

def fetch_page(url, params):
    try:
        r = requests.get(url=url, params=params, verify=False, timeout=30)
        if r.status_code == 200:
            return r.json()
        print("HTTP", r.status_code)
        return None
    except Exception as e:
        print("Request error:", e)
        return None

def frange(a, b, step):
    x = a
    # 防止浮点误差
    while x <= b + 1e-12:
        yield x
        x += step

def run_tile(tile_min_lon, tile_max_lon, tile_min_lat, tile_max_lat, tile_id):
    out_base = os.path.join(OUT_DIR, f"tile_{tile_id}_lon{tile_min_lon:.3f}-{tile_max_lon:.3f}_lat{tile_min_lat:.3f}-{tile_max_lat:.3f}")
    out_shp = out_base + ".shp"

    # 已经跑过的块就跳过（断点续跑的核心）
    if os.path.exists(out_shp):
        print(f"[SKIP] {os.path.basename(out_shp)} exists")
        return

    features = []
    total = 0
    hit = 0

    for lon in frange(tile_min_lon, tile_max_lon, STEP):
        for lat in frange(tile_min_lat, tile_max_lat, STEP):
            total += 1
            geometry_str = json.dumps({"x": lon, "y": lat, "spatialReference": {"wkid": 4326}})

            params = {
                "sr": 4326,
                "layers": "all",
                "tolerance": 1,
                "returnGeometry": "true",
                "imageDisplay": "651,852,96",
                # 注意：mapExtent 用“当前块”的范围，不用全上海
                "mapExtent": f"{tile_min_lon},{tile_min_lat},{tile_max_lon},{tile_max_lat}",
                "geometry": geometry_str,
                "geometryType": "esriGeometryPoint",
                "f": "json",
            }

            j = fetch_page(URL, params)
            time.sleep(DELAY)

            if j and "results" in j and j["results"]:
                for result in j["results"]:
                    geom = result.get("geometry", {})
                    attrs = result.get("attributes", {}) or {}
                    # 只处理 polygon rings
                    if geom and "rings" in geom:
                        try:
                            geom_geojson = {"type": "Polygon", "coordinates": geom["rings"]}
                            polygon = shape(geom_geojson)

                            planland_1 = attrs.get("PLANLAND_1", "")
                            if not isinstance(planland_1, str):
                                planland_1 = str(planland_1)

                            features.append({
                                "geometry": polygon,
                                **attrs,
                                "PLANLAND_1": planland_1
                            })
                            hit += 1
                        except Exception as e:
                            print("Geometry convert error:", e)

            if total % 2000 == 0:
                print(f"  tile {tile_id}: sampled {total}, hits {hit}, features {len(features)}")

    if features:
        gdf = gpd.GeoDataFrame(features, crs="EPSG:4326")
        # Shapefile 字段名会被截断，这是正常的
        gdf.to_file(out_shp, encoding="utf-8", driver="ESRI Shapefile")
        print(f"[OK] Saved {out_shp}  (features={len(gdf)})")
    else:
        print(f"[EMPTY] tile {tile_id} no valid polygons")

def main():
    # 生成 tile 网格
    tile_id = 0
    lon = MIN_LON
    while lon < MAX_LON - 1e-12:
        lat = MIN_LAT
        lon2 = min(lon + TILE_SIZE, MAX_LON)
        while lat < MAX_LAT - 1e-12:
            lat2 = min(lat + TILE_SIZE, MAX_LAT)
            tile_id += 1
            print(f"\n=== TILE {tile_id}: lon {lon:.3f}-{lon2:.3f}, lat {lat:.3f}-{lat2:.3f} ===")
            run_tile(lon, lon2, lat, lat2, tile_id)
            lat += TILE_SIZE
        lon += TILE_SIZE

    print("\nAll tiles done. Output:", OUT_DIR)

if __name__ == "__main__":
    main()
