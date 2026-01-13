import requests
import json
import geopandas as gpd
from shapely.geometry import shape
from urllib3.exceptions import InsecureRequestWarning
import time  # 用于在请求之间添加延迟
import os  # 导入os模块
 
# 禁用不安全请求警告
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
 
 
def fetch_page(url, params):
    """发送请求获取网页内容"""
    try:
        response = requests.get(url=url, params=params, verify=False)  # 禁用SSL验证
        if response.status_code == 200:
            return response.json()  # 返回JSON格式的数据
        else:
            print(f"Failed to retrieve the page. Status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
 
 
def create_grid(min_longitude, max_longitude, min_latitude, max_latitude, step):
    """创建一个网格，返回一系列的(longitude, latitude)坐标对"""
    grid_points = []
    current_longitude = min_longitude
 
    while current_longitude <= max_longitude:
        current_latitude = min_latitude
        while current_latitude <= max_latitude:
            grid_points.append((current_longitude, current_latitude))
            current_latitude += step
        current_longitude += step
 
    return grid_points
 
 
if __name__ == '__main__':
    # 请求配置
    url = 'https://map4.shanghai-map.net/arcgis/rest/services/planblock20250110/MapServer/identify'
 
    # 上海市的地理范围和步长
    min_longitude = 120.80012536879065  # 上海市最小经度
    max_longitude = 122.27874755895266  # 上海市最大经度
    min_latitude = 30.54553222673087  # 上海市最小纬度
    max_latitude = 31.945383160404504  # 上海市最大纬度
    step = 0.005  # 步长，单位：度
 
    # 创建网格
    grid_points = create_grid(min_longitude, max_longitude, min_latitude, max_latitude, step)
 
    features = []
    for point in grid_points:
        geometry_str = json.dumps({'x': point[0], 'y': point[1], 'spatialReference': {'wkid': 4326}})
        params = {
            'sr': 4326,
            'layers': 'all',
            'tolerance': 1,
            'returnGeometry': 'true',
            'imageDisplay': '651,852,96',
            'mapExtent': f'{min_longitude},{min_latitude},{max_longitude},{max_latitude}',
            'geometry': geometry_str,
            'geometryType': 'esriGeometryPoint',
            'f': 'json'
        }
 
        print(f"Fetching data for point {point}...")
        json_response = fetch_page(url, params)
 
        # 在请求之间添加延迟以减少服务器负载
        time.sleep(0.1)
 
        if json_response and 'results' in json_response:
            for result in json_response['results']:
                geom = result.get('geometry', {})
                attributes = result.get('attributes', {})
 
                if geom and 'rings' in geom:
                    geom_with_type = {'type': 'Polygon', 'coordinates': geom['rings'],
                                      'spatialReference': geom.get('spatialReference')}
                    try:
                        polygon = shape(geom_with_type)  # 使用shapely的shape函数直接转换GeoJSON到Shapely几何对象
 
                        # 确保PLANLAND_1字段存在并且是字符串类型
                        planland_1 = attributes.get('PLANLAND_1', '')
                        if not isinstance(planland_1, str):
                            planland_1 = str(planland_1)
 
                        # 将几何和属性信息添加到列表中
                        features.append({
                            'geometry': polygon,
                            **attributes,
                            'PLANLAND_1': planland_1.encode('utf-8').decode('utf-8')  # 确保使用UTF-8编码
                        })
                    except Exception as e:
                        print(f"Error converting geometry to shape: {e}")
 
    if features:
        gdf = gpd.GeoDataFrame(features, crs="EPSG:4326")
 
        # 检查并创建输出文件夹
        output_dir = './shp'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)  # 创建文件夹
 
        # 将结果保存为shapefile，确保使用UTF-8编码
        gdf.to_file(os.path.join(output_dir, "output.shp"), encoding='utf-8', driver="ESRI Shapefile")
        print("Shapefile saved successfully.")
    else:
        print("No valid polygons found.")
 
 
 
