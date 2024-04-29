import geopandas as gpd
from shapely.geometry import Polygon, mapping
import json

fname = "./data.geojson"

df = gpd.read_file(fname)

def point_in_shape (lat, lon):
  lon_upper_left, lat_upper_left = 20.85, -7.3
  #print (lon, lat, lon_upper_left, lon_down_right, lat_upper_left, lat_down_right)
  return lon >= lon_upper_left-0.25 and lon <= lon_upper_left+0.25 and lat <= lat_upper_left+0.25 and lat >= lat_upper_left-0.25

features = []
for index, row in df.iterrows():
  polygon = row['geometry']
  poly_mapped = mapping(polygon)
  poly_coordinates = poly_mapped['coordinates'][0]
  for coords in poly_coordinates:
    if point_in_shape(coords[1],coords[0]):
      stroke = "#2196F3"
      fill = "#03A9F4"
      if row['DN'] > 20:
        stroke = "#4CAF50"
        fill = "#8BC34A"
      if row['DN'] > 30:
        stroke = "#FF9800"
        fill = "#FFC107"
      feature = {
        "type": "Feature",
        "properties": {
          "stroke": stroke,
          "fill": fill,
          "DN": row['DN'],
          "tags": {},
        },
        "geometry":{
          "type": "Polygon",
          "coordinates": [poly_coordinates]
        }
      }
      features.append (feature)
      break

output_json = {
  "type": "FeatureCollection",
  "features": 
    features
}
with open('output.json', 'w') as f:
    json.dump(output_json, f)
