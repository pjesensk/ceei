import rasterio
from rasterio import features
import numpy as np
import pandas as pd
import multiprocessing as mp
from multiprocessing import Pool
from shapely.geometry import Polygon
from shapely.geometry import Point
import os
import json
import time
files = [
# "population_AF01_2018-10-01.tif",
# "population_AF02_2018-10-01.tif",
# "population_AF03_2018-10-01.tif",
# "population_AF04_2018-10-01.tif",
# "population_AF05_2018-10-01.tif",
# "population_AF06_2018-10-01.tif",
# "population_AF07_2018-10-01.tif",
# "population_AF08_2018-10-01.tif",
# "population_AF10_2018-10-01.tif",
# "population_AF11_2018-10-01.tif",
# "population_AF12_2018-10-01.tif",
# "population_AF13_2018-10-01.tif",
# "population_AF14_2018-10-01.tif",
# "population_AF15_2018-10-01.tif",
# "population_AF16_2018-10-01.tif",
# "population_AF17_2018-10-01.tif",
# "population_AF18_2018-10-01.tif",
# "population_AF19_2018-10-01.tif",
# "population_AF20_2018-10-01.tif",
"population_AF21_2018-10-01.tif",
# "population_AF22_2018-10-01.tif",
# "population_AF23_2018-10-01.tif",
# "population_AF24_2018-10-01.tif",
# "population_AF25_2018-10-01.tif",
# "population_AF26_2018-10-01.tif",
# "population_AF27_2018-10-01.tif",
# "population_AF28_2018-10-01.tif",
]

def point_in_shape (lat, lon, lon_upper_left, lon_down_right, lat_upper_left, lat_down_right ):
  print (lon, lat, lon_upper_left, lon_down_right, lat_upper_left, lat_down_right)
  return lon >= lon_upper_left and lon <= lon_down_right and lat <= lat_upper_left and lat >= lat_down_right

file_path = 'population.parquet'
features_geojson = []
# for filename in os.listdir('./data/cog'):
#   if filename.endswith ('.tif'):
#     f = os.path.join('./data/cog', filename)
#     # Ensure .xml is included in allowed extensions
for f in files:
    print (f)
    with rasterio.Env(CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif,.xml"):
      with rasterio.open('./data/population/'+f) as src:
        geotransform = src.transform
        print (src.xy(0,0))
        print (src.xy(src.height,src.width))
        win_col_min, win_row_min = rasterio.transform.rowcol(geotransform, 19.85, -6.3)
        win_col_max, win_row_max = rasterio.transform.rowcol(geotransform, 21.85, -8.3)

        # Get a windowed view of the data for the desired area
        window = rasterio.windows.Window(win_col_min, win_row_min, win_col_max - win_col_min + 1, win_row_max - win_row_min + 1)
      
        data = src.read()#, window=window)
        mask = data != 0
        shapes = features.shapes(data, mask=mask, transform=geotransform)
        print (list(shapes))
        for i, (s, v) in shapes:
          #print (value)
          print (i, (s, v))
        # lon_upper_left, lat_upper_left = src.xy(0,0)
        # lon_down_right, lat_down_right = src.xy(src.height,src.width)
        lon_upper_left, lat_upper_left = 21.85, -8.3
        lon_down_right, lat_down_right = 19.85, -6.3
        polygon = [
          [lon_upper_left, lat_down_right],
          [lon_down_right,lat_down_right],
          [lon_down_right,lat_upper_left],
          [lon_upper_left,lat_upper_left],
          [lon_upper_left,lat_down_right]
          ]
        shapely_polygon = Polygon(polygon)
       
        

        shape_features = [{"geometry": shape, "properties": {"value": value}} for shape, value in shapes]

        # Create a GeoDataFrame from the features list
        df = gpd.GeoDataFrame.from_features(features)
        print (df)
        # Extract rows and columns
        # rows, cols = data.shape

        # # Loop through each pixel and get coordinates and value
        # df = pd.DataFrame(columns=['lon','lat','population'])
        pc = 0
        print (rows)
        print (cols)
        for row in range(rows):
          for col in range(cols):
            xs, ys = rasterio.transform.xy(geotransform, row, col)
            if not np.isnan(data[row, col]):
              df.loc[pc] = [xs,ys,data[row, col]]
              pc = pc + 1
              print (row,col, xs,ys,data[row, col])
              feature = {
                "type": "Feature",
                "properties": {
                  "file": f,
                  "population": data[row, col],
                  "tags": {},
                  "longitude_latitude":{
                    "type": "Point",
                    "coordinates": [xs,ys]
                  }
                },
                "geometry":{
                  "type": "Point",
                  "coordinates": [xs,ys]
                }
              }
              features_geojson.append (feature)
      src.close()

output_json = {
  "type": "FeatureCollection",
  "features": 
    features_geojson
}

print (json.dumps(output_json))
          
          # if file_path.exists():
          #   df.to_parquet(file_path, engine='fastparquet', append=True)
          # else:
          #   df.to_parquet(file_path, engine='fastparquet')




      