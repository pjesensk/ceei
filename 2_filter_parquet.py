import pandas as pd
import geopandas as gpd
import json
import re

data = pd.read_parquet ('./data.parquet')

def point_in_shape (lon, lat, shape):
    lons = []
    lats = []
    if shape['type'] == 'Polygon':
        for coord in shape['coordinates'][0]:
            lons.append(coord[0])
            lats.append(coord[1])
        return lon >= min(lons) and lon <= max(lons) and lat >= min(lats) and lat<= max(lats)
    else:
        for coord in shape['coordinates'][0][0]:
            lons.append(coord[0])
            lats.append(coord[1])
        return lon >= min(lons) and lon <= max(lons) and lat >= min(lats) and lat<= max(lats)

def fuels_equal (f1, f2):
    return f1.upper() in f2.upper()

data = data[data.apply( lambda x: point_in_shape(x['longitude'],x['latitude'],json.loads(x['Geo Shape'])) ,axis=1)]
data = data[data.apply( lambda x: fuels_equal(x['primary_fuel'],x['Technology']) ,axis=1)]
data.to_parquet('./data.parquet')
