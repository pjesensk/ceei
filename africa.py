import gc
import pandas as pd
import geopandas as gpd
import json
import numpy as np
import multiprocessing as mp
from multiprocessing import Pool
from shapely.geometry import Polygon
from shapely.geometry import Point
from shapely import wkt
import os
import time
from osgeo import gdal
from osgeo import gdal_array
from pyrosm import OSM
osm = OSM("./boundaries.osm.pbf")

STATISTICS_MEAN = 0.78443079274767

# world administrative boundaries
world_boundaries_path = "./data/world-administrative-boundaries.geojson"
l2_boundaries_path = "./data/admin2.geojson"
education_share_path = "./data/share-of-the-population-with-completed-tertiary-education.csv"
population_path = './data/landscan-global-2022.tif'
ceei_index_path = './data/full_ceei_data2.xlsx'

#num_processes = mp.cpu_count()
num_processes = 32

#Paralell processing function
def mp_func (src_path, band, row_start, row_end, col_start, col_end):
  global GT
  global gdf_boundaries
  global output_df
  src_ds = gdal.OpenEx(src_path, gdal.OF_READONLY|gdal.OF_RASTER)
  srcband = src_ds.GetRasterBand(band)
  band_values = srcband.ReadAsArray(col_start,row_start,col_end-col_start,row_end-row_start)
  Y_line = np.arange(row_start, row_end)
  X_pixel =np.arange(col_start, col_end)
  xv, yv = np.meshgrid(X_pixel, Y_line, indexing='ij')

  X_geo = (GT[0] + xv * GT[1] + yv * GT[2])
  Y_geo = (GT[3] + xv * GT[4] + yv * GT[5])

  df = pd.DataFrame({'row_index': xv.ravel(),
                   'col_index': yv.ravel(),
                   'x_geo': X_geo.ravel(),
                   'y_geo': Y_geo.ravel(),
                   'value': band_values.ravel()})

  filtered_df = df[df['value']>0]
  gdf = gpd.GeoDataFrame(
    filtered_df, geometry=gpd.points_from_xy(filtered_df['x_geo'], filtered_df['y_geo']), crs=gdf_boundaries.crs
  )
  gdf.drop(columns=['row_index','col_index','x_geo','y_geo'],inplace = True)
  gdf = gdf.to_crs(gdf_boundaries.crs)
  #result_gdf = gdf.sjoin (gdf_boundaries, how="right", predicate='intersects')
  #result_gdf = result_gdf.dissolve(by='name', aggfunc='sum')
  #print (gdf_boundaries['geometry'])
  result_arr = []
  for idx, row in gdf_boundaries.iterrows():
    #print (col_start,idx)
    # Filter gdf2 where geometry is within geometry1
    within_gdf = gdf[row['geometry'].contains(gdf['geometry'])]    
#     # # If there are geometries within, sum their values in column B
    if not within_gdf.empty:
      print (within_gdf)
#     #   result_arr.append ([df_boundaries.loc[idx]['iso3_country_code'], df_boundaries.loc[idx]['admin_name'], within_gdf['value'].sum(),df_boundaries.loc[idx]['geometry_x']])
#   #print (result_arr)
# #  result_df = result_df.group_by(by='name',aggfunc='sum')
#   del [[filtered_df,gdf]]
  gc.collect()
  return result_arr


# Get data about admin level boundaries and select specific level

world_boundaries = gpd.read_file(world_boundaries_path)
l2_boundaries = gpd.read_file(l2_boundaries_path)
df_education = pd.read_csv(education_share_path)
df_education.drop(columns=["Year"],inplace = True)
df_education = df_education.groupby(by=['Code']).mean(numeric_only=True)
df_ceei = pd.read_excel(open(ceei_index_path, 'rb'),sheet_name='iData')  

df_boundaries = world_boundaries.merge (l2_boundaries, left_on="iso3", right_on="iso3_country_code")
# for col in df_ceei.columns:
#     print(col)

# print (len(df_boundaries))
# for col in df_boundaries.columns:
#     print(col)

#df_boundaries = df_boundaries.merge (df_ceei, left_on="iso3", right_on="iso3_country_code")

df_boundaries['tertiary_edu_share'] = 0
for key, item in df_education['Combined'].items():
  # print(key, item)
  df_boundaries['tertiary_edu_share'] = np.where (df_boundaries['iso3_country_code'] == key, item, df_boundaries['tertiary_edu_share'])

#drop unnecessary columns
df_boundaries.drop(columns=['geometry_y','adminid','fid', 'french_short', 'iso_3166_1_alpha_2_codes','color_code','status','geo_point_2d'],inplace = True)
#,'shapeID','uName','rwi','pvout','windspeed','eletdens','emission','accesselect','loans','netimports','ffshare','invest','emission_USD','emission_HDI'
print (len(df_boundaries))
#convert to geodataframe
gdf_boundaries = gpd.GeoDataFrame(
    df_boundaries, geometry=df_boundaries['geometry_x'], crs=world_boundaries.crs
  )
del [[world_boundaries,l2_boundaries,df_education, df_ceei]]
gc.collect()

# for col in gdf_boundaries.columns:
#     print(col)

# print (len(gdf_boundaries))
  
output_arr = []

# Open and process population geotiff
src_ds = gdal.OpenEx(population_path, gdal.OF_READONLY|gdal.OF_RASTER)
GT = src_ds.GetGeoTransform()
rows = src_ds.RasterYSize
cols = src_ds.RasterXSize
for band in range( src_ds.RasterCount ):
  band += 1
  srcband = src_ds.GetRasterBand(band)

  # Calculate block size based on number of processes
  block_rows, remainder = divmod(rows, num_processes)
  block_cols, remainder = divmod(cols, num_processes)
  # Handle remaining rows/cols for the last process
  if remainder > 0:
      block_rows += 1 if remainder % num_processes == 0 else remainder // num_processes
      block_cols += 1 if remainder % num_processes == 0 else remainder // num_processes

  # Create tasks with block boundaries
  tasks = []
  for process_id in range(num_processes):
      row_start = process_id * block_rows
      row_end = min((process_id + 1) * block_rows, rows)
      col_start = process_id * block_cols
      col_end = min((process_id + 1) * block_cols, cols)
      tasks.append((population_path, band, row_start, row_end, col_start, col_end))

  #Use multiprocessing pool to process blocks in parallel
  with mp.Pool(processes=num_processes) as pool:
    result = pool.starmap(mp_func, tasks)
    for item in result:
      output_arr = output_arr + item

print ("----------------------------------------------------")
print (output_arr)
columns = ['admin_name','iso3_country_code','population','geometry']
df = pd.DataFrame(output_arr, columns=columns)
df.to_csv('l3.csv', index=False)
print ("done")