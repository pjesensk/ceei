import gc
import multiprocessing as mp
import pandas as pd
import geopandas as gpd
import numpy as np
from osgeo import gdal

# world administrative boundaries
WORLD_BOUNDARIES_PATH = "./data/world-administrative-boundaries.geojson"
L2_BOUNDARIES_PATH = "./data/admin2.geojson"
EDUCATION_SHARE_PATH = "./data/share-of-the-population-with-completed-tertiary-education.csv"
POPULATION_PATH = './data/landscan-global-2022.tif'
CEEI_INDEX_PATH = './data/full_ceei_data2.xlsx'

#num_processes = mp.cpu_count()
num_processes = 32

#Paralell processing function for raster band
def mp_func (src_path, band, row_start, row_end, col_start, col_end):
    global GT
    global gdf_boundaries
    global output_df
    src_dsx = gdal.OpenEx(src_path, gdal.OF_READONLY|gdal.OF_RASTER)
    srcbandx = src_dsx.GetRasterBand(band)
    band_values = srcbandx.ReadAsArray(col_start,row_start,col_end-col_start,row_end-row_start)
    y_line = np.arange(row_start, row_end)
    x_pixel =np.arange(col_start, col_end)
    xv, yv = np.meshgrid(x_pixel, y_line, indexing='ij')

    x_geo = (GT[0] + xv * GT[1] + yv * GT[2])
    y_geo = (GT[3] + xv * GT[4] + yv * GT[5])

    df = pd.DataFrame({'row_index': xv.ravel(),
                    'col_index': yv.ravel(),
                    'x_geo': x_geo.ravel(),
                    'y_geo': y_geo.ravel(),
                    'value': band_values.ravel()})

    filtered_df = df[df['value']>0]
    gdf = gpd.GeoDataFrame(
      filtered_df, geometry=gpd.points_from_xy(filtered_df['x_geo'], filtered_df['y_geo']), crs=gdf_boundaries.crs
    )
    gdf.drop(columns=['row_index','col_index','x_geo','y_geo'],inplace = True)
    gdf = gdf.to_crs(gdf_boundaries.crs)
    result_arr = []
    for idx, row in gdf_boundaries.iterrows():
      # Filter gdf2 where geometry is within geometry1
      within_gdf = gdf[row['geometry'].contains(gdf['geometry'])]    
      # If there are geometries within, sum their values in column B
      if not within_gdf.empty:
        #we have some items lets create new entry in result array
        r =[]
        for col in gdf_boundaries.columns:
          r.append (gdf_boundaries.loc[idx][col])
        r.append (within_gdf['value'].sum())
        result_arr.append (r)
    del [[filtered_df,gdf]]
    gc.collect()
    return result_arr


###############################
# Processing part 
###############################

# Get data about admin level boundaries and select specific level

world_boundaries = gpd.read_file(WORLD_BOUNDARIES_PATH)
l2_boundaries = gpd.read_file(L2_BOUNDARIES_PATH)
df_education = pd.read_csv(EDUCATION_SHARE_PATH)
africa_education_df = df_education[df_education['Entity'] == 'Africa']
df_education.drop(columns=["Year"],inplace = True)
df_education = df_education.groupby(by=['Code']).mean(numeric_only=True)
df_ceei = pd.read_excel(open(CEEI_INDEX_PATH, 'rb'),sheet_name='iData')  

df_boundaries = world_boundaries.merge (l2_boundaries, left_on="iso3", right_on="iso3_country_code")
df_boundaries = df_boundaries.merge (df_ceei, left_on="admin_name", right_on="uName")
df_boundaries['tertiary_edu_share'] = 0

df_boundaries['tertiary_edu_share'] = np.where (df_boundaries['continent'] == 'Africa' , africa_education_df['Combined'].mean(), df_boundaries['tertiary_edu_share'])
for key, item in df_education['Combined'].items():
    df_boundaries['tertiary_edu_share'] = np.where (df_boundaries['iso3'] == key , item, df_boundaries['tertiary_edu_share'])

#convert to geodataframe
gdf_boundaries = gpd.GeoDataFrame(
    df_boundaries, geometry=df_boundaries['geometry_y'], crs=world_boundaries.crs
    )
#drop unnecessary columns
gdf_boundaries.drop(columns=['geometry_y','geometry_x','adminid','fid', 'french_short', 'iso_3166_1_alpha_2_codes','color_code','status','geo_point_2d','iso3_country_code_x','shapeID'],inplace = True)
gdf_boundaries.drop_duplicates()
#print(len(gdf_boundaries))
#gdf_boundaries.to_csv('combined.csv', index=False)
del [[world_boundaries,l2_boundaries,df_education]]
gc.collect()
  
output_arr = []

# Open and process population geotiff
src_ds = gdal.OpenEx(POPULATION_PATH, gdal.OF_READONLY|gdal.OF_RASTER)
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
        tasks.append((POPULATION_PATH, band, row_start, row_end, col_start, col_end))

    #Use multiprocessing pool to process blocks in parallel
    with mp.Pool(processes=num_processes) as pool:
      result = pool.starmap(mp_func, tasks)
      for item in result:
        output_arr = output_arr + item

print ("----------------------------------------------------")
columns = gdf_boundaries.columns.to_list()
columns.append('population')
df = pd.DataFrame(output_arr, columns=columns)
df.to_csv('ceei_jobs.csv', index=False)
print ("done")