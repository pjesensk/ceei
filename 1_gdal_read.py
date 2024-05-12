import numpy as np
import pandas as pd
import multiprocessing as mp
from multiprocessing import Pool
from shapely.geometry import Polygon
from shapely.geometry import Point
import os
import json
import time
from osgeo import gdal
from osgeo import gdal_array
from itertools import chain

num_processes = mp.cpu_count()

def mp_func (src_path, band, row_start, row_end, col_start, col_end):
  global GT
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

  print (df[df['value']>0])

#src_path = './data/landscan-global-2022.tif'
#src_path = './data/Education_Index.tiff'
src_path = './data/Sub_National_HDI.tiff'
src_ds = gdal.OpenEx(src_path, gdal.OF_READONLY|gdal.OF_RASTER)
print ("[ RASTER BAND COUNT ]: ", src_ds.RasterCount)
GT = src_ds.GetGeoTransform()
rows = src_ds.RasterYSize
cols = src_ds.RasterXSize
print (GT)
for band in range( src_ds.RasterCount ):
  band += 1
  print ("[ GETTING BAND ]: ", band)
  srcband = src_ds.GetRasterBand(band)
  #print (srcband.GetStatistics( True, True ))
  print (srcband.GetMetadata())
#    stats = srcband.GetStatistics( True, True )
#    print ("[ STATS ] =  Minimum=%.3f, Maximum=%.3f, Mean=%.3f, StdDev=%.3f" % ( \
#            stats[0], stats[1], stats[2], stats[3] ))

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
      tasks.append((src_path, band, row_start, row_end, col_start, col_end))

  print (tasks)
  # Use multiprocessing pool to process blocks in parallel
  with mp.Pool(processes=num_processes) as pool:
      results = pool.starmap(mp_func, tasks)