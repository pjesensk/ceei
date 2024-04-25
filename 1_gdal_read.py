import rasterio 

#from osgeo import gdal

raster_vrt = rasterio.open(r'./hrsl_general/cog_globallat_10_lon_30_general-v1.5.6.tif')

# Ensure .xml is included in allowed extensions
with rasterio.Env(CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif,.xml"):
  with rasterio.open(raster_vrt) as src:
    print (src)
    # Access data and metadata from the GeoTIFF
    data = src.read(1)  # Read data from band 1
    nodata_value = src.nodata
    # ... other operations on the dataset

    # Information from aux.xml might be automatically loaded
    # depending on the specific data stored in aux.xml

