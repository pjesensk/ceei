# ceei
UNDP Ceei computation of jobs per renewable energy sources 

## Sources
1. World administrative boundaries
[https://public.opendatasoft.com/explore/dataset/world-administrative-boundaries/export/]
2. L4 boundaries
[https://earthworks.stanford.edu/catalog/sde-columbia-fewsn_1996_kenyaadmn4]


```
gdalinfo -oo INTERLEAVE=PIXEL -oo OffsetsPositive -oo NrOffsets=2 -oo NoGridAxisSwap -oo BandIdentifier=none cog_globallat_10_lon_30_general-v1.5.6.tif
```

gdal_polygonize.py ./data/population/population_AF21_2018-10-01.tif -f "GeoJSON" data.geojson
