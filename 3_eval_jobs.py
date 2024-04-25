import pandas as pd
import geopandas as gpd
import json

data = pd.read_parquet ('./data.parquet')

pv_year_jobs = 885/50 #885 employees per year per 50MW solar 
pv_year_jobs_maintenance = pv_year_jobs * 56 / 100 # 56% of jobs is maintenance
pv_year_jobs_low_skill = pv_year_jobs * 64 / 100 # 64% of jobs are low skill
pv_year_jobs_woman = pv_year_jobs * 40 / 100 # woman employed in PV

print ( data.columns.tolist())

for index, row in data.iterrows():
    predicted_needed_jobs = 0.0
    predicted_needed_jobs_percentage = 0.0
    total_jobs_country = row['Jobs (thousand)'] * 1000
    capacity_mw = row['capacity_mw']
    energy_type = row['primary_fuel']
    if energy_type in 'Solar':
        predicted_needed_jobs = capacity_mw * pv_year_jobs
        predicted_needed_jobs_percentage = predicted_needed_jobs * 100 / total_jobs_country
    print (row['country'],row['Region of the territory'],row['capacity_mw'],row['primary_fuel'],row['Technology'],row['Jobs (thousand)'], predicted_needed_jobs, predicted_needed_jobs_percentage)
