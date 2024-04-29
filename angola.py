import pandas as pd
import geopandas as gpd
import json

boundaries = './data/world-administrative-boundaries.csv'
powerplants = './data/global_power_plant_database.csv'
jobs = './data/IRENA_RE_Jobs_Annual_Review_2023.xlsx'

df_boundaries = pd.read_csv(boundaries, delimiter=';', low_memory=False)
df_boundaries = df_boundaries.loc[df_boundaries['ISO 3 country code'] == 'AGO']
df_powerplants = pd.read_csv(powerplants, delimiter=',', low_memory=False)
df_powerplants = df_powerplants.loc[df_powerplants['country'] == 'AGO']
df_powerplants = df_powerplants.loc[df_powerplants['primary_fuel'] == 'Hydro']
df_jobs = pd.read_excel(jobs)
df_jobs = df_jobs.loc[df_jobs['Country/area'] == 'Angola']
df_jobs = df_jobs.loc[df_jobs['Technology'] == 'Hydropower']

merged_df = df_powerplants.merge(df_boundaries, right_on="ISO 3 country code", left_on="country")
merged_df = merged_df.merge(df_jobs,left_on="country_long", right_on="Country/area")

print (merged_df.head())
features = []
for col in merged_df.columns:
    print(col)

merged_df['computed_workforce'] = merged_df['Jobs (thousand)']*1000*merged_df['capacity_mw']/merged_df['capacity_mw'].sum()
merged_df['computed_workforce_high'] = merged_df['computed_workforce']*6/100
for index, row in merged_df.iterrows():
    print(row)
    feature = {
      "type": "Feature",
      "properties": {
        "computed_workforce": row['computed_workforce'],
        "computed_workforce_high": row['computed_workforce_high'],
        "tags": {},
        "longitude_latitude":{
          "type": "Point",
          "coordinates": [row['latitude'],row['longitude']]
        }
      },
      "geometry":{
        "type": "Point",
        "coordinates": [row['latitude'],row['longitude']]
      }
    }
    features.append (feature)

output_json = {
  "type": "FeatureCollection",
  "features": 
    features
}
print (json.dumps(output_json))
