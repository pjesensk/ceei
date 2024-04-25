import pandas as pd

boundaries = '../world-administrative-boundaries.csv'
powerplants = '../global_power_plant_database.csv'
jobs = '../IRENA_RE_Jobs_Annual_Review_2023.xlsx'
output_file = 'data.parquet'

df_boundaries = pd.read_csv(boundaries, delimiter=';', low_memory=False)
df_boundaries = df_boundaries.loc[df_boundaries['Continent of the territory'] == 'Africa']
df_powerplants = pd.read_csv(powerplants, delimiter=',', low_memory=False)
df_jobs = pd.read_excel(jobs)


merged_df = df_boundaries.merge(df_powerplants, left_on="ISO 3 country code", right_on="country")
merged_df = merged_df.merge(df_jobs,left_on="country_long", right_on="Country/area")

# Write merged data to parquet file
merged_df.to_parquet(output_file)

print(f"Combined data saved to: {output_file}")