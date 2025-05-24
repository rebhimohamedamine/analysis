import pandas as pd
from extract import extract_data

def transform():
    # Extract datasets
    air_pollution, co2_emission, continents, energy_use, electricity_access, risk_index = extract_data()

    # === Transform Continents Dataset ===
    data_country = continents.copy()  # Work on a copy of extracted dataframe

    # Rename columns
    data_country.rename(columns={
        'name': 'country_name',
        'alpha-2': 'alpha_2',
        'alpha-3': 'country_code',
        'country-code': 'country_number',
        'iso_3166-2': 'iso_3166_2',
        'sub-region': 'sub_region',
        'intermediate-region': 'intermediate_region',
        'region-code': 'region_code',
        'sub-region-code': 'sub_region_code',
        'intermediate-region-code': 'intermediate_region_code'
    }, inplace=True)

    # Drop unnecessary columns
    data_country.drop(columns=[
        'alpha_2', 'country_number', 'iso_3166_2',
        'intermediate_region', 'intermediate_region_code'
    ], inplace=True)

    # Handle missing values
    data_country["region"].fillna("Antarctic", inplace=True)
    data_country["sub_region"].fillna("Subantarctic", inplace=True)
    data_country["region_code"].fillna(672, inplace=True)
    data_country["sub_region_code"].fillna(10, inplace=True)

    # Convert to integer
    data_country['region_code'] = data_country['region_code'].astype(int)
    data_country['sub_region_code'] = data_country['sub_region_code'].astype(int)

    # Normalize country_name to lowercase
    data_country['country_name'] = data_country['country_name'].str.strip().str.lower()

    # Save cleaned data
    data_country.to_csv('data/continents2_clean.csv', index=False)
    print("✅ Transformed and saved: continents2_clean.csv")

    # === Transform CO2 Emissions Dataset ===
    data_co2_emission = co2_emission.copy()

    # Rename columns
    data_co2_emission.rename(columns={
        'Entity': 'country_name',
        'Code': 'country_code',
        'Year': 'year',
        'Annual CO₂ emissions (tonnes )': 'annual_co2_emissions'
    }, inplace=True)

    # Drop NaN values
    data_co2_emission = data_co2_emission.dropna()

    # Normalize country_name to lowercase
    data_co2_emission['country_name'] = data_co2_emission['country_name'].str.strip().str.lower()

    # Filter rows where country_name exists in data_country
    data_co2_emission = data_co2_emission[
        data_co2_emission['country_name'].isin(data_country['country_name'])
    ]

    # Save cleaned data
    data_co2_emission.to_csv('data/co2_emission_clean.csv', index=False)
    print("✅ Transformed and saved: co2_emission_clean.csv")

    # === Transform Per Capita Energy Use Dataset ===
    data_per_capita_energy_use = energy_use.copy()

    # Rename columns
    data_per_capita_energy_use.rename(columns={
        'Entity': 'country_name',
        'Code': 'country_code',
        'Year': 'year',
        'Energy consumption per capita (kWh)': 'energy_consumption_per_capita'
    }, inplace=True)

    # Drop NaN values
    data_per_capita_energy_use = data_per_capita_energy_use.dropna()

    # Normalize country_name to lowercase
    data_per_capita_energy_use['country_name'] = data_per_capita_energy_use['country_name'].str.strip().str.lower()

    # Filter rows where country_name exists in data_country
    data_per_capita_energy_use = data_per_capita_energy_use[
        data_per_capita_energy_use['country_name'].isin(data_country['country_name'])
    ]

    # Save cleaned data
    data_per_capita_energy_use.to_csv('data/per-capita-energy-use_clean.csv', index=False)
    print("✅ Transformed and saved: per-capita-energy-use_clean.csv")

    # === Transform Population Access to Electricity Dataset ===
    data_population_access_electricity = electricity_access.copy()

    # Rename columns
    data_population_access_electricity.rename(columns={
        'Entity': 'country_name',
        'Code': 'country_code',
        'Year': 'year',
        'Access to electricity (% of population)': 'access_to_electricity_percent_of_population'
    }, inplace=True)

    # Drop NaN values
    data_population_access_electricity = data_population_access_electricity.dropna()

    # Normalize country_name to lowercase
    data_population_access_electricity['country_name'] = data_population_access_electricity['country_name'].str.strip().str.lower()

    # Filter rows where country_name exists in data_country
    data_population_access_electricity = data_population_access_electricity[
        data_population_access_electricity['country_name'].isin(data_country['country_name'])
    ]

    # Save cleaned data
    data_population_access_electricity.to_csv('data/share-of-the-population-with-access-to-electricity_clean.csv', index=False)
    print("✅ Transformed and saved: share-of-the-population-with-access-to-electricity_clean.csv")

    # === Transform World Risk Index Dataset ===
    data_world_risk_index = risk_index.copy()

    # Rename columns
    data_world_risk_index.rename(columns={
        'Region': 'country_name',
        'WRI': 'wri',
        'Exposure': 'exposure',
        'Vulnerability': 'vulnerability',
        'Susceptibility': 'susceptibility',
        'Lack of Coping Capabilities': 'lack_of_coping_capabilities',
        ' Lack of Adaptive Capacities': 'lack_of_adaptive_capacities',
        'Year': 'year',
        'Exposure Category': 'exposure_category',
        'WRI Category': 'wri_category',
        'Vulnerability Category': 'vulnerability_category',
        'Susceptibility Category': 'susceptibility_category'
    }, inplace=True)

    # Fix specific country name
    data_world_risk_index.loc[data_world_risk_index["country_name"] == "Korea Republic of 4.59", 
                              ["country_name"]] = ["republic of korea"]

    # Replace NaN values
    data_world_risk_index["vulnerability_category"].fillna("Very Low", inplace=True)
    mode_value_wri = data_world_risk_index['wri_category'].mode()[0]
    data_world_risk_index['wri_category'].fillna(mode_value_wri, inplace=True)
    mode_value_adaptive = data_world_risk_index['lack_of_adaptive_capacities'].mode()[0]
    data_world_risk_index['lack_of_adaptive_capacities'].fillna(mode_value_adaptive, inplace=True)

    # Normalize country_name to lowercase
    data_world_risk_index['country_name'] = data_world_risk_index['country_name'].str.strip().str.lower()

    # Filter rows where country_name exists in data_country
    data_world_risk_index = data_world_risk_index[
        data_world_risk_index['country_name'].isin(data_country['country_name'])
    ]

    # Save cleaned data
    data_world_risk_index.to_csv('data/world_risk_index_clean.csv', index=False)
    print("✅ Transformed and saved: world_risk_index_clean.csv")

    # === Transform Air Pollution Dataset ===
    data_air_pollution = air_pollution.copy()

    # Rename columns
    data_air_pollution.rename(columns={
        'Country': 'country_name',
        'Year': 'year',
        'Nitrogen Oxide': 'nitrogen_oxide',
        'Sulphur Dioxide': 'sulphur_dioxide',
        'Carbon Monoxide': 'carbon_monoxide',
        'Organic Carbon': 'organic_carbon',
        'NMVOCs': 'nmvoc',
        'Black Carbon': 'black_carbon',
        'Ammonia': 'ammonia'
    }, inplace=True)

    # Ensure 'year' is integer
    data_air_pollution['year'] = data_air_pollution['year'].astype(int)

    # Normalize country_name to lowercase
    data_air_pollution['country_name'] = data_air_pollution['country_name'].str.strip().str.lower()

    # Filter rows where country_name exists in data_country
    data_air_pollution = data_air_pollution[
        data_air_pollution['country_name'].isin(data_country['country_name'])
    ]

    # Save cleaned data
    data_air_pollution.to_csv('data/air-pollution_clean.csv', index=False)
    print("✅ Transformed and saved: air-pollution_clean.csv")

    # === Compare country names between datasets ===
    countries_in_country_not_in_co2 = set(data_country['country_name']) - set(data_co2_emission['country_name'])
    print(f"Countries in Country dataset but NOT in CO2 dataset ({len(countries_in_country_not_in_co2)}):")
    print(sorted(countries_in_country_not_in_co2))

    countries_in_co2_not_in_country = set(data_co2_emission['country_name']) - set(data_country['country_name'])
    print(f"Countries in CO2 dataset but NOT in Country dataset ({len(countries_in_co2_not_in_country)}):")
    print(sorted(countries_in_co2_not_in_country))

    # Return transformed datasets
    return (data_air_pollution, data_co2_emission, data_country, 
            data_per_capita_energy_use, data_population_access_electricity, data_world_risk_index)

# Run transform function
transform()
