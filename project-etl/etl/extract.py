import pandas as pd

def extract_data():
    try:
        air_pollution = pd.read_csv("../air-pollution.csv")
        co2_emission = pd.read_csv("../co2_emission.csv")
        continents = pd.read_csv("../continents2.csv")
        energy_use = pd.read_csv("../per-capita-energy-use.csv")
        electricity_access = pd.read_csv("../share-of-the-population-with-access-to-electricity.csv")
        risk_index = pd.read_csv("../world_risk_index.csv")

        print("✅ Data Extracted Successfully")

        print("\n📄 Air Pollution")
        print(air_pollution.head())

        print("\n📄 CO2 Emission")
        print(co2_emission.head())

        print("\n📄 Continents")
        print(continents.head())

        print("\n📄 Energy Use")
        print(energy_use.head())

        print("\n📄 Electricity Access")
        print(electricity_access.head())

        print("\n📄 World Risk Index")
        print(risk_index.head())

        return air_pollution, co2_emission, continents, energy_use, electricity_access, risk_index

    except Exception as e:
        print(f"❌ Error during extraction: {e}")
        return None

# Call extract function
extract_data()
