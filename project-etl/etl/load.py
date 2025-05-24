import psycopg2

def load_data_to_db():
    conn = psycopg2.connect(
        dbname="mydatabase", user="postgres", password="password", host="localhost", port="5432"
    )
    cur = conn.cursor()

    try:
        with open('data/continents2_clean.csv', 'r') as f:
            next(f)  # Skip header row
            cur.copy_expert("COPY country_fact FROM STDIN WITH CSV", f)

        with open('data/per-capita-energy-use_clean.csv', 'r') as f:
            next(f)
            cur.copy_expert("COPY energy_dimension FROM STDIN WITH CSV", f)

        with open('data/air-pollution_clean.csv', 'r') as f:
            next(f)
            cur.copy_expert("COPY air_pollution_dimension FROM STDIN WITH CSV", f)

        with open('data/co2_emission_clean.csv', 'r') as f:
            next(f)
            cur.copy_expert("COPY co2_emission_dimension FROM STDIN WITH CSV", f)

        with open('data/world_risk_index_clean.csv', 'r') as f:
            next(f)
            cur.copy_expert("COPY risk_dimension FROM STDIN WITH CSV", f)

        with open('data/share-of-the-population-with-access-to-electricity_clean.csv', 'r') as f:
            next(f)
            cur.copy_expert("COPY access_dimension FROM STDIN WITH CSV", f)

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error loading data: {e}")
    finally:
        cur.close()
        conn.close()

# Call the function to load data
load_data_to_db()
