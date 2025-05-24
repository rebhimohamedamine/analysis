# 🌍 Climate Change ETL Pipeline

## Overview

This project is an **ETL (Extract, Transform, Load)** pipeline developed in Python to automate the processing of cleaned climate change datasets. The pipeline loads multiple CSV files, applies transformations such as cleaning and normalization, and loads the structured data into a **PostgreSQL** database for further exploration and analysis.

---

## 📁 Project Structure

project-etl/

└── etl/

├── pycache/

├── data/

│ ├── air-pollution_clean.csv

│ ├── co2_emission_clean.csv

│ ├── continents2_clean.csv

│ ├── per-capita-energy-use_clean.csv

│ ├── share-of-the-population-witout access-to-clean-fuels-for-cooking_clean.csv

│ ├── world_risk_index_clean.csv

│ ├── Untitled document.docx

│ └── co2_weather_energy_analysis.ipynb

├── extract.py

├── transform.py

└── load.py


---

## 📊 Datasets Used

- `co2_emission_clean.csv`: CO₂ emission data by country and year
- `air-pollution_clean.csv`: Air pollution statistics (PM2.5 etc.)
- `per-capita-energy-use_clean.csv`: Per capita energy consumption
- `share-of-the-population-wit...csv`: Clean fuel access data
- `continents2_clean.csv`: Mapping countries to continents
- `world_risk_index_clean.csv`: Global risk index scores

---

## 🔄 ETL Workflow

### ✅ Extract
- Load all CSV files from the `data/` directory into Pandas DataFrames.
- Log missing files or format issues.

### 🔧 Transform
- Clean column names
- Standardize formats (e.g. lowercase, underscores)
- (Optional) Merge or normalize datasets

### 🛢 Load
- Connect to PostgreSQL using `psycopg2`
- Auto-create tables based on CSV headers
- Load transformed data into PostgreSQL tables

---

## ⚙️ PostgreSQL Configuration

In `load.py`, update your database connection settings:

```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'climate_db',
    'user': 'your_username',
    'password': 'your_password'
}
```
---

🚀 Clone & Run This Project

```bash
git clone https://github.com/rebhimohamedamine/analysis.git

cd analysis/etl

pip install pandas psycopg2

python extract.py
python transform.py
python load.py


