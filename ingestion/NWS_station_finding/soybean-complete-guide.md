# Soybean Growing Regions: County Coordinates & ZIP Codes Guide

**Last Updated:** November 11, 2025

## Quick Start

This guide provides step-by-step instructions for obtaining latitude/longitude coordinates for soybean-growing counties and ZIP codes for Mississippi River region counties.

### What You'll Create

1. **soybean_counties_coordinates.csv** (~850 counties)
   - Full coverage states + Eastern portions + Northern Missouri
   - Columns: State, County, FIPS, Latitude, Longitude

2. **mississippi_river_county_zips.csv** (1,500-2,000+ ZIP codes)
   - Mississippi River counties across 6 states
   - Columns: State, County_FIPS, ZIP

---

## Part 1: Download & Process County Coordinates

### Step 1: Download Census Gazetteer File

The 2024 file works perfectly (2025 is not yet released):

```bash
cd ./data
wget "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_counties_national.zip" -O counties_2024.zip
unzip counties_2024.zip
```

**Note:** The file name in the zip will be `2024_Gaz_counties_national.txt` (may vary slightly).

### Step 2: Process Counties with Python

Use this corrected script that handles the trailing whitespace in column names:

```python
#!/usr/bin/env python3
"""
Soybean County Coordinates Processor
Handles trailing whitespace in column names from Census gazetteer file
"""

import pandas as pd
import sys

# Read the gazetteer file
print("Reading gazetteer file...")
df = pd.read_csv('2024_Gaz_counties_national.txt', sep='\t', encoding='latin1')

# CRITICAL FIX: Strip whitespace from all column names
# The Census file has trailing spaces in column names!
df.columns = df.columns.str.strip()

print("Cleaned columns:")
print(df.columns.tolist())
print(f"Total counties in file: {len(df)}")

# Verify the columns exist now
if 'INTPTLONG' not in df.columns:
    print("ERROR: INTPTLONG still not found!")
    sys.exit(1)

if 'INTPTLAT' not in df.columns:
    print("ERROR: INTPTLAT still not found!")
    sys.exit(1)

print("✓ Columns found successfully\n")

# ==============================================
# PART 1: Full Coverage States
# ==============================================
print("Processing full coverage states...")
full_states = ['IA', 'MN', 'WI', 'IL', 'IN', 'OH']
full_coverage = df[df['USPS'].isin(full_states)].copy()
print(f"  ✓ Found {len(full_coverage)} counties in full coverage states")

# ==============================================
# PART 2: Eastern Portions
# ==============================================
print("\nProcessing eastern portions...")
eastern_states = ['ND', 'SD', 'NE', 'KS', 'MI']
eastern_counties_list = []

for state in eastern_states:
    state_data = df[df['USPS'] == state].copy()
    if len(state_data) == 0:
        print(f"  WARNING: No data found for state {state}")
        continue
    
    # Convert to numeric
    state_data['INTPTLONG'] = pd.to_numeric(state_data['INTPTLONG'], errors='coerce')
    
    # Use median longitude as threshold
    median_lon = state_data['INTPTLONG'].median()
    eastern = state_data[state_data['INTPTLONG'] > median_lon]
    print(f"  ✓ {state}: Median lon={median_lon:.2f}, Found {len(eastern)} eastern counties")
    eastern_counties_list.append(eastern)

if eastern_counties_list:
    eastern_coverage = pd.concat(eastern_counties_list, ignore_index=True)
else:
    eastern_coverage = pd.DataFrame()

# ==============================================
# PART 3: Northern Missouri
# ==============================================
print("\nProcessing northern Missouri...")
mo_data = df[df['USPS'] == 'MO'].copy()
if len(mo_data) > 0:
    mo_data['INTPTLAT'] = pd.to_numeric(mo_data['INTPTLAT'], errors='coerce')
    
    median_lat = mo_data['INTPTLAT'].median()
    northern_mo = mo_data[mo_data['INTPTLAT'] > median_lat]
    print(f"  ✓ MO: Median lat={median_lat:.2f}, Found {len(northern_mo)} northern counties")
else:
    northern_mo = pd.DataFrame()

# ==============================================
# COMBINE ALL SOYBEAN REGIONS
# ==============================================
print("\nCombining all regions...")
all_soybean_counties = pd.concat([
    full_coverage,
    eastern_coverage,
    northern_mo
], ignore_index=True)

# Remove duplicates
all_soybean_counties = all_soybean_counties.drop_duplicates(subset=['GEOID'])

# Select and rename columns
output = all_soybean_counties[['USPS', 'NAME', 'GEOID', 'INTPTLAT', 'INTPTLONG']].copy()
output.columns = ['State', 'County', 'FIPS', 'Latitude', 'Longitude']

# Convert to numeric
output['Latitude'] = pd.to_numeric(output['Latitude'], errors='coerce')
output['Longitude'] = pd.to_numeric(output['Longitude'], errors='coerce')

# Sort
output = output.sort_values(['State', 'County']).reset_index(drop=True)

# Save
output.to_csv('soybean_counties_coordinates.csv', index=False)

print(f"\n{'='*70}")
print("SUCCESS! ✓")
print(f"{'='*70}")
print(f"Total counties exported: {len(output)}")
print(f"\nCounties by state:")
print(output['State'].value_counts().sort_index())
print(f"\nSample output (first 10 rows):")
print(output.head(10).to_string(index=False))
print(f"\nFile saved as: soybean_counties_coordinates.csv")

# Data Quality Check
print(f"\n{'='*70}")
print("Data Quality Check")
print(f"{'='*70}")
print(f"Rows with missing Latitude: {output['Latitude'].isna().sum()}")
print(f"Rows with missing Longitude: {output['Longitude'].isna().sum()}")
print(f"Latitude range: {output['Latitude'].min():.4f} to {output['Latitude'].max():.4f}")
print(f"Longitude range: {output['Longitude'].min():.4f} to {output['Longitude'].max():.4f}")

# Summary by region
print(f"\n{'='*70}")
print("Counties by Region")
print(f"{'='*70}")

full_count = output[output['State'].isin(full_states)].shape[0]
eastern_count = output[output['State'].isin(eastern_states)].shape[0]
northern_mo_count = output[output['State'] == 'MO'].shape[0]

print(f"Full coverage states (IA, MN, WI, IL, IN, OH): {full_count}")
print(f"Eastern portions (ND, SD, NE, KS, MI): {eastern_count}")
print(f"Northern Missouri: {northern_mo_count}")
print(f"TOTAL: {len(output)}")
```

**Save as:** `processing_counties.py`

**Run it:**
```bash
python3 processing_counties.py
```

---

## Part 2: Download & Process Mississippi River ZIP Codes

### Step 1: Download HUD ZIP-County Crosswalk

Download from HUD website:
- URL: https://www.huduser.gov/portal/datasets/usps_crosswalk.html
- Select: Q4 2024 (or latest)
- Download: COUNTY (to ZIP) file
- Save to: `data/ZIP_COUNTY_122024.xlsx`

**Important:** The direct download URL varies. Use the HUD website interface or:

```bash
cd ./data

# Try this alternative download from archive
python3 << 'EOF'
import pandas as pd
import urllib.request

url = "https://www.icpsr.umich.edu/download/62567/ZIP_COUNTY_122024.xlsx"
try:
    print("Downloading from ICPSR...")
    urllib.request.urlretrieve(url, "ZIP_COUNTY_122024.xlsx")
    print("✓ Downloaded successfully!")
except:
    print("Download failed. Please download manually from HUD website.")
    print("https://www.huduser.gov/portal/datasets/usps_crosswalk.html")
EOF
```

### Step 2: Install openpyxl (for Excel support)

```bash
conda install -c conda-forge openpyxl
# OR
pip install openpyxl
```

### Step 3: Process Mississippi River ZIP Codes

First, inspect the file to confirm columns:

```bash
python3 << 'EOF'
import pandas as pd

df = pd.read_excel('data/ZIP_COUNTY_122024.xlsx', sheet_name='Export Worksheet')

print("Actual columns in file:")
print(df.columns.tolist())
print(f"\nFirst few rows:")
print(df.head())
print(f"\nData shape: {df.shape}")
EOF
```

Expected columns:
- `ZIP` - ZIP code
- `COUNTY` - 5-digit FIPS code
- `USPS_ZIP_PREF_STATE` - State abbreviation
- `RES_RATIO`, `BUS_RATIO`, `OTH_RATIO`, `TOT_RATIO` - Ratios

### Step 4: Extract ZIP Codes

Use this corrected script:

```python
#!/usr/bin/env python3
"""
Extract ZIP codes for Mississippi River counties
Fixed for actual HUD USPS ZIP-County Crosswalk column names
"""

import pandas as pd
import sys

print("Reading HUD ZIP-County crosswalk...")
try:
    zip_crosswalk = pd.read_excel('data/ZIP_COUNTY_122024.xlsx', sheet_name='Export Worksheet')
except FileNotFoundError:
    print("ERROR: data/ZIP_COUNTY_122024.xlsx not found!")
    sys.exit(1)

print(f"✓ Loaded {len(zip_crosswalk)} records")
print(f"Columns: {zip_crosswalk.columns.tolist()}\n")

# ==============================================
# MISSISSIPPI RIVER COUNTY FIPS CODES
# ==============================================

ms_river_fips = {
    # Missouri (17 counties)
    29031, 29045, 29099, 29111, 29113, 29127, 29133, 
    29143, 29155, 29157, 29163, 29173, 29201, 29183, 
    29189, 29186, 29510,
    
    # Arkansas (9 counties)
    5001, 5017, 5027, 5035, 5041, 5043, 5095, 5107, 5123,
    
    # Louisiana (18 parishes)
    22021, 22025, 22029, 22035, 22041, 22051, 22059, 22065,
    22067, 22073, 22083, 22087, 22107, 22111, 22113, 22127,
    22133, 22135,
    
    # Mississippi (18 counties)
    28011, 28015, 28027, 28033, 28053, 28059, 28083, 28085,
    28125, 28133, 28135, 28143, 28149, 28151, 28153, 28157,
    28163, 28175,
    
    # Tennessee (5 counties)
    47045, 47095, 47097, 47157, 47167,
    
    # Kentucky (4 counties)
    21007, 21039, 21075, 21105,
}

print(f"Mississippi River county FIPS codes: {len(ms_river_fips)}")
print("States: MO, AR, LA, MS, TN, KY\n")

# ==============================================
# FILTER FOR MISSISSIPPI RIVER COUNTIES
# ==============================================
print("Filtering for Mississippi River counties...")

# Convert COUNTY to numeric
zip_crosswalk['COUNTY_NUM'] = pd.to_numeric(zip_crosswalk['COUNTY'], errors='coerce')

# Filter
ms_river_data = zip_crosswalk[zip_crosswalk['COUNTY_NUM'].isin(ms_river_fips)].copy()

print(f"✓ Found {len(ms_river_data)} ZIP-County combinations\n")

# Create output
output = ms_river_data[['USPS_ZIP_PREF_STATE', 'COUNTY', 'ZIP']].drop_duplicates().copy()
output.columns = ['State', 'County_FIPS', 'ZIP']
output = output.sort_values(['State', 'County_FIPS', 'ZIP']).reset_index(drop=True)

# Format FIPS as 5-digit
output['County_FIPS'] = output['County_FIPS'].astype(str).str.zfill(5)

# Save
output.to_csv('mississippi_river_county_zips.csv', index=False)

print(f"\n{'='*70}")
print("SUCCESS! ✓")
print(f"{'='*70}")
print(f"Total ZIP-County records: {len(output)}")
print(f"Unique ZIP codes: {output['ZIP'].nunique()}")
print(f"States covered: {output['State'].nunique()}")
print(f"\nZIP codes by state:")
print(output['State'].value_counts().sort_index())
print(f"\nSample output (first 15 rows):")
print(output.head(15).to_string(index=False))
print(f"\nFile saved as: mississippi_river_county_zips.csv")

# State breakdown
print(f"\n{'='*70}")
print("State Breakdown")
print(f"{'='*70}")
for state in sorted(output['State'].unique()):
    state_data = output[output['State'] == state]
    state_zips = state_data['ZIP'].nunique()
    state_counties = state_data['County_FIPS'].nunique()
    print(f"{state}: {state_counties} counties, {state_zips} ZIP codes")
```

**Save as:** `processing_mississippi_river_final.py`

**Run it:**
```bash
python3 processing_mississippi_river_final.py
```

---

## Troubleshooting

### Column Name Errors

**Problem:** `KeyError: 'INTPTLONG' not in index`

**Solution:** The Census file has trailing whitespace in column names. The script includes:
```python
df.columns = df.columns.str.strip()
```

This automatically fixes it.

### Missing openpyxl

**Problem:** `ImportError: Missing optional dependency 'openpyxl'`

**Solution:**
```bash
conda install -c conda-forge openpyxl
```

### HUD Download Issues

**Problem:** Direct download URL returns 404

**Solution:** Download manually from https://www.huduser.gov/portal/datasets/usps_crosswalk.html

---

## Output Files

### soybean_counties_coordinates.csv

Columns: `State`, `County`, `FIPS`, `Latitude`, `Longitude`

Example:
```
State,County,FIPS,Latitude,Longitude
IA,Adair County,19001,41.330739,-94.471068
IA,Adams County,19003,41.029198,-94.699593
IL,Alexander County,17001,37.254155,-88.896722
...
```

**Typical counts:**
- Full coverage (IA, MN, WI, IL, IN, OH): ~540 counties
- Eastern portions (ND, SD, NE, KS, MI): ~200 counties
- Northern Missouri: ~60 counties
- **Total: ~800 counties**

### mississippi_river_county_zips.csv

Columns: `State`, `County_FIPS`, `ZIP`

Example:
```
State,County_FIPS,ZIP
AR,05001,71601
AR,05001,71603
KY,21007,42025
KY,21039,42033
LA,22021,71209
...
```

**Typical counts:**
- 6 states
- 60-70 counties
- 1,500-2,000 ZIP codes

---

## Data Architecture Integration

### For Kafka/Streaming Pipelines

1. Load `soybean_counties_coordinates.csv` into a reference table
2. Use it to:
   - Filter NWS weather station data by region
   - Correlate commodity prices with geographic areas
   - Build geographic partitions for event streams

3. Load `mississippi_river_county_zips.csv` for:
   - Geofencing specific regions
   - Regional aggregation
   - Flood/water level monitoring correlations

### For BigQuery/GCP

```sql
-- Load soybean counties
LOAD DATA INTO my_project.geo.soybean_counties
FROM FILES (
  format = 'CSV',
  uris = ['gs://my-bucket/soybean_counties_coordinates.csv']
);

-- Load Mississippi River ZIPs
LOAD DATA INTO my_project.geo.mississippi_river_zips
FROM FILES (
  format = 'CSV',
  uris = ['gs://my-bucket/mississippi_river_county_zips.csv']
);
```

### For Feature Store (Feast)

```yaml
sources:
  - name: soybean_counties
    type: file
    path: soybean_counties_coordinates.csv
    
  - name: ms_river_zips
    type: file
    path: mississippi_river_county_zips.csv

features:
  - name: county_location
    source: soybean_counties
    columns: [latitude, longitude]
    
  - name: river_region
    source: ms_river_zips
    columns: [zip_code]
```

---

## Summary

| Dataset | Records | States | Key Columns |
|---------|---------|--------|------------|
| Soybean Counties | ~850 | 12 | State, County, FIPS, Lat, Lon |
| MS River ZIPs | ~2000 | 6 | State, County_FIPS, ZIP |

**Both datasets are production-ready for:**
- Real-time data streaming (Kafka/Pub-Sub)
- Data warehouse loading (BigQuery)
- Feature engineering (Feast/Tecton)
- GIS analysis (spatial joins)
- API reference services

---

## References

- U.S. Census Bureau Gazetteer Files: https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html
- HUD USPS ZIP-County Crosswalk: https://www.huduser.gov/portal/datasets/usps_crosswalk.html
- USDA NASS Soybean Data: https://www.nass.usda.gov/Statistics_by_Subject/result.php?commodity=SOYBEANS
