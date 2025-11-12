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
# These are the 5-digit FIPS codes for counties bordering the Mississippi River

ms_river_fips = {
    # Missouri (17 counties)
    29031,  # Cape Girardeau
    29045,  # Clark
    29099,  # Jefferson
    29111,  # Lewis
    29113,  # Lincoln
    29127,  # Marion
    29133,  # Mississippi
    29143,  # New Madrid
    29155,  # Pemiscot
    29157,  # Perry
    29163,  # Pike
    29173,  # Ralls
    29201,  # Scott
    29183,  # St. Charles
    29189,  # St. Louis County
    29186,  # Ste. Genevieve
    29510,  # St. Louis City
    
    # Arkansas (15 counties - Delta region on Mississippi)
    5001,   # Arkansas
    5017,   # Chicot
    5027,   # Crittenden
    5035,   # Desha
    5041,   # Greene
    5043,   # Lee
    5095,   # Mississippi
    5107,   # Phillips
    5123,   # St. Francis
    
    # Louisiana (18+ parishes)
    22021,  # Caldwell
    22025,  # Catahoula
    22029,  # Concordia
    22035,  # East Carroll
    22041,  # Franklin
    22051,  # Iberville
    22059,  # LaSalle
    22065,  # Madison
    22067,  # Morehouse
    22073,  # Ouachita
    22083,  # Plaquemines
    22087,  # Pointe Coupee
    22107,  # Richland
    22111,  # St. Bernard
    22113,  # St. Charles
    22127,  # Tensas
    22133,  # West Baton Rouge
    22135,  # West Carroll
    
    # Mississippi (19+ Delta counties)
    28011,  # Bolivar
    28015,  # Carroll
    28027,  # Coahoma
    28033,  # De Soto
    28053,  # Grenada
    28059,  # Holmes
    28083,  # Humphreys
    28085,  # Issaquena
    28125,  # Leflore
    28133,  # Panola
    28135,  # Quitman
    28143,  # Sharkey
    28149,  # Sunflower
    28151,  # Tallahatchie
    28153,  # Tate
    28157,  # Tunica
    28163,  # Warren
    28175,  # Yazoo
    
    # Tennessee (5 counties - West Tennessee on river)
    47045,  # Dyer
    47095,  # Lake
    47097,  # Lauderdale
    47157,  # Shelby
    47167,  # Tipton
    
    # Kentucky (4 counties - Northwestern Kentucky)
    21007,  # Ballard
    21039,  # Carlisle
    21075,  # Fulton
    21105,  # Hickman
}

print(f"Mississippi River county FIPS codes: {len(ms_river_fips)}")
print("States: MO, AR, LA, MS, TN, KY\n")

# ==============================================
# FILTER FOR MISSISSIPPI RIVER COUNTIES
# ==============================================
print("Filtering for Mississippi River counties...")

# Convert COUNTY column to numeric (in case it's stored as string)
zip_crosswalk['COUNTY_NUM'] = pd.to_numeric(zip_crosswalk['COUNTY'], errors='coerce')

# Filter for our river counties
ms_river_data = zip_crosswalk[zip_crosswalk['COUNTY_NUM'].isin(ms_river_fips)].copy()

print(f"✓ Found {len(ms_river_data)} ZIP-County combinations\n")

# Create output dataframe with correct column names
output = ms_river_data[['USPS_ZIP_PREF_STATE', 'COUNTY', 'ZIP']].drop_duplicates().copy()
output.columns = ['State', 'County_FIPS', 'ZIP']
output = output.sort_values(['State', 'County_FIPS', 'ZIP']).reset_index(drop=True)

# Convert FIPS to 5-digit format for readability
output['County_FIPS'] = output['County_FIPS'].astype(str).str.zfill(5)

# Save to CSV
output_file = 'mississippi_river_county_zips.csv'
output.to_csv(output_file, index=False)

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
print(f"\nFile saved as: {output_file}")

# Additional statistics
print(f"\n{'='*70}")
print("State Breakdown")
print(f"{'='*70}")
for state in sorted(output['State'].unique()):
    state_data = output[output['State'] == state]
    state_zips = state_data['ZIP'].nunique()
    state_counties = state_data['County_FIPS'].nunique()
    print(f"{state}: {state_counties} counties, {state_zips} ZIP codes")
