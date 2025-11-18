#!/usr/bin/env python3
"""
Soybean County Coordinates Processor
Handles trailing whitespace in column names from Census gazetteer file
"""

import pandas as pd
import sys

# Read the gazetteer file
print("Reading gazetteer file...")
df = pd.read_csv('data/2024_Gaz_counties_national.txt', sep='\t', encoding='latin1')

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
    
    # Convert to numeric (in case there are any string values)
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
    print("  ERROR: No eastern counties found!")

# ==============================================
# PART 3: Northern Missouri
# ==============================================
print("\nProcessing northern Missouri...")
mo_data = df[df['USPS'] == 'MO'].copy()
if len(mo_data) > 0:
    # Convert to numeric
    mo_data['INTPTLAT'] = pd.to_numeric(mo_data['INTPTLAT'], errors='coerce')
    
    median_lat = mo_data['INTPTLAT'].median()
    northern_mo = mo_data[mo_data['INTPTLAT'] > median_lat]
    print(f"  ✓ MO: Median lat={median_lat:.2f}, Found {len(northern_mo)} northern counties")
else:
    northern_mo = pd.DataFrame()
    print("  ERROR: No data found for Missouri")

# ==============================================
# COMBINE ALL SOYBEAN REGIONS
# ==============================================
print("\nCombining all regions...")
all_soybean_counties = pd.concat([
    full_coverage,
    eastern_coverage,
    northern_mo
], ignore_index=True)

# Remove duplicates if any
all_soybean_counties = all_soybean_counties.drop_duplicates(subset=['GEOID'])

# Select and rename columns
output = all_soybean_counties[['USPS', 'NAME', 'GEOID', 'INTPTLAT', 'INTPTLONG']].copy()
output.columns = ['State', 'County', 'FIPS', 'Latitude', 'Longitude']

# Convert lat/lon to numeric
output['Latitude'] = pd.to_numeric(output['Latitude'], errors='coerce')
output['Longitude'] = pd.to_numeric(output['Longitude'], errors='coerce')

# Sort by state and county
output = output.sort_values(['State', 'County']).reset_index(drop=True)

# Save to CSV
output_file = 'soybean_counties_coordinates.csv'
output.to_csv(output_file, index=False)

print(f"\n{'='*70}")
print("SUCCESS! ✓")
print(f"{'='*70}")
print(f"Total counties exported: {len(output)}")
print(f"\nCounties by state:")
print(output['State'].value_counts().sort_index())
print(f"\nSample output (first 10 rows):")
print(output.head(10).to_string())
print(f"\nFile saved as: {output_file}")

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
northern_mo_count = output[(output['State'] == 'MO')].shape[0]

print(f"Full coverage states (IA, MN, WI, IL, IN, OH): {full_count}")
print(f"Eastern portions (ND, SD, NE, KS, MI): {eastern_count}")
print(f"Northern Missouri: {northern_mo_count}")
print(f"TOTAL: {len(output)}")
