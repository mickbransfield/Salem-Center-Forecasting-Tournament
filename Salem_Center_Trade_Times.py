# Post 56 - Zubby Badger

import numpy as np
import pandas as pd
import json
import datetime

# Current datetime to use in file name
snapshotdate = datetime.datetime.today().strftime("%Y-%m-%d %H_%M_%S")


### BETS ###

# Path to your bets file
file_bets_path = 'bets.json'

# Read the JSON data from the file
with open(file_bets_path, 'r') as file:
    json_data = json.load(file)

# Normalize the bets data and handle nested structures
all_bets_rows = []
for item in json_data:
    # Flatten 'fees' data
    fees_data = item.get('fees', {})
    for key in fees_data:
        item[f'fees_{key}'] = fees_data[key]

    # Remove the original 'fees' dictionary
    item.pop('fees', None)

    # Handle 'fills' data
    fills_data = item.get('fills', [])
    if not fills_data:
        # If there are no fills, add the item data as is
        all_bets_rows.append(item)
    else:
        # If there are fills, add each fill as a separate row
        for fill in fills_data:
            # Add 'fills_' prefix to each key in the fill data
            fill_with_prefix = {f'fills_{key}': value for key, value in fill.items()}
            combined_data = {**item, **fill_with_prefix}
            # Remove the 'fills' key as it's no longer needed
            combined_data.pop('fills', None)
            all_bets_rows.append(combined_data)

# Create a DataFrame from the combined data
bets_df = pd.DataFrame(all_bets_rows)


### CONTRACTS ###

# Path to your contracts file
file_contracts_path = 'contracts.json'

# Read the JSON data from the file
with open(file_contracts_path, 'r') as file:
    json_contracts_data = json.load(file)

# Normalize the data including the nested structures
contracts_df = pd.json_normalize(
    json_contracts_data, 
    record_path=None, 
    meta=[
        'visibility', 'question', 'creatorName', 'creatorId', 'creatorAvatarUrl', 
        'initialProbability', 'tags', 'outcomeType', 'creatorUsername', 'createdTime', 
        'id', 'mechanism', 'lowercaseTags', 'slug', 'groupSlugs', 
        ['description', 'type'], 
        ['description', 'content'], 
        'totalLiquidity', 'closeTime', 
        ['collectedFees', 'liquidityFee'], 
        ['collectedFees', 'creatorFee'], 
        ['collectedFees', 'platformFee'], 
        'lastBetTime', 'closeEmailsSent', 'resolutionProbability', 'resolutionTime', 
        'resolution', 'isResolved', 'volume24Hours', 'popularityScore', 'volume7Days', 
        'lastCommentTime', 'lastUpdatedTime', 'uniqueBettorIds', 'uniqueBettorCount', 
        'volume', 'pool.NO', 'pool.YES', 'p'
    ],
    errors='ignore'
)


### MERGE BETS & CONTRACTS ###

# Merging the DataFrames
df = pd.merge(bets_df, contracts_df[['id', 'resolution', 'question', 'resolutionTime']], left_on='contractId', right_on='id', how='left')

# Dropping the extra 'id' column from dataframe_B
df = df.drop('id_y', axis=1)

# Create new column if bet was correct
df['Accuracy'] = np.where(df['outcome'] == df['resolution'], 'RIGHT', 'WRONG')

# Filter down to unique bet id
df = df.drop_duplicates(subset='id_x')


### TRADE TIMES ###

# Convert 'createdTime' from Unix time in milliseconds to standard datetime format
df['DateTime'] = pd.to_datetime(df['createdTime'], unit='ms')
df['resolutionTime_formatted'] = pd.to_datetime(df['resolutionTime'], unit='ms')

# Sort by 'contractId' and 'DateTime'
df = df.sort_values(by=['contractId', 'DateTime'])

# Calculate time difference within each 'contractId' in UNIX
df['Time Between Trades Unix'] = df.groupby('contractId')['createdTime'].diff()

# Calculate time difference within each 'contractId'
df['Time Between Trades'] = df.groupby('contractId')['DateTime'].diff()

# Convert 'Time Between Trades' to a more readable format (e.g., days, hours, minutes, seconds, milliseconds)
def format_timedelta(td):
    if pd.isna(td):
        return None
    days = td.days
    hours, remainder = divmod(td.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    milliseconds = int(td.microseconds / 1000)
    return f'{days}d {hours}h {minutes}m {seconds}s {milliseconds}ms'

df['Time Between Trades'] = df['Time Between Trades'].apply(format_timedelta)

# Create column of time between trade and close time
df['Time to Resolution Unix'] = df['resolutionTime'] - df['createdTime']
df['Time to Resolution'] = df['resolutionTime_formatted'] - df['DateTime']
df['Time to Resolution'] = df['Time to Resolution'].apply(format_timedelta)

# Write result dataframe to local directory
df.to_csv('Salem_Center_Trade_Times_'+snapshotdate+'.csv', index=False)