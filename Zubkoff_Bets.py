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
df = pd.merge(bets_df, contracts_df[['id', 'resolution', 'question']], left_on='contractId', right_on='id', how='left')

# Dropping the extra 'id' column from dataframe_B
df = df.drop('id_y', axis=1)

# Create new column if bet was correct
df['Accuracy'] = np.where(df['outcome'] == df['resolution'], 'RIGHT', 'WRONG')

# Filter down to unique bet id
#df = df.drop_duplicates(subset='id_x')


### Zubby ###

# Filter rows to Zubby bets
df = df[df['userId'] == '102hYaQkuxZ7wnPNGZ5JcDMR2aR2']


# Write Zubby dataframe to local directory
df.to_csv('Zubkoff_Bets_'+snapshotdate+'.csv', index=False)