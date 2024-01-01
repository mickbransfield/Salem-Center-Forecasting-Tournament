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
df = pd.merge(bets_df, contracts_df[['id', 'resolution']], left_on='contractId', right_on='id', how='left')

# Dropping the extra 'id' column from dataframe_B
df = df.drop('id_y', axis=1)

# Create new column if bet was correct
df['Accuracy'] = np.where(df['outcome'] == df['resolution'], 'RIGHT', 'WRONG')

# Filter down to unique bet id
df = df.drop_duplicates(subset='id_x')


### ACCURACY ###

# Filter rows where Accuracy is 'Right'
correct_predictions = df[df['Accuracy'] == 'RIGHT']

# Group by userId and sum the shares for correct predictions
profit_from_shares = correct_predictions.groupby('userId')['shares'].sum()


### PROFIT & BET COUNTS ###

# Group by userId and sum all amounts regardless of prediction accuracy
total_amounts = df.groupby('userId')['amount'].sum()
bet_counts = df.groupby('userId')['id_x'].count()

# Ensure both series have the same index
all_user_ids = df['userId'].unique()
profit_from_shares = profit_from_shares.reindex(all_user_ids).fillna(0)
total_amounts = total_amounts.reindex(all_user_ids).fillna(0)
bet_counts = bet_counts.reindex(all_user_ids).fillna(0)

# Calculate profit: shares - amounts
profit = profit_from_shares - total_amounts

# Rename Profit & Bet Count column
profit = profit.reset_index().rename(columns={0: 'Profit'})
bet_counts = bet_counts.reset_index().rename(columns={'id_x': 'Bet Count'})

# Merge profit & bet_counts
profit = pd.merge(profit, bet_counts, left_on='userId', right_on='userId', how='left')

# Sort the DataFrame by 'Profit' in descending order and reset index
profit = profit.sort_values(by='Profit', ascending=False).reset_index(drop=True)

# Display the result
print(profit)

# Write Proift by userId to local directory
profit.to_csv('Salem_Center_Profit_'+snapshotdate+'.csv', index=False)
