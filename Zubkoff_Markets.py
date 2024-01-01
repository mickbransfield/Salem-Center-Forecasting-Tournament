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
df = df.drop_duplicates(subset='id_x')


### Zubby ###

# Filter rows to Zubby bets
df = df[df['userId'] == '102hYaQkuxZ7wnPNGZ5JcDMR2aR2']


### ACCURACY ###

# Filter rows where Accuracy is 'Right'
correct_predictions = df[df['Accuracy'] == 'RIGHT']

# Group by question, then sum the shares for correct predictions
profit_from_shares = correct_predictions.groupby('question')['shares'].sum()

# Group by question, then sum all amounts regardless of prediction accuracy
total_amounts = df.groupby('question')['amount'].sum()

# Calculate profit: shares - amounts
profit = profit_from_shares.subtract(total_amounts, fill_value=0)

# Count unique trades (id_x) for each question
unique_trades = df.groupby('question')['id_x'].nunique()

# Combine profit and unique trades into a single DataFrame
result = pd.concat([profit, unique_trades], axis=1)
result.columns = ['Total_Profit', 'Unique_Trades']  # Ensure this matches the number of columns

# Rename Profit column
result = result.reset_index().rename(columns={0: 'question'})

# Sort the DataFrame by 'result' in descending order by profit and reset index
result = result.sort_values(by='Total_Profit', ascending=False).reset_index(drop=True)

# Display the result
print(result)

# Write result dataframe to local directory
result.to_csv('Zubkoff_Markets_'+snapshotdate+'.csv', index=False)