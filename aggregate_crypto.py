from datetime import datetime
import pandas as pd

# Set the display format to avoid scientific notation 
pd.options.display.float_format = '{:.8f}'.format
    
pd.set_option('display.max_rows', 1000)

TOKENS_TO_AGGREGATE=[
    'BTC',
    'ETH'
]

def convert_ts_to_dt(records):
    for idx, record in enumerate(records):
        dt = datetime.fromtimestamp(record['timestamp'])
        records[idx]['timestamp'] = dt.strftime("%Y-%m-%d %H:%M:%S")
    return records

def retrieve_gemini_transaction_details(token=''):
    gem_trans_data = retrieve_transactions_from_file(
        token=token+'USD', \
        token_column_name='Symbol', \
        filename='gemini_transaction_history.csv', \
        addit_filters={ 'Type': 'Buy' }    
    )
    selected_columns = [
        'Date', 'Type', 'Symbol', 'USD Amount USD', 'Fee (USD) USD',\
            token+' Amount '+token
            #, token+' Balance '+token
    ]
    return filter_data_by_selected_columns(gem_trans_data, selected_columns)

def retrieve_gemini_stake_details(token=''):
    gem_stake_data = retrieve_transactions_from_file(
        token=token, \
        token_column_name='Symbol', \
        filename='gemini_staking_transaction_history.csv', \
        addit_filters={ 'Type': 'Interest Credit' }    
    )
    selected_columns = [
        'Date', 'Type', 'Symbol', 'Amount '+token,'Price USD', \
            'Amount USD', 'Balance '+token
    ]
    return filter_data_by_selected_columns(gem_stake_data, selected_columns)
    
def retrieve_coinbase_transaction_details(token=''):
    cb_transaction_data = retrieve_transactions_from_file(
        token=token, \
        token_column_name='Asset', \
        filename='coinbase_transactions.csv', \
        addit_filters={ 'Transaction Type': 'Buy' }  
    )
    selected_columns = [
        'Timestamp', 'Asset', 'Quantity Transacted', \
        'Price at Transaction','Subtotal','Total (inclusive of fees and/or spread)',\
        'Fees and/or Spread'
    ]
    return filter_data_by_selected_columns(cb_transaction_data, selected_columns)


def retrieve_transactions_from_file(token='', token_column_name='', filename='', addit_filters={}) -> pd:
    # Load the CSV file 
    df = pd.read_csv(filename) 

    #Filter the data 
    transaction_data = df[df[token_column_name] == token] 
    
    for col in addit_filters.keys():
        #print('key:value : ', col+':'+addit_filters[col])
        transaction_data = transaction_data[transaction_data[col] == addit_filters[col]]
    
    # Apply custom row index
    transaction_data = transaction_data.reset_index(drop=True) 
    
    return transaction_data

def filter_data_by_selected_columns(transaction_data: pd, selected_columns: list[str] = []) -> pd:
    return transaction_data[selected_columns]

def format_gem_transaction_details(transaction_data: pd, token = '') -> pd:
    # To be used later
    transaction_data['First Date'] =\
        transaction_data['Date']
    transaction_data['Latest Date'] =\
        transaction_data['Date']
        
    transaction_data['USD Amount USD'] = \
        transaction_data['USD Amount USD'].str.slice(1, -1)
    transaction_data['USD Amount USD'] = \
        transaction_data['USD Amount USD'].str.replace('$', '').astype(float)
        
    transaction_data['Fee (USD) USD'] = \
        transaction_data['Fee (USD) USD'].str.slice(1, -1)
    transaction_data['Fee (USD) USD'] = \
        transaction_data['Fee (USD) USD'].str.replace('$', '').astype(float)
        
    transaction_data[token+' Amount '+token] = \
        transaction_data[token+' Amount '+token].str.replace(' '+token, '').astype(float)
    
    return transaction_data

def format_gem_stake_details(transaction_data: pd, token='') -> pd:
    # To be used later
    transaction_data['First Date'] =\
        transaction_data['Date']
    transaction_data['Latest Date'] =\
        transaction_data['Date']
    
    aggregate_column = 'Amount ' + token
    aggregate_col_data = transaction_data[[aggregate_column]]
    transaction_data[aggregate_column] = \
         aggregate_col_data[aggregate_column].str.replace(token, '').astype(float)
    return transaction_data

def format_cb_trans_details(transaction_data: pd) -> pd:
    # Convert column to Datetime first
    transaction_data['Timestamp'] =\
        pd.to_datetime(transaction_data['Timestamp'], errors='coerce')
    # Convert Datetime to Date - to be used later
    transaction_data['First Date'] =\
        transaction_data['Timestamp'].dt.date
    transaction_data['Latest Date'] =\
        transaction_data['Timestamp'].dt.date
    
    # TODO: This value is incorrect
    transaction_data['Quantity Transacted'] =\
        transaction_data['Quantity Transacted'].astype(float)
    
    transaction_data['Fees and/or Spread'] =\
        transaction_data['Fees and/or Spread'].str.replace('$', '').astype(float)
    
    transaction_data['Subtotal'] =\
        transaction_data['Subtotal'].str.replace('$', '').astype(float)
    
    return transaction_data

def aggregate_transaction_details(transaction_data: pd, col_names={}) -> pd:
    aggregate_details = pd.DataFrame(
        columns=[
            'First Date',
            'Latest Date',
            'Quantity',
            'Subtotal',
            'Fees',
            'Total',
            'Spot Price'
        ]
    )
       
    transaction_data['First Date'] = \
        pd.to_datetime(transaction_data['First Date'], errors='coerce')
    first_date = transaction_data['First Date'].min()
    
    transaction_data['Latest Date'] = \
        pd.to_datetime(transaction_data['Latest Date'], errors='coerce')
    latest_date = transaction_data['Latest Date'].max()
    
    # Aggregates
    quantity_col = col_names.get('quantity', False)
    quantity = 0.0    
    if quantity_col:
        quantity = transaction_data[quantity_col].sum()       

    subtotal_col = col_names.get('subtotal', False)
    subtotal = 0.0    
    if subtotal_col:
        subtotal = transaction_data[subtotal_col].sum()     
    
    fees_col = col_names.get('fees', False)
    fees = 0.0    
    if fees_col:
        fees = transaction_data[fees_col].sum()     
        
    total = subtotal + fees
    
    # Aggregate Spot price (Estimate, not actually used in calc)
    # aggregate subtotal / aggregate quantity = Est. mean spot price
    est_spot_price = subtotal / quantity
    
    aggregate_details.loc[0] = [
        first_date,
        latest_date,
        quantity, 
        subtotal, 
        fees, 
        total, 
        est_spot_price 
    ]
    
    return aggregate_details

def aggregate_gemini_transactions(token='') -> pd:
    gem_trans_data = retrieve_gemini_transaction_details(
        token=token
    )
    gem_trans_data = format_gem_transaction_details(
        transaction_data=gem_trans_data, 
        token=token
    )
    gem_aggregate_trans_data = aggregate_transaction_details(
        transaction_data=gem_trans_data, 
        col_names={
            'quantity': token+' Amount '+token,
            'subtotal': 'USD Amount USD',
            'fees': 'Fee (USD) USD'
        }
    )
    return gem_aggregate_trans_data

def aggregate_gemini_staking(token='') -> pd:
    gem_stake_data = retrieve_gemini_stake_details(token=token)
    gem_stake_data = format_gem_stake_details(
        transaction_data=gem_stake_data, 
        token=token
    )
    gem_aggregate_stake_data = aggregate_transaction_details(
        transaction_data=gem_stake_data, 
        col_names={
            'quantity': 'Amount '+token,
        }
    )
    return gem_aggregate_stake_data

def aggregate_coinbase_transactions(token='') -> pd:
    cb_data = retrieve_coinbase_transaction_details(token)
    cb_data = format_cb_trans_details(cb_data)
    cb_aggregate_trans_data = aggregate_transaction_details(
       transaction_data=cb_data, 
       col_names={
           'quantity': 'Quantity Transacted',
           'subtotal': 'Subtotal',
           'fees': 'Fees and/or Spread'
       }
    )
    return cb_aggregate_trans_data
    
if __name__ == '__main__':
    print()
    print('-------------------- AGGREGATE TRANSACTION RECORDS --------------------')
    for token in TOKENS_TO_AGGREGATE:
        gemini_transaction_aggregate = aggregate_gemini_transactions(token)
        gemini_staking_aggregate = aggregate_gemini_staking(token)
        coinbase_transaction_aggregate = aggregate_coinbase_transactions(token=token)
        
        # Merge the DataFrames 
        merged_transaction_data = pd.concat(
            [
                gemini_transaction_aggregate, 
                gemini_staking_aggregate, 
                coinbase_transaction_aggregate
            ], 
            ignore_index=True
        )
        #print(merged_transaction_data)
        
        final_aggregate = aggregate_transaction_details(
            transaction_data=merged_transaction_data, 
            col_names={
                'quantity': 'Quantity',
                'subtotal': 'Subtotal',
                'fees': 'Fees'
            }
        )
        print(token+' AGGREGATE:')
        print(final_aggregate)
        print('----------------------------------------------------------------------')