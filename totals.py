import requests, json
import base64, hmac, hashlib
import time
from datetime import datetime
import pandas as pd

pd.set_option('display.max_rows', 1000)

from coinbase_advanced_trader.enhanced_rest_client import EnhancedRESTClient

# CONSTANTS
BTC_ASOF = datetime.strptime("1/10/2023", '%m/%d/%Y')
ETH_ASOF = datetime.strptime("10/1/2022", '%m/%d/%Y')
# Get milliseconds since epoch
BTC_ASOF_MS = int(ETH_ASOF.timestamp() * 1000)
ETH_ASOF_MS = BTC_ASOF_MS

# Current time in seconds
PAYLOAD_NONCE = time.time()

# spreadsheets instead of API? -Access may be difficult to attain.
BASE_URL = "https://api.gemini.com"

GEMINI_API_KEY = ''
GEMINI_API_SECRET = ''
COINBASE_KEY = ''
COINBASE_SECRET = ''
with open('secrets.json', 'r') as file: 
    data = json.load(file)
    GEMINI_API_KEY = data['GEMINI']['API_KEY'].encode()
    GEMINI_API_SECRET = data['GEMINI']['API_SECRET'].encode()
    COINBASE_KEY = data['COINBASE']['API_KEY']
    COINBASE_SECRET = data['COINBASE']['API_SECRET']
COINBASE_CLIENT = EnhancedRESTClient(api_key=COINBASE_KEY, api_secret=COINBASE_SECRET)

def convert_ts_to_dt(records):
    for idx, record in enumerate(records):
        dt = datetime.fromtimestamp(record['timestamp'])
        records[idx]['timestamp'] = dt.strftime("%Y-%m-%d %H:%M:%S")
    return records

def make_post_request(endpoint, payload):
    encoded_payload = json.dumps(payload).encode()
    b64 = base64.b64encode(encoded_payload)
    signature = hmac.new(GEMINI_API_SECRET, b64, hashlib.sha384).hexdigest()

    request_headers = {
        'Content-Type': "text/plain",
        'Content-Length': "0",
        'X-GEMINI-APIKEY': GEMINI_API_KEY,
        'X-GEMINI-PAYLOAD': b64,
        'X-GEMINI-SIGNATURE': signature,
        'Cache-Control': "no-cache"
    }
    
    response = requests.post(
        url=BASE_URL+endpoint, 
        headers=request_headers,
        json=payload
    )

    #print("Response:")
    response_data = response.json()
    return convert_ts_to_dt(response_data)

def retrieve_trade_history(token, timestamp):
    endpoint = "/v1/mytrades"
    payload =  {
        "request": endpoint, 
        "nonce": PAYLOAD_NONCE,
        "symbol": token+'USD',
        "timestamp": timestamp
    }
    
    return make_post_request(endpoint, payload)

def retrieve_stake_history(token, timestamp):
    endpoint = "/v1/staking/history"
    payload =  {
        "request": endpoint, 
        "nonce": PAYLOAD_NONCE#,
        #"currency": token#,
        #"since": timestamp
    }
    
    return make_post_request(endpoint, payload)

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

def aggregate_stake_details(transaction_data: pd, token='') -> None:
    # Set the display format to avoid scientific notation 
    pd.options.display.float_format = '{:.8f}'.format
    
    aggregate_column = 'Amount ' + token
    aggregate_col_data = transaction_data[[aggregate_column]]
    aggregate_col_data = aggregate_col_data[aggregate_column].str.replace(token, '').astype(float)
    #print(aggregate_col_data)
    print(str(aggregate_col_data.sum()) + ' ' + token + ' staked')
    
def aggregate_transaction_details(transaction_data: pd) -> None:
    # Display the filtered data
    selected_columns = ['Timestamp', 'Asset', 'Quantity Transacted', \
                        'Price at Transaction','Subtotal','Total (inclusive of fees and/or spread)',\
                            'Fees and/or Spread']
    
    print(transaction_data[selected_columns])
    print('')
    
    # Get the sum
    # TODO: This value is incorrect
    quantity = transaction_data['Quantity Transacted'].astype(float).sum() 
    print("Total crypto:", quantity)
    
    # remove '$' and convert str to float
    transaction_data['Fees and/or Spread'] = transaction_data['Fees and/or Spread'].str.replace('$', '').astype(float)
    total_fees = transaction_data['Fees and/or Spread'].sum() 
    print("Total Fees:", total_fees)
    
    transaction_data['Subtotal'] = transaction_data['Subtotal'].str.replace('$', '').astype(float)
    subtotal = transaction_data['Subtotal'].sum() 
    print("Subtotal:", subtotal)
    
    transaction_data['Total (inclusive of fees and/or spread)'] =\
        transaction_data['Total (inclusive of fees and/or spread)'].str.replace('$', '').astype(float)
    total_with_fees = transaction_data['Total (inclusive of fees and/or spread)'].sum()
    print("Total w/t fees:", total_with_fees)
    
    # Will need to filter this data before calculating
    # total_wo_transfer = total_with_fees-1374.50
    # print("total_w/o_transfer (Grand Total Coinbase):", total_wo_transfer)
    
    # quantity_wo_transfer = quantity - 0.524035
    # print("quantity_wo_transfer:", quantity_wo_transfer)
    

    # Not super useful    
    # transaction_data['Price at Transaction'] = transaction_data['Price at Transaction'].str.replace('$', '').astype(float)
    # spot_price = transaction_data['Price at Transaction'].mean() # get average/'spot' price
    # print('spot price:', spot_price)
    
    #
    spot_price = subtotal / quantity
    print("spot_price:", spot_price)
    
    
    print()
    
if __name__ == '__main__':
    # Gemini API doesn't provide all necessary data - use csv instead
    #gem_btc_trades = retrieve_trade_history('BTC', BTC_ASOF_MS)
    #gem_eth_trades = retrieve_trade_history('ETH', ETH_ASOF_MS)
    
    # TODO: Retrieve and Aggregate Gemini Transaction History
    gem_btc_trans_data = retrieve_gemini_transaction_details(token='BTC')
    
    # Retrieve and Aggregate Gemini Stake History
    # gem_btc_stake_data = retrieve_gemini_stake_details(token='BTC')
    # aggregate_stake_details(transaction_data=gem_btc_stake_data, token='BTC')
    # gem_eth_stake_data = retrieve_gemini_stake_details(token='ETH')
    # aggregate_stake_details(transaction_data=gem_eth_stake_data, token='ETH')

    # cb_btc_data = retrieve_transactions_from_file(
    #     token='BTC', \
    #     token_column_name='Asset', \
    #     filename='coinbase_transactions.csv' \
    # )
    #cb_eth_data = retrieve_transactions_from_file(
        # token='ETH', \ 
        # token_column_name='Asset', \
        # filename='coinbase_transactions.csv' \
    # )
    # selected_columns = ['Timestamp', 'Asset', 'Quantity Transacted', \
    # 'Price at Transaction','Subtotal','Total (inclusive of fees and/or spread)',\
    # 'Fees and/or Spread']
    
    
    #aggregate_transaction_details(btc_data)
    
    #print(gem_btc_stake_data)
    print()