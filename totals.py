import requests, json

BTC_TOKEN = 'BTC'
ETH_TOKEN = 'ETH'

# spreadsheets instead of API? -Access may be difficult to attain.
BASE_URL = "https://api.gemini.com/v1"

def network_info(token):
    # Retrieve Blockchain Network Info
    response = requests.get(BASE_URL + "/network/" + token)

    print("Response:")
    print(response.json())
    
    
network_info(ETH_TOKEN)