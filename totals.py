import requests, json
import time
import base64
import hmac
import hashlib

# CONSTANTS
BTC_TOKEN = 'BTC'
ETH_TOKEN = 'ETH'

# spreadsheets instead of API? -Access may be difficult to attain.
BASE_URL = "https://api.gemini.com"

API_KEY = ''
API_SECRET = ''
with open('secrets.json', 'r') as file: 
    data = json.load(file)
    API_KEY = data['API_KEY'].encode()
    API_SECRET = data['API_SECRET'].encode()
    
# Current time in seconds
PAYLOAD_NONCE = time.time()

def trade_info(token):
    payload =  {
        "request": "/v1/mytrades", 
        "nonce": PAYLOAD_NONCE,
    }
    
    encoded_payload = json.dumps(payload).encode()
    b64 = base64.b64encode(encoded_payload)
    signature = hmac.new(API_SECRET, b64, hashlib.sha384).hexdigest()

    request_headers = {
        'Content-Type': "text/plain",
        'Content-Length': "0",
        'X-GEMINI-APIKEY': API_KEY,
        'X-GEMINI-PAYLOAD': b64,
        'X-GEMINI-SIGNATURE': signature,
        'Cache-Control': "no-cache"
    }
    
    response = requests.post(
        url=BASE_URL+"/v1/mytrades", 
        headers=request_headers,
        json=payload
    )

    print("Response:")
    print(response.json())
    
    
trade_info(ETH_TOKEN)