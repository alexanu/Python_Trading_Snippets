import requests

url = "https://yahoo-finance15.p.rapidapi.com/api/yahoo/et/topetfs"

querystring = {"start":"0"}

headers = {
    'x-rapidapi-key': "7a3621970amsh9275d1c7c9ed661p15c96ajsnbc45c95d106c",
    'x-rapidapi-host': "yahoo-finance15.p.rapidapi.com"
    }

response = requests.request("GET", url, headers=headers, params=querystring)

print(response.text)


import requests
import pandas as pd

url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v2/get-profile"

querystring = {"symbol":"AMRN","region":"US"}

headers = {
    'x-rapidapi-key': "7a3621970amsh9275d1c7c9ed661p15c96ajsnbc45c95d106c",
    'x-rapidapi-host': "apidojo-yahoo-finance-v1.p.rapidapi.com"
    }

response = requests.request("GET", url, headers=headers, params=querystring)
result=response.json()
df = pd.json_normalize(result)



result[1]
print(response.text)