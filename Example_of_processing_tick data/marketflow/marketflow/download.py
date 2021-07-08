from boxsdk import Client, OAuth2
from boxsdk.network.default_network import DefaultNetwork
from pprint import pformat

import requests

print('Downloading public test data...')
PUBLIC_URL = 'https://berkeley.box.com/shared/static/c4tqpu0h601b2ypimpst3uutfc8e4s93.zip'

# Writes test_data_public.zip to test-data/
resp = requests.get(PUBLIC_URL)
with open('test-data/test_data_public.zip', 'wb') as test_data:
    test_data.write(resp.content)

CLIENT_ID = None
CLIENT_SECRET = None
DEVELOPER_TOKEN = None

with open('boxauth.cfg', 'r') as boxauth:
    CLIENT_ID = boxauth.readline()
    CLIENT_SECRET = boxauth.readline()
    DEVELOPER_TOKEN = boxauth.readline()

# # Forward user to Box's login page
# req = 'https://app.box.com/api/oauth2/authorize?response_type=code&client_id='+CLIENT_ID+'&state=security_token%3DKnhMJatFipTAnM0nHlZA'
# resp = requests.get(req)
# print(resp.content)


class LoggingNetwork(DefaultNetwork):
    def request(self, method, url, access_token, **kwargs):
        """ Base class override. Pretty-prints outgoing requests and incoming responses. """
        print ('\x1b[36m{} {} {}\x1b[0m'.format(method, url, pformat(kwargs)))
        response = super(LoggingNetwork, self).request(
            method, url, access_token, **kwargs
        )
        if response.ok:
            print ('\x1b[32m{}\x1b[0m'.format(response.content))
        else:
            print ('\x1b[31m{}\n{}\n{}\x1b[0m'.format(
                response.status_code,
                response.headers,
                pformat(response.content),
            ))
        return response

oauth = OAuth2(CLIENT_ID, CLIENT_SECRET, access_token=DEVELOPER_TOKEN)
client = Client(oauth, LoggingNetwork())

root_folder = client.folder(folder_id='0').get()
items = client.folder(folder_id='0').get_items(limit=100, offset=0)

shared_link = client.folder(folder_id='3800889110').get_shared_link()






