"""Key Economic Event Notification Engine.
The engine will notify the impacful infomation to the required agent.
Example:
        $ python main.py
Todo:
    * Get level 3 impactful data (done)
    * Change timezone to locals (done)
    * Get list of time and change it into 3 minutes before timestamp (done)
    * Send twillo message to phone when current time is in the time list. (done)
    * Make one API call per day only
"""

# Schema is here:
# https://bubbl.us/NDc3NDc4NC8zODM0MjE0LzVkYzg2MzJlZGJjZTgxZGFkNzA4ZWEzNDY1Mjk0ZWI3@X?utm_source=shared-link&utm_medium=link&s=10033237


import requests as rq
import pandas as pd
import json
from datetime import datetime, timezone, timedelta
from twilio.rest import Client


class API(object):
    """docstring for API."""

    def __init__(self, url):
        super(API, self).__init__()
        self.url = url
        request = rq.get(self.url)
        data = json.loads(request.text)
        self.data_df = pd.DataFrame(data)
        self.data_df = self.data_df.dropna(how='any', axis=0)

    def utc_to_local(self, utc_dt):
        utc_dt = datetime.strptime(utc_dt, "%Y-%m-%d %H:%M:%S")
        local = utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)
        return local.strftime("%Y-%m-%d %H:%M:%S")

    def three_min_behind(self, timestring):
        time_dt = datetime.strptime(timestring, "%Y-%m-%d %H:%M:%S")
        time_dt_3_min = time_dt - timedelta(minutes=3)
        time_dt_3_min = time_dt_3_min.strftime("%s")
        return time_dt_3_min

    def change_24_to_12(self, timestring):
        time_dt = datetime.strptime(timestring, "%Y-%m-%d %H:%M:%S")
        time_dt = time_dt.strftime("%I:%M:%S %p")
        return time_dt

    def send_msg(self, msg):
        # Your Account SID from twilio.com/console
        account_sid = "AC0dadb0ce3f1db887ecf2cc5209932676"
        # Your Auth Token from twilio.com/console
        auth_token = "7eb9f55beeb3cfea278ccc183cf0f46a"

        client = Client(account_sid, auth_token)

        message = client.messages.create(
            to="+601128174379",
            from_="+17067395816",
            body=msg)
        return message.sid

    def processData(self, date, major):
        impactful_data = self.data_df.loc[self.data_df['impact'] == 3].copy()
        impactful_data = self.data_df
        impactful_data['timestamp_af'] = impactful_data['timestamp'].apply(lambda x: self.utc_to_local(x))
        impactful_data['date'] = impactful_data['timestamp_af'].apply(lambda x: x[:10])
        impactful_data = impactful_data.loc[impactful_data['date'] == date].copy()
        impactful_data['tmsp_3_min'] = impactful_data['timestamp_af'].apply(lambda x: self.three_min_behind(x))
        impactful_data['major'] = impactful_data['economy'].apply(lambda x: True if x in major else False)
        impactful_data = impactful_data.loc[impactful_data['major'] == True].copy()
        impactful_data['timestamp_af'] = impactful_data['timestamp_af'].apply(lambda x: self.change_24_to_12(x))
        self.final_data = impactful_data[['economy', 'name','impact','timestamp_af','tmsp_3_min']].copy()
        self.time_list = list(impactful_data['tmsp_3_min'])

    def create_msg(self, time_to_send):
        if time_to_send in self.time_list:
            data = self.final_data.loc[
                self.final_data['tmsp_3_min'] == time_to_send].copy()
            data = json.loads(data.to_json(orient='records'))
            main_body = str()
            for each in data:
                body = "Based: "+each['economy']+"\n"\
                    + "Info: "+each['name']+"\n"\
                    + "Impact: "+str(each['impact'])+"\n"\
                    + "Time: "+each['timestamp_af']
                main_body = main_body + "\n"+body
            code = self.send_msg(msg=body)
            print(code)


if __name__ == '__main__':
    major = ['EUR', 'USD', 'JPY', 'GBP', 'AUD', 'CHF', 'NZD', 'CAD']
    url = 'https://eco-event.000webhostapp.com/'
    date_time = datetime.now()
    date = date_time.strftime("%Y-%m-%d")
    print(date)
    date_time = date_time - timedelta(minutes=3)
    date_time = date_time.strftime("%s")
    sample_call = API(url)
    sample_call.processData(date, major)
    sample_call.create_msg(date_time)
