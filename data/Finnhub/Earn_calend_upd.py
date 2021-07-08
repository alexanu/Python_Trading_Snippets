
import pandas as pd
import finnhub
import datetime as dt

def main():
    token='br4gvpvrh5r8ufeot14g'
    hub = finnhub.Client(api_key=token)
    today = dt.datetime.now().strftime("%Y-%m-%d")
    n_days_ago = (dt.datetime.now() - dt.timedelta(days=20)).strftime("%Y-%m-%d")
    Earn_new = pd.json_normalize(hub.earnings_calendar(international=True)['earningsCalendar'])
    Earn_new.to_csv("Earnings_calendar_check.csv")
    Earn_new = Earn_new[Earn_new.date>n_days_ago]
    # Earn_new=Earn_new.drop(Earn_new.columns[[0]],axis=1)
    Earn_new=Earn_new.drop_duplicates()
    Earn_new = Earn_new[(Earn_new == 0).sum(1) < 3] # remove rows with no data
    Earn_new.sort_values(by=['date']).to_csv("Earnings_calendar.csv",mode='a',header=False)

if __name__== "__main__":
      main()
