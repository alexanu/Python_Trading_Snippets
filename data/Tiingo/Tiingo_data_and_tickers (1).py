import threading
from tiingo import TiingoClient
import csv


config = {}
config['session'] = True
config['api_key'] = "69954dec5d2661170ef8550cb28571cfa504c100"
client = TiingoClient(config)

# создаём список для приёма котировок
s=[]

# скачивание котировок через TiingoClient и запись в список s

def potock (thr):
    global s
    tickers = client.list_stock_tickers()  # получение списка тикеров (акций)
    for t in range(thr,len(tickers),10):
        a = tickers[t]['ticker']  # название акции
        quotes = client.get_ticker_price(a, fmt='json', startDate=str(tickers[t]['endDate']), frequency='daily')
        if quotes != []:  # отсекает встречающийся пустой список
            print([a, quotes[0]['high']])  # выводит название акции и котировку по заданному значению
            s.append([a, quotes[0]['high']])


for i in range(0,10):
    my_thread = threading.Thread(target=potock, args=(i,))
    my_thread.start()


from tiingo import TiingoClient
from threading import Thread   # библиотека для создания потоков


# скачивание котировок через TiingoClient и запись в файл CSV
with open("E:\\code\\tiingoCSV\\rec.csv", 'w', newline='') as f:
    tickers = client.list_stock_tickers()    # получение списка тикеров (акций)
    for t in range(len(tickers)):
        a = tickers[t]['ticker']  # название акции
        quotes = client.get_ticker_price(a, fmt='json', startDate=str(tickers[t]['endDate']), frequency='daily')
        if quotes != []:  # отсекает встречающийся пустой список
            print([a, quotes[0]['high']])  # выводит название акции и котировку по заданному значению
            writer = csv.writer(f, delimiter=';')
            writer.writerow([a, quotes[0]['high']])

quotes = client.get_ticker_price('000001', fmt='json', startDate='1980-01-01', frequency='daily')
print (quotes)

with open("E:\code\ploe.txt","w") as out:
   for i in quotes:
      print(i,file=out)

def json_to_csv(quotes):
   with open("E:\code\pp.csv", 'w', newline='') as f:
      writer = csv.writer(f, delimiter=';')
      writer.writerow(
         ['date', 'o', 'h', 'l', 'c', 'v', 'a_o', 'a_h', 'a_l',
          'a_c', 'a_v', 'div', 'split'])
      for d in quotes:
         o = d['open']
         c = d['close']
         h = d['high']
         l = d['low']
         v = d['volume']
         adj_o = d['adjOpen']
         adj_c = d['adjClose']
         adj_h = d['adjHigh']
         adj_l = d['adjLow']
         adj_v = d['adjVolume']
         div_cash = d['divCash']
         split_factor = d['splitFactor']
         date = d['date'].split("T")[0].replace("-", "")
         writer.writerow(
            [date, o, h, l, c, v, adj_o, adj_h, adj_l, adj_c, adj_v, div_cash, split_factor])
json_to_csv(quotes)
print ('ok')


import requests
import zipfile

f = open(r'E:\code\tiingoCSV\tiingoDATA.zip', "wb")  # открываем файл для записи, в режиме wb
ufr = requests.get("https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip")  # делаем запрос
f.write(ufr.content)  # записываем содержимое в файл
f.close()

z = zipfile.ZipFile('E:\\code\\tiingoCSV\\tiingoDATA.zip', 'r')
z.printdir()