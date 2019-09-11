import pandas
import urllib2
from emails import SUBSCRIBE_LIST, email_login, send_mail

def get_ETF_fr_YH():
    response = urllib2.urlopen('http://finance.yahoo.com/etf/lists/?mod_id=mediaquotesetf&tab=tab3&rcnt=50')
    the_page = response.read()    
    splits = the_page.split('<a href=\\"\/q?s=')
    etf_symbols = [split.split('\\')[0] for split in splits[1:]]
    return etf_symbols

def get_ETFSymbols(source):
    if source.lower() == 'yahoo':
        return get_ETF_fr_YH()
    elif source.lower() == 'nasdaq':
        return pandas.read_csv('http://www.nasdaq.com/investing/etfs/etf-finder-results.aspx?download=Yes')['Symbol'].values

def find_validETF(filename):
    ls_etf = []
    for etf in get_ETFSymbols('Nasdaq'):
        ls_etf.append(etf)

    df = pandas.DataFrame({'ETF': ls_etf})
    df.set_index('ETF').to_csv(filename)

def run(me, password):
    find_validETF('ETF.csv')
    send_mail(
        send_from = 'NoReply@gmail.com', 
        send_to = SUBSCRIBE_LIST, 
        subject = 'Suggested ETF', 
        text = """Hello,

Attachment includes the ETFs.

Sincerely,

""", 
        files = ['ETF.csv'], 
        server = 'smtp.gmail.com',
        username = me,
        password = password
    )

if __name__ == '__main__':
    me, password = email_login()
    run(me, password)
