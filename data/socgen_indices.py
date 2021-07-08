import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
plt.switch_backend('agg')
from mail import Email
import quandl
token='QWe8iSbyAFzRuod2aroM'
sns.set_context("poster")
sns.set(font_scale=1)

web_root='https://wholesale.banking.societegenerale.com/fileadmin/indices_feeds/'
indices={'CTA':'CTA_Historical.xls',
         'CTA Mutual Funds':'CTAM_Historical.xls',
         'Trend Index':'Trend_Index_Historical.xls',
         'Short Term Traders Index':'STTI_Historical.xls',
         'Multi Alternative Risk Premia':'MARP_Historical.xls'
        }
data_index=pd.DataFrame()
for i in indices.keys():
    file='https://wholesale.banking.societegenerale.com/fileadmin/indices_feeds/'+indices[i]
    data_index[i]=pd.read_csv(file,sep='\t',index_col=0,parse_dates=[0],usecols=[0,1]).ix[:,0]

data_pct=data_index.pct_change()

ax1=data_pct['2019':].cumsum().ffill().plot(colormap='jet')
ax1.set_xlabel("")
ax1.get_figure().savefig('socgen.png')
plt.show()
plt.gcf().clear()

df = pd.DataFrame()
df['CTA']=data_index.CTA
df['SP500']=quandl.get('CHRIS/CME_SP1',authtoken=token).Last
df=df.dropna().pct_change()
ax2=pd.ewmcorr(df.CTA,df['SP500'],20)['2019':].plot(colormap='jet',title='20 Day Rolling Correlation: CTA index to S&P 500')
ax2.set_xlabel("")
ax2.get_figure().savefig('socgen_corr.png')

e=Email(subject='Morning Update: Soc Gen Indices')
e.add_attachments(['socgen.png','socgen_corr.png'])
e.send()