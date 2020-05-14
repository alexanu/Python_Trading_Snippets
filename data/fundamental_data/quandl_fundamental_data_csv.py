import joblib

from tradeasystems_connector.fundamental_data.fundamental_data import FundamentalData
from tradeasystems_connector.fundamental_data.quandl_fundamental_calculator import *
from tradeasystems_connector.fundamental_data.quandl_fundamental_data import QuandlFundamentalData, calculateRatio
from tradeasystems_connector.model.ratio import Ratio
from tradeasystems_connector.util.configuration_keys_util import getDatabasePath, getTempPath


class QuandlFundamentalDataCSV(FundamentalData):
    instance = None

    def __init__(self, user_settings):
        if self.instance is None:
            self.instance = QuandlFundamentalDataCSV.__QuandlFundamentalDataCSV(user_settings)
        # else:
        #     self.instance.user_settings = user_settings

    def download(self, instrument, fundamental_ratio, fromDate, toDate=None):
        return self.instance.download(instrument, fundamental_ratio, fromDate, toDate)

    def __getattr__(self, name):
        return getattr(self.instance, name)

    class __QuandlFundamentalDataCSV(FundamentalData):
        columns_ratio_dict = \
            {
                Ratio.ratio: 'Value',
                Ratio.time: 'Date',
            }

        class QuandlFrequency:
            quaterly = ['SF1', 'MRQ', 60]
            annual = ['SF0', 'MRY', 250]
            annual_prem = ['SF1', 'MRY', 250]

        # quandlDatabase = 'SHARADAR'
        frequencyChosen = QuandlFrequency.annual
        dateFormat = "%Y-%m-%d"
        urlPrefixDownload = 'https://www.quandl.com/api/v3/databases/SF0/data?api_key=%s'
        tokenKey = None
        user_settings = None

        url = None
        pandasData = None
        quandl_fundamental_data = None
        ratio_dict = None
        cacher = None
        temp_dir = None

        # SF0 / AAPL_DEBTUSD_MRY
        def __init__(self, user_settings):
            import os
            self.tokenKey = user_settings.quandl_token
            self.user_settings = user_settings
            self.url = self.urlPrefixDownload % self.tokenKey
            self.quandl_fundamental_data = QuandlFundamentalData(self.user_settings)

            weekString = datetime.datetime.today().strftime("%W_%Y")
            pathFile = getTempPath(self.user_settings) + os.sep + 'SF0_%s.csv' % weekString

            isDownloaded = self.downloadUnzip(pathFile)
            if not isDownloaded:
                # logger.debug('Getting manually default file')
                pathFile = getDatabasePath(self.user_settings) + os.sep + 'SF0.csv'
            self.ratio_dict = self.quandl_fundamental_data.ratio_dict

            self.temp_dir = getTempPath(self.user_settings)
            self.cacher = joblib.Memory(self.temp_dir)
            self.pandasData = self.getCSVData(pathFile)
            pass

        def downloadUnzip(self, pathFile):

            import os
            output = False
            if os.path.isfile(pathFile):
                return
            # try:
            #     #TODO not connecting
            #     logger.debug('Downloading Quandl Data CSV: %s'%self.url)
            #     url = urlopen(self.url)
            #
            #     zipfile = ZipFile(io.BytesIO(url.content))
            #     zipfile.extractall(path =pathFile )
            #     output = True
            # except Exception as e:
            #     logger.error('Error downloading quandl csv from %s : %s'%(self.url,str(e)))
            return output

        def getCSVData(self, pathFile):
            import pandas as pd
            logger.debug('Reading  Quandl Data CSV: %s' % pathFile)
            pandasData = pd.read_csv(pathFile, names=["name", "date", "value"])
            pandasData['date'] = pd.to_datetime(pandasData['date'])
            return pandasData

        def makeQuandlName(self, instrument, fundamental_ratio):
            fundamental = None
            if fundamental_ratio in self.ratio_dict.keys():
                fundamental = self.ratio_dict[fundamental_ratio]
            if fundamental is None:
                # calculate it
                logger.debug('%s not found in quandl=> calculating ' % fundamental_ratio)
                return None
            else:
                # PVH_TBVPS_MRY
                return '%s_%s_%s' % (instrument.symbol, fundamental, self.frequencyChosen[1])

        def getFrequencyFundamentalRatio(self, fundamental_ratio):
            return self.quandl_fundamental_data.getFrequencyFundamentalRatio(fundamental_ratio)

        def setFrequencyChosen(self, fundamental_ratio):
            return self.quandl_fundamental_data.setFrequencyChosen(fundamental_ratio)

        def calculateRatio(self, instrument, fundamental_ratio, fromDate, toDate):
            return calculateRatio(self, instrument, fundamental_ratio, fromDate, toDate)

        def getData(self, tableName, start_date=None, end_date=datetime.datetime.today()):
            # AABA_NETINCCMNUSD_MRY,2010-12-31,1231663000.0
            output_1 = self.pandasData[self.pandasData['name'] == tableName]
            output_1 = output_1.set_index('date')
            if start_date is None:
                start_date = output_1.index[0]
            if end_date is None:
                end_date = output_1.index[-1]

            return output_1[start_date:end_date]

        def download(self, instrument, fundamental_ratio, fromDate=None, toDate=None):

            self.setFrequencyChosen(fundamental_ratio)

            tableName = self.makeQuandlName(instrument, fundamental_ratio=fundamental_ratio)
            if tableName is None:
                logger.debug('calculating  quandl csv  %s ' % fundamental_ratio)
                output = self.calculateRatio(instrument, fundamental_ratio, fromDate, toDate)
            else:
                logger.debug('getting quandl csv %s ' % tableName)
                try:
                    if fromDate is not None and toDate is not None:
                        dataDownloaded = self.getData(tableName, start_date=fromDate, end_date=toDate)
                    elif fromDate is not None and toDate is None:
                        dataDownloaded = self.getData(tableName, start_date=fromDate)
                    else:
                        dataDownloaded = self.getData(tableName)
                except Exception as e:
                    logger.error(
                        'error not found in quandl csv %s for %s  => return none %s' % (
                            tableName, instrument.symbol, e))
                    return None
                # dataDownloaded = quandl.get_table(tableName,authtoken=self.tokenKey)
                if len(dataDownloaded) == 0:
                    logger.error('csv quandl empty  %s for %s  => return none ' % (tableName, instrument.symbol))
                    return None
                output = self.formatFundamental(dataDownloaded)
                output = self.setTimeCorrect(output, instrument)
            return output

        def formatTime(self, timeColumn):
            return self.quandl_fundamental_data.formatTime(timeColumn)

        def formatFundamental(self, input):
            import pandas as pd
            input_df = input.copy().reset_index()

            output = pd.DataFrame(0, columns=Ratio.columns, index=input_df.index)
            for column in output.columns:
                if column == Ratio.time:
                    timeProcessed = input_df['date']
                    output[column] = timeProcessed
                else:
                    output[column] = input_df['value']

            output.set_index(Ratio.index, inplace=True)

            return output
