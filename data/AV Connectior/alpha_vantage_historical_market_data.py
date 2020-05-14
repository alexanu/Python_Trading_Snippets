from alpha_vantage.timeseries import TimeSeries

from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.conf.region_settings import timezone_setting
from tradeasystems_connector.historical_market_data.historical_market_data import HistoricalMarketData
from tradeasystems_connector.model.bar import Bar
from tradeasystems_connector.model.period import Period


class AlphaVantageHistoricalMarketData(HistoricalMarketData):
    period_dict = \
        {  # '1min', '5min', '15min', '30min', '60min'
            Period.day: 'day',
            Period.minute: 'min',
            Period.hour: '60min'
        }

    columns_intra_historical_dict = \
        {
            Bar.close: '4. close',
            Bar.open: '1. open',
            Bar.high: '2. high',
            Bar.low: '3. low',
            Bar.time: 'index',
            Bar.volume: '5. volume'
        }
    columns_eod_historical_dict = \
        {
            Bar.close: '5. adjusted close',
            Bar.open: '1. open',
            Bar.high: '2. high',
            Bar.low: '3. low',
            Bar.time: 'index',
            Bar.volume: '6. volume'
        }
    timeseries = None

    def __init__(self, user_settings):
        HistoricalMarketData.__init__(self, user_settings)
        self.timeseries = TimeSeries(key=user_settings.alphavantage_token, retries=3)
        pass

    def formatTime(self, timeColumn):
        import pandas as pd
        # The price comes from the daily info -
        #  so it would be the price at the end of the day GMT based on the requested TS

        # http://pvlib-python.readthedocs.io/en/latest/timetimezones.html
        originTZ = 'Etc/GMT'

        datetimeSeries = pd.to_datetime(timeColumn)

        return pd.DatetimeIndex(pd.to_datetime(datetimeSeries, unit='ms')).tz_localize(originTZ).tz_convert(
            timezone_setting)

    def formatHistorical(self, input, period):
        import pandas as pd

        input_df = pd.DataFrame.from_dict(input).T

        output = pd.DataFrame(0, columns=Bar.columns, index=range(len(input_df)))
        input_df.reset_index(inplace=True)

        columnDict = self.columns_intra_historical_dict
        if period == Period.day:
            columnDict = self.columns_eod_historical_dict

        for column in output.columns:
            if column == Bar.time:
                timeProcessed = self.formatTime(input_df[columnDict[column]])
                output[column] = timeProcessed
            else:
                output[column] = input_df[columnDict[column]].astype(float)

        output.set_index(Bar.index, inplace=True)

        return output

    # day unlimited
    # minute limited to 7 days!
    def download(self, instrument, period, number_of_periods, fromDate, toDate=None):
        import datetime
        logger.debug("Downloading %s" % instrument.symbol)
        try:
            if period != Period.day:
                # '1min', '5min', '15min', '30min', '60min'
                if number_of_periods in [1, 5, 15, 30, 60]:
                    periodAlpha = '%i%s' % (number_of_periods, self.period_dict[period])
                    data_downloaded, meta_data = self.timeseries.get_intraday(instrument.symbol, interval=periodAlpha)
                else:
                    logger.error(
                        "AlphaVantage can only download intradaily! '1min', '5min', '15min', '30min', '60min'    not %s" % period)
                    return None
            else:
                # dateFormat = "%Y-%m-%d"
                if toDate is None:
                    toDate = datetime.datetime.today()
                # download dataframe
                try:
                    data_downloaded, meta_data = self.timeseries.get_daily_adjusted(instrument.symbol)
                    # data_downloaded = pdr.get_data_yahoo(instrument.symbol, start=fromDate.strftime(dateFormat), end=toDate.strftime(dateFormat))
                except Exception as e:
                    logger.error("Cand download from alphavantage %s => return None   %s" % (instrument.symbol, e))
                    return None
        except Exception as e:
            logger.error('Error downloading from alphavantage %s %s return None :%s' % (instrument.symbol, period, e))
            return None
        outputComplete = self.formatHistorical(data_downloaded, period=period)
        outputComplete = self.setTimeCorrect(outputComplete, period=period, instrument=instrument)

        return outputComplete
