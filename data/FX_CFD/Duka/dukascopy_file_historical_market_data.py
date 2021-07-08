import glob
import os


class DukasCopyFileHistoricalMarketData(HistoricalMarketData):
    period_dict = \
        {  # '1min', '5min', '15min', '30min', '60min'
            Period.day: 'D',
            Period.minute: 'm',
            Period.hour: 'h',
            Period.second: 's',
            Period.tick: 'Ticks'
        }

    columns_historical_dict = \
        {
            Bar.close: 'Close',
            Bar.open: 'Open',
            Bar.high: 'High',
            Bar.low: 'Low',
            Bar.time: 'Gmt time',  # 01.06.2018 00:00:00.000
            Bar.volume: 'Volume'
        }
    columns_ticks_dict = \
        {
            Tick.ask: 'Ask',
            Tick.bid: 'Bid',
            Tick.ask_volume: 'AskVolume',
            Tick.bid_volume: 'BidVolume',
            Tick.time: 'Gmt time',
        }
    timeseries = None
    inputPath = None
    dateFormat = '%d.%m.%Y %H:%M:%S'  # 25.06.2018 00:00:00.000   02.07.2018
    filenameDateFormat = '%d.%m.%Y'

    tick_filename_format = '%s%s_Ticks_%s-%s.csv'  # symbol currency fromDate toDate
    bar_filename_format = '%s%s_Candlestick_%i_%s_%s_%s-%s.csv'  # symbol currency numberperiod period ASK/BID fromDate toDate
    processedFilesPattern = '.processed'

    filesInDirectory = None

    def __init__(self, user_settings):

        HistoricalMarketData.__init__(self, user_settings)

        self.inputPath = getDukascopyInputPath(user_settings)
        self.filesInDirectory = glob.glob(self.inputPath + os.sep + "*.csv")
        logger.info("dukascopy detected %i files can be processed" % (len(self.filesInDirectory)))

        pass

    def getToDateFromFile(self, toDate, period):
        return toDate

    def getDataFile_instrument_from_to_period_numberPeriods(self, file):
        import datetime
        filename = file.split(os.sep)[-1][:-4]
        fileSplitted = filename.split('_')
        symbol = fileSplitted[0][:3]
        currency = fileSplitted[0][3:]
        periodDukas = fileSplitted[1]
        if periodDukas == self.period_dict[Period.tick]:
            # tick
            number_periods = int(1)
            period = Period.tick
            dates = fileSplitted[2].split('-')
            fromDate = datetime.datetime.strptime(dates[0], self.filenameDateFormat)
            toDate = datetime.datetime.strptime(dates[1], self.filenameDateFormat)
        else:
            # bars
            number_periods = int(fileSplitted[2])
            period = {key for key, value in self.period_dict.items() if
                      value == fileSplitted[3]}.pop()  # self.period_dict[fileSplitted[3]]
            dates = fileSplitted[5].split('-')
            fromDate = datetime.datetime.strptime(dates[0], self.filenameDateFormat)
            toDate = datetime.datetime.strptime(dates[1], self.filenameDateFormat)

        toDate = self.getToDateFromFile(toDate, period)
        instrument = Instrument(symbol=symbol, currency=currency, asset_type=AssetType.forex)
        return instrument, fromDate, toDate, period, number_periods

    # def downloadFromFile(self,file):
    #     instrument, fromDate, toDate, period, number_periods = self.getDataFile_instrument_from_to_period_numberPeriods(file)
    #     dataframe = self.download( instrument, period, number_periods, fromDate, toDate)
    #     return dataframe

    def formatTime(self, timeColumn):
        import pandas as pd
        # The price comes from the daily info -
        #  so it would be the price at the end of the day GMT based on the requested TS

        # http://pvlib-python.readthedocs.io/en/latest/timetimezones.html
        originTZ = 'Etc/GMT'

        datetimeSeries = pd.to_datetime(timeColumn,
                                        dayfirst=True)  # date_parser=lambda x: datetime.strptime(x, '%d.%m.%Y %H:%M:%S.%f')
        return pd.DatetimeIndex(pd.to_datetime(datetimeSeries, unit='ms')).tz_localize(originTZ).tz_convert(
            timezone_setting)

    def formatHistorical(self, input_df, period):
        import pandas as pd

        output = pd.DataFrame(0, columns=Bar.columns, index=range(len(input_df)))
        input_df.reset_index(inplace=True)
        columnDict = self.columns_historical_dict
        columnTime = Bar.time
        columnIndex = Bar.index
        if period == Period.tick:
            columnDict = self.columns_ticks_dict
            output = pd.DataFrame(0, columns=Tick.columns, index=range(len(input_df)))
            columnTime = Tick.time
            columnIndex = Tick.index
        for column in output.columns:
            if column == columnTime:
                timeProcessed = self.formatTime(input_df[columnDict[column]])
                output[column] = timeProcessed
            else:
                if column in columnDict:
                    output[column] = input_df[columnDict[column]]

        output.set_index(columnIndex, inplace=True)

        return output

    def markFileAsProcessed(self, file):
        logger.debug('Marking %s as processed with file type %s' % (file, self.processedFilesPattern))
        os.rename(file, file + self.processedFilesPattern)  # mark processed

    # day unlimited
    # minute limited to 7 days!
    def download(self, instrument, period, number_of_periods, fromDate, toDate=None):
        import pandas as pd
        self.filesInDirectory = glob.glob(self.inputPath + os.sep + "*.csv")
        logger.debug("Downloading from dukascopy file %s_%s %i_%s from %s to %s" % (
            instrument.symbol, instrument.currency, number_of_periods, period, fromDate, toDate))
        toDateFile = self.getToDateFromFile(toDate, period)
        if period == Period.tick:
            # '%s%s_Ticks_%s-%s.csv'
            file = self.tick_filename_format % (
                instrument.symbol, instrument.currency, fromDate.strftime(self.filenameDateFormat),
                toDateFile.strftime(self.filenameDateFormat))  # '%s%s_Candlestick_%i_%s_%s-%s
        else:
            # '%s%s_Candlestick_%i_%s_%a_%s-%s.csv'
            file = self.bar_filename_format % (
                instrument.symbol, instrument.currency, number_of_periods, self.period_dict[period], 'ASK',
                fromDate.strftime(self.filenameDateFormat),
                toDateFile.strftime(self.filenameDateFormat))  # '%s%s_Candlestick_%i_%s_%s-%s

        for fileIterate in self.filesInDirectory:
            fileIterateName = fileIterate.split(os.sep)[-1]
            if fileIterateName == file:
                logger.debug("File %s found , processing" % file)
                try:
                    # data_downloaded = pd.DataFrame.from_csv(fileIterate, dayfirst=True)  # date_format=self.dateFormat)  date_parser=lambda x: datetime.strptime(x, '%d.%m.%Y %H:%M:%S.%f'))
                    data_downloaded = pd.read_csv(fileIterate)
                except Exception as e:
                    logger.error('Cant read file %s some errors there' % file)
                    return None
                outputComplete = self.formatHistorical(data_downloaded, period=period)

                # if fromDate is not None and toDate is not None:
                #     outputComplete = outputComplete[fromDate: toDate]
                # elif fromDate is not None:
                #     outputComplete = outputComplete[fromDate:]
                # elif toDate is not None:
                #     outputComplete = outputComplete[:toDate]

                # self.markFileAsProcessed(file)
                return outputComplete
