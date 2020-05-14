import threading
from time import sleep

from ib.ext.Contract import Contract
from ib.opt import ibConnection



class InteractiveBrokersHistoricalMarketData(HistoricalMarketData):
    import datetime

    whatToShow = None
    ib_formatDate_return = 1  # "yyyyMMdd"
    ib_object = None
    period_dict = \
        {
            Period.second: 'sec',
            Period.day: 'day',
            Period.minute: 'min',
            Period.hour: 'hour'
        }

    columns_historical_dict = \
        {
            Bar.close: 'close',
            Bar.open: 'open',
            Bar.high: 'high',
            Bar.low: 'low',
            Bar.time: 'time',
            Bar.volume: 'volume'
        }
    sec_type_dict = \
        {
            AssetType.equity: 'STK',
            AssetType.forex: 'CASH',
            AssetType.cfd: 'CFD',
            AssetType.future: 'FUT',
            AssetType.bond: 'BOND',
            AssetType.index: 'IND',
            AssetType.commodity: 'CMDTY'

        }
    exchange_dict = \
        {
            AssetType.future: 'GLOBEX',
            AssetType.cfd: 'SMART',
            AssetType.equity: 'SMART',
            AssetType.etf: 'SMART',
            AssetType.forex: 'IDEALPRO',
            AssetType.bond: 'SMART',
            AssetType.commodity: 'SMART',
        }

    max_duration_dict = \
        {
            Period.second: datetime.timedelta(minutes=1) * 30,
            Period.minute: datetime.timedelta(days=1),
            Period.hour: datetime.timedelta(days=30),
            Period.day: datetime.timedelta(days=365)
        }

    max_durationStr_dict = \
        {
            Period.second: '1800 S',
            Period.minute: '1 D',
            Period.hour: '1 M',
            Period.day: '1 Y'
        }
    tickId = 0

    formatDate = '%Y%m%d %H:%M:%S'
    formatDateDay = formatDate  # '%Y%m%d'

    def __init__(self, user_settings):
        HistoricalMarketData.__init__(self, user_settings)
        self.receivedDataObject = self.ListenerIBClass()
        if self.ib_object is None:
            # self.ib_object = Connection.create(port=7497, clientId=100)
            self.ib_object = ibConnection(host=user_settings.ib_host,
                                          port=user_settings.ib_port,
                                          clientId=user_settings.ib_client_id)

            self.ib_object.register(self.receivedDataObject.historical_data_handler, 'HistoricalData')
            self.ib_object.register(self.receivedDataObject.error_handler, 'Error')
            self.ib_object.connect()

        self.tickId = 0
        self.lock = threading.Lock()
        pass

    def __formatTime__(self, timeColumn):
        import pandas as pd
        # The price comes from the daily info -
        #  so it would be the price at the end of the day GMT based on the requested TS

        # http://pvlib-python.readthedocs.io/en/latest/timetimezones.html
        originTZ = 'Etc/GMT'

        datetimeSeries = timeColumn  # pd.to_datetime(timeColumn)
        return pd.DatetimeIndex(pd.to_datetime(datetimeSeries, unit='ms')).tz_localize(originTZ).tz_convert(
            timezone_setting)

    def __formatHistorical__(self, input):
        import pandas as pd
        input_df = input
        output = pd.DataFrame(0, columns=Bar.columns, index=input_df.index)
        for column in output.columns:
            if column == Bar.time:
                timeProcessed = self.__formatTime__(input_df[self.columns_historical_dict[column]])
                output[column] = timeProcessed
            else:
                output[column] = input_df[self.columns_historical_dict[column]]

        output.set_index(Bar.index, inplace=True)

        return output

    def __getIBExpiry__(self, instrument):
        # TODO
        return instrument.maturity

    def __makeContract__(self, instrument):
        newContract = Contract()
        newContract.m_symbol = instrument.symbol
        newContract.m_secType = self.sec_type_dict[instrument.asset_type]
        newContract.m_exchange = self.exchange_dict[instrument.asset_type]
        newContract.m_currency = instrument.currency
        newContract.m_primaryExch = self.exchange_dict[instrument.asset_type]
        newContract.m_expiry = self.__getIBExpiry__(instrument)
        return newContract

    def __makeRequestSingle__(self, contract, durationStr, toDateString, barSizeSetting):

        self.receivedDataObject.reset()

        self.ib_object.reqHistoricalData(self.tickId,
                                         contract,
                                         toDateString,
                                         durationStr,
                                         barSizeSetting,
                                         self.whatToShow,
                                         1,
                                         self.ib_formatDate_return
                                         )
        self.tickId += 1

        logger.info("send req historical %s : waiting" % contract.m_symbol)

        while (self.receivedDataObject.receivedAllHistorical is False):
            sleep(3)

        logger.debug("finished single request %s " % contract.m_symbol)
        dataframeReceived = self.receivedDataObject.getDataframe()
        if dataframeReceived is None:
            logger.error("Some error appears on single request !! check it")
            return None
        # is necessary???
        outputComplete = self.__formatHistorical__(dataframeReceived)
        return outputComplete

    def getPeriodsRequests(self, period, fromDate, toDate):
        # split in different requests to respect maximun
        import math
        durationStr_list = []
        toDateString_list = []
        duration = (toDate - fromDate)

        maxDurationPeriod = self.max_duration_dict[period]
        numberOfPeriods = duration / maxDurationPeriod
        numberOfPeriods = math.ceil(max(1, numberOfPeriods))
        durationStr = self.max_durationStr_dict[period]

        toDateIte = None
        for i in range(numberOfPeriods):
            # maximun
            durationStr_list.append(durationStr)
            if i == 0:
                toDateIte = toDate
            else:
                toDateIte = toDateIte - maxDurationPeriod
            if period == Period.day:
                toDateString_list.append(toDateIte.strftime(self.formatDateDay))
            else:
                toDateString_list.append(toDateIte.strftime(self.formatDate))
        # return in order
        toDateString_list.reverse()
        durationStr_list.reverse()
        return durationStr_list, toDateString_list

    # minute limited to https://interactivebrokers.github.io/tws-api/historical_limitations.html
    def download(self, instrument, period, number_of_periods, fromDate, toDate=None):
        self.lock.acquire()
        logger.debug("Downloading %s %s from %s to %s" % (instrument.symbol, period, str(fromDate), str(toDate)))

        if instrument.asset_type == AssetType.forex:
            self.whatToShow = 'MIDPOINT'
        else:
            self.whatToShow = 'TRADES'

        contract = self.__makeContract__(instrument)

        period = self.period_dict[period]
        barSizeSetting = '%i %s' % (number_of_periods, period)
        if number_of_periods > 1 or period == Period.second:
            barSizeSetting = barSizeSetting + 's'

        outputAppended = None
        durationStr_list, toDateString_list = self.getPeriodsRequests(period, fromDate, toDate)
        for i in range(len(durationStr_list)):
            durationStr = durationStr_list[i]
            toDateString = toDateString_list[i]
            output_request = self.__makeRequestSingle__(contract, durationStr, toDateString, barSizeSetting)
            if outputAppended is None:
                outputAppended = output_request
            else:
                outputAppended = outputAppended.append(output_request)
        if outputAppended is None:
            logger.error("Couldnt download anything=> release return None")
        else:
            # clean unique index and sort
            outputAppended = outputAppended[~outputAppended.index.duplicated(keep='first')]
            outputAppended.sort_index(inplace=True)
            if fromDate is not None and toDate is not None:
                outputAppended = outputAppended[fromDate: toDate]
            elif fromDate is not None:
                outputAppended = outputAppended[fromDate:]
            elif toDate is not None:
                outputAppended = outputAppended[:toDate]
        self.lock.release()
        return outputAppended

    ######
    ### received Data
    class ListenerIBClass:
        close = []
        open = []
        high = []
        low = []
        volume = []
        date = []
        messageCounter = 0
        receivedAllHistorical = False
        notErrorsList = [2104, 2106]
        formatDate = '%Y%m%d %H:%M:%S'
        formatDateDay = '%Y%m%d'

        def __init__(self):
            self.close = []
            self.open = []
            self.high = []
            self.low = []
            self.volume = []
            self.time = []
            self.messageCounter = 0
            self.receivedAllHistorical = False
            pass

        def reset(self):
            self.close = []
            self.open = []
            self.high = []
            self.low = []
            self.volume = []
            self.time = []
            self.messageCounter = 0
            self.receivedAllHistorical = False

        def appendData(self, msg):
            self.time.append(self.dateToDatetime(msg.date))
            self.open.append(msg.open)
            self.close.append(msg.close)
            self.high.append(msg.high)
            self.volume.append(msg.volume)
            self.low.append(msg.low)

        def getDataframe(self):
            import pandas as pd
            if len(self.close) == 0:
                logger.error("Not saved anything to return data")
                print("Not saved anything to return data")
                return None
            output = pd.DataFrame(0, columns=Bar.columns, index=range(len(self.date)))
            output[Bar.close] = self.close
            output[Bar.open] = self.open
            output[Bar.high] = self.high
            output[Bar.low] = self.low
            output[Bar.volume] = self.volume
            output[Bar.time] = self.time

            return output

        def dateToDatetime(self, dateInt):
            import datetime
            date_time = str(dateInt)  # '29.08.2011 11:05:02'

            # datetime format YYYY-MM-DD HH:MM[:ss[.uuuuuu]][TZ] format.
            try:
                dateOut = datetime.datetime.strptime(date_time, self.formatDate)  # .strftime()
            except:
                dateOut = datetime.datetime.strptime(date_time, self.formatDateDay)  # .strftime()

            return dateOut

        def historical_data_handler(self, msg):
            logger.debug("IB historical received %s" % msg)

            if self.messageCounter == 0:
                logger.info("IB historical started! %s" % msg.date)
            self.messageCounter += 1
            if ('finished' in str(msg.date)) == True:
                logger.info("IB historical finished! %s" % msg.date)
                self.receivedAllHistorical = True
            else:
                self.appendData(msg)

        # %% Handlers
        def error_handler(self, msg):
            """Handles the capturing of error messages"""
            if msg.errorCode in self.notErrorsList:
                return
            logger.error("IB error received %s :code[%i] %s" % (msg, msg.errorCode, msg.errorMsg))

            print("Server Error: %s" % msg)
            self.receivedAllHistorical = True

        def my_tickprice_handler(self, msg):
            """Handles of server replies"""
            # 1 = bid
            # 2 = ask
            # 4 = last
            # 6 = high
            # 7 = low
            # 9 = close
            global askPriceArray, bidPriceArray, tickerToInstrumentArrayRealTime
            if msg.tickerId in tickerToInstrumentArrayRealTime:
                instrument = tickerToInstrumentArrayRealTime[msg.tickerId]
                if msg.field == 1:
                    bidPriceArray[instrument] = msg.price
                if msg.field == 2:
                    askPriceArray[instrument] = msg.price
                    # if instrument in askPriceArray and instrument in bidPriceArray:
                    # print "Update price %s  bid:%f ask:%f" % (instrument, bidPriceArray[instrument],askPriceArray[instrument])

        def my_ticksize_handler(self, msg):
            # 0 = bid size
            # 3 = ask size
            # 5 = last size
            # 8 = volume
            """Handles of server replies"""
            global askSizeArray, bidSizeArray, tickerToInstrumentArrayRealTime
            if msg.tickerId in tickerToInstrumentArrayRealTime:
                instrument = tickerToInstrumentArrayRealTime[msg.tickerId]
                if msg.field == 0:
                    bidSizeArray[instrument] = msg.size
                if msg.field == 3:
                    askSizeArray[instrument] = msg.size
                    # if instrument in bidSizeArray and instrument in askSizeArray:
                    # print "Update size %s  bid:%f ask:%f" % (instrument, bidSizeArray[instrument],askSizeArray[instrument])
