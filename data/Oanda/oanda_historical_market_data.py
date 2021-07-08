from collections import OrderedDict

import oandapy


def convrec(r, m):
    """convrec - convert OANDA candle record.

    return array of values, dynamically constructed, corresponding with config in mapping m.
    """
    v = []
    for keys in [x.split(":") for x in m.keys()]:
        _v = r.get(keys[0])
        for k in keys[1:]:
            _v = _v.get(k)
        v.append(_v)

    return v


def DataFrameFactory(r, colmap, conv):
    import pandas as pd
    df = pd.DataFrame([list(conv(rec, colmap)) for rec in r.get('candles')])
    df.columns = list(colmap.values())
    return df


class OandaHistoricalMarketData(HistoricalMarketData):
    candleFormat = 'midpoint'

    '''
    The hour of day used to align candles with
     hourly, daily, weekly, or monthly granularity.
      The value specified is interpretted as an hour in the timezone 
      set through the alignmentTimezone parameter and 
      must be an integer between 0 and 23. Default is 17 NY time
    '''
    dailyAlignment = 17

    '''
    alignmentTimezone: Optional The timezone to be used for the dailyAlignment parameter.
     This parameter does NOT affect the returned timestamp, 
     the start or end parameters, these will always be in UTC. 
     The timezone format used is defined by the IANA Time Zone Database,
      a full list of the timezones supported by the REST API can be found here.
    Etc/UTC
    The default for alignmentTimezone is “America/New_York” if the alignmentTimezone parameter is not specified.
    '''
    alignmentTimezone = 'Etc/UTC'
    weeklyAlignment = 'Friday'
    formatDate = '%Y-%m-%dT%H:%M:%S.%fZ'

    period_dict = \
        {  # '1min', '5min', '15min', '30min', '60min'
            Period.second: 'S',
            Period.day: 'D',
            Period.minute: 'M',
            Period.hour: 'H'
        }

    columns_historical_dict = \
        {
            Bar.close: 'closeMid',
            Bar.open: 'openMid',
            Bar.high: 'highMid',
            Bar.low: 'lowMid',
            Bar.time: 'time',
            Bar.volume: 'volume'
        }
    column_map_ohlcv = OrderedDict([
        ('time', Bar.time),
        ('openMid', Bar.open),
        ('highMid', Bar.high),
        ('lowMid', Bar.low),
        ('closeMid', Bar.close),
        ('volume', Bar.volume)
    ])

    oanda = None
    user_settings = None

    def __init__(self, user_settings):
        HistoricalMarketData.__init__(self, user_settings)
        self.user_settings = user_settings
        self.oanda = oandapy.API(environment=self.user_settings.oanda_environment,
                                 access_token=self.user_settings.oanda_token)
        pass

    def formatTime(self, timeColumn):
        import pandas as pd
        # The price comes from the daily info -
        #  so it would be the price at the end of the day GMT based on the requested TS

        # http://pvlib-python.readthedocs.io/en/latest/timetimezones.html
        # 2014-07-03T04:00:00.000000Z
        originTZ = 'America/New_York'

        datetimeSeries = pd.to_datetime(timeColumn)
        return pd.DatetimeIndex(pd.to_datetime(datetimeSeries, unit='ms')).tz_localize(originTZ).tz_convert(
            timezone_setting)

    def formatHistorical(self, input, period):
        output = DataFrameFactory(input, self.column_map_ohlcv, convrec)
        timeProcessed = self.formatTime(output[Bar.time])
        output[Bar.time] = timeProcessed

        output.set_index(Bar.index, inplace=True)

        return output

    # day unlimited
    # minute limited to 7 days!
    def download(self, instrument, period, number_of_periods, fromDate, toDate=None):
        import datetime
        logger.debug("Downloading %s" % instrument)

        oandaInstrument = '%s_%s' % (instrument.symbol, instrument.currency)
        if period == Period.day:
            oandaGranularity = self.period_dict[period]
        else:
            oandaGranularity = '%s%i' % (self.period_dict[period], number_of_periods)
        # 2014-07-03T04:00:00.000000Z

        startDate = fromDate.strftime(self.formatDate)
        if toDate is None:
            toDate = datetime.datetime.today()
        endDate = toDate.strftime(self.formatDate)
        try:
            data_downloaded = self.oanda.get_history(instrument=oandaInstrument,
                                                     granularity=oandaGranularity,
                                                     start=startDate,
                                                     end=endDate,
                                                     candleFormat=self.candleFormat,
                                                     dailyAlignment=self.dailyAlignment,
                                                     alignmentTimezone=self.alignmentTimezone,
                                                     weeklyAlignment=self.weeklyAlignment,
                                                     )
        except Exception as e:
            logger.error("Cant download from oanda %s %s=> return None   %s" % (instrument.symbol, period, e))
            return None
        logger.info("formatting oanda data for %s" % oandaInstrument)

        outputComplete = self.formatHistorical(data_downloaded, period=period)
        # Already added
        # outputComplete = self.setTimeCorrect(outputComplete, period=period, instrument=instrument)

        return outputComplete
