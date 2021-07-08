import pandas as pd

from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.historical_market_data.historical_market_data import getCloseTimeHourMinInUTC


class FundamentalData:
    def __init__(self, user_settings):
        pass

    def downloadBatch(self, instrument, fundamental_ratio_list, fromDate, toDate=None):
        logger.debug('Downloading batch iterating... not own implementation')
        output = None
        for fundamental_ratio in fundamental_ratio_list:
            ratioDF = self.download(instrument, fundamental_ratio, fromDate, toDate)
            if output is None:
                output = pd.DataFrame(None, columns=fundamental_ratio_list, index=ratioDF.index)
            output[fundamental_ratio] = ratioDF
        return output

    def download(self, instrument, fundamental_ratio, fromDate, toDate=None):
        pass

    def setTimeCorrect(self, outputComplete, instrument):
        import pandas as pd
        # must be in UTC directly
        try:
            if outputComplete is not None and outputComplete.index[0].hour == 0:
                hour, minute = getCloseTimeHourMinInUTC(instrument)
                outputComplete.index = outputComplete.index + pd.DateOffset(hours=hour, minute=minute)
                # outputComplete.index=outputComplete.index.tz_convert( timezone_setting)
        except Exception as e:
            logger.error('Error setting time of fundamental data of %s:%s' % (instrument.symbol, str(e)))
            outputComplete = None
        return outputComplete
