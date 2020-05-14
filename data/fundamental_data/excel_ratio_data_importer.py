import pandas as pd

from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.fundamental_data.fundamental_data import FundamentalData
from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.currency import Currency
from tradeasystems_connector.model.instrument import Instrument
from tradeasystems_connector.model.ratio import Ratio
from tradeasystems_connector.service.ratio_data_service import RatioDataService
from tradeasystems_connector.util.configuration_keys_util import getRatioInputPath


class ExcelRatioData(FundamentalData):
    filename_dict_sheet = {
        AssetType.us_equity: AssetType.us_equity,
        AssetType.es_equity: AssetType.es_equity,
    }

    dateFormat = "%Y-%m-%d"
    user_settings = None

    inputPath = None

    ratioDataService = None

    def __init__(self, user_settings):
        self.user_settings = user_settings
        self.inputPath = getRatioInputPath(user_settings)
        self.ratioDataService = RatioDataService(self.user_settings)
        pass

    def getFilesInPath(self, suffix='xlsx'):
        # File name shound be the asset type
        import glob
        import os
        filesInDirectory = glob.glob(self.inputPath + os.sep + "*.%s" % suffix)
        return filesInDirectory

    def getRatioFromSheet(self, sheetName):
        stringName = 'fundamental_%s' % sheetName
        return stringName

    def getSheetFromRatio(self, ratio):
        if "fundamental_" in ratio:
            param, value = ratio.split("fundamental_", 1)

        return value

    def getInstrument(self, filename):
        import os
        filenameArray = filename.split(os.sep)
        lastPart = filenameArray[-1]
        nameFile = os.path.splitext(lastPart)[0]
        nameFileList = nameFile.split('_')
        symbol = nameFileList[0]
        currency = nameFileList[1]
        if currency == Currency.eur:
            assetType = AssetType.es_equity
        elif currency == Currency.usd:
            assetType = AssetType.us_equity
        instrument = Instrument(symbol=symbol, asset_type=assetType, currency=currency)
        return instrument

    def formatFundamental(self, dataDownloaded):
        # 'Int64Index' object has no attribute 'tz_convert'
        output = dataDownloaded.copy()
        output.columns = [Ratio.index, Ratio.ratio]
        output.set_index(Ratio.index, inplace=True)
        return output

    def importExcel(self, file, instrument):
        reader = pd.ExcelFile(file)
        sheetNames = reader.sheet_names
        output = True
        for sheet in sheetNames:
            ratio = self.getRatioFromSheet(sheet)
            logger.debug('%s_%s reading %s' % (instrument.symbol, instrument.currency, ratio))
            dataDownloaded = reader.parse(sheet)

            df_to_save = self.formatFundamental(dataDownloaded)

            # save it to database
            if df_to_save is not None:
                logger.debug('%s_%s saving to database %s' % (instrument.symbol, instrument.currency, ratio))
                try:
                    self.ratioDataService.saveRatioDataFrame(df_to_save, instrument, ratio)
                except Exception as e:
                    logger.error(
                        'Error saving Ratio %s of %s_%s :%s' % (ratio, instrument.symbol, instrument.currency, str(e)))
                    output = False
            else:
                logger.error('Error getting Ratio %s of %s_%s' % (ratio, instrument.symbol, instrument.currency))
                output = False
        return output

    def download(self):
        filesInDirectory = self.getFilesInPath()
        output = True
        for file in filesInDirectory:
            if 'empty' in file:
                logger.debug('file: %s skipped=> empty is a FW' % file)
                continue
            instrument = self.getInstrument(file)
            logger.debug('Importing ratios of %s_%s' % (instrument.symbol, instrument.currency))
            isImported = self.importExcel(file, instrument)
            if isImported is False:
                logger.error('Error ratios of %s_%s :%s' % (instrument.symbol, instrument.currency, file))
            output = output and isImported
        return output
