import joblib
import quandl

from tradeasystems_connector.conf.region_settings import timezone_setting
from tradeasystems_connector.fundamental_data.fundamental_data import FundamentalData
from tradeasystems_connector.fundamental_data.quandl_fundamental_calculator import *
from tradeasystems_connector.model.fundamental_period import FundamentalPeriod
from tradeasystems_connector.model.ratio import Ratio
from tradeasystems_connector.util.configuration_keys_util import getTempPath


def calculateRatio(quandl_fundamental_data, instrument, fundamental_ratio, fromDate, toDate, df=None):
    fundamental_period = quandl_fundamental_data.getFrequencyFundamentalRatio(fundamental_ratio)

    if fundamental_ratio.startswith('fundamental_net_asset_value'):
        output = getNetAssetValue(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_market_capitalization'):
        output = getCapitalization(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_enterprise_value'):
        output = getEnterpriseValue(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_operating_assets'):
        output = getOperatingAssets(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_operating_liabilities'):
        output = getOperatingLiabilities(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_day_sales_receivables'):
        output = getDaySalesReceivables(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_sales_growth'):
        output = getSalesGrowth(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_book_value_liabilities'):
        output = getBookValueLiabilities(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_adjusted_book_value'):
        output = getAdjustedBookValue(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_return_over_assets'):
        output = getReturnOverAssets(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate)

    if fundamental_ratio.startswith('fundamental_return_over_equity'):
        output = getReturnOverEquity(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate)

    if fundamental_ratio.startswith('fundamental_book_market'):
        output = getReturnOverEquity(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_return_over_capital'):
        output = getReturnOverCapital(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_long_term_debt'):
        output = getLongTermDebt(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_short_term_debt'):
        output = getShortTermDebt(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_leverage'):
        output = getLeverage(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_accrual'):
        output = getAccrual(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_day_sales_receivables_index'):
        output = getDaySalesReceivablesIndex(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate,
                                             df)

    if fundamental_ratio.startswith('fundamental_gross_margin'):
        output = getGrossMargin(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_gross_margin_index'):
        output = getGrossMarginIndex(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_asset_quality_index'):
        output = getAssetQualityIndex(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_sales_growth_index'):
        output = getSalesGrowthIndex(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_depreciation_index'):
        output = getDepreciationIndex(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_sgai'):
        output = getSGAI(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_lvgi'):
        output = getLVGI(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_roa_8_year_avg'):
        output = getROAAvg8(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_roc_8_year_avg'):
        output = getROCAvg8(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_fcfa'):
        output = getFCFA(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_mg'):
        output = getMG(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_liquidity'):
        output = getLiquidity(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_leverageDiff'):
        output = getLeverageDiff(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_roaDiff'):
        output = getROADiff(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_fcftaDiff'):
        output = getFCFTADiff(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_marginDiff'):
        output = getMarginDiff(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_turnDiff'):
        output = getTurnDiff(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_ms'):
        output = getMS(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    if fundamental_ratio.startswith('fundamental_leverage'):
        output = getLeverage(quandl_fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

    # TODO add more ratios
    if df is not None:
        df[fundamental_ratio] = output

    return output


class QuandlFundamentalData(FundamentalData):
    ratio_dict = {
        Ratio.fundamental_ebitda_Y: 'EBITDA',
        Ratio.fundamental_net_asset_value_Y: None,
        Ratio.fundamental_earnings_per_share_Y: 'EPS',
        Ratio.fundamental_cost_of_goods_sold_Y: 'COR',  # 'COGS',
        Ratio.fundamental_current_ratio_Y: 'CURRENTRATIO',
        Ratio.fundamental_enterprise_value_Y: None,
        Ratio.fundamental_gross_profit_Y: 'GP',
        Ratio.fundamental_assets_Y: 'ASSETS',
        Ratio.fundamental_debt_Y: 'DEBT',
        Ratio.fundamental_liabilities_Y: 'LIABILITIES',
        Ratio.fundamental_net_income_Y: 'NETINC',
        Ratio.fundamental_revenue_Y: 'REVENUE',
        Ratio.fundamental_market_capitalization_Y: None,  # 'MARKETCAP',
        Ratio.fundamental_shares_Y: 'SHARESWA',  # 'SHARESBAS',
        Ratio.fundamental_book_value_per_share_Y: 'BVPS',
        Ratio.fundamental_free_cashflow_per_share_Y: 'FCFPS',
        Ratio.fundamental_operating_cashflow_Y: 'NCFO',
        Ratio.fundamental_operating_assets_Y: 'ASSETSC',
        Ratio.fundamental_operating_liabilities_Y: 'LIABILITIESC',
        Ratio.fundamental_day_sales_receivables_Y: None,
        Ratio.fundamental_non_current_assets_Y: 'ASSETSNC',
        Ratio.fundamental_sales_growth_Y: None,
        Ratio.fundamental_depreciation_Y: 'DEPAMOR',
        Ratio.fundamental_working_capital_Y: 'WORKINGCAPITAL',
        Ratio.fundamental_book_value_liabilities_Y: None,
        Ratio.fundamental_cash_and_equivalents_Y: 'CASHNEQUSD',
        Ratio.fundamental_adjusted_book_value_Y: None,
        Ratio.fundamental_ebit_Y: 'EBIT',
        Ratio.fundamental_return_over_assets_Y: None,
        Ratio.fundamental_return_over_capital_Y: None,
        Ratio.fundamental_free_cashflow_Y: 'FCF',
        Ratio.fundamental_long_term_debt_Y: None,
        Ratio.fundamental_short_term_debt_Y: None,
        Ratio.fundamental_net_equity_issuance_Y: 'NCFCOMMON',
        Ratio.fundamental_receivables_Y: 'RECEIVABLES',
        # Ratio.fundamental_capital_expenditure_Y: 'CAPEX',
        Ratio.fundamental_inventory_Y: 'INVENTORY',
        Ratio.fundamental_selling_general_administrative_expenses_Y: 'SGNA',

    }

    columns_ratio_dict = \
        {
            Ratio.ratio: 'Value',
            Ratio.time: 'Date',
        }

    temp_dir = None
    cacher = None
    cacheFunction = True  # avoid overloading developing

    class QuandlFrequency:
        quaterly = ['SF1', 'MRQ', 60]
        annual = ['SF0', 'MRY', 250]
        annual_prem = ['SF1', 'MRY', 250]

    # quandlDatabase = 'SHARADAR'
    frequencyChosen = QuandlFrequency.annual
    dateFormat = "%Y-%m-%d"

    tokenKey = None
    user_settings = None
    quandl = None

    # SF0 / AAPL_DEBTUSD_MRY
    def __init__(self, user_settings):
        self.tokenKey = user_settings.quandl_token
        self.user_settings = user_settings
        quandl.ApiConfig.api_key = self.tokenKey

        self.temp_dir = getTempPath(userSettings=user_settings)
        self.cacher = joblib.Memory(self.temp_dir, compress=9, verbose=0)

        pass

    def calculateRatio(self, instrument, fundamental_ratio, fromDate, toDate, df=None):
        return calculateRatio(self, instrument, fundamental_ratio, fromDate, toDate, df)

    def calculateRatioDataframe(self, instrument, fundamental_ratio_list, fromDate, toDate):
        raise NotImplementedError

    def makeQuandlName(self, instrument, fundamental_ratio):
        fundamental = None
        if fundamental_ratio in self.ratio_dict.keys():
            fundamental = self.ratio_dict[fundamental_ratio]
        if fundamental is None:
            # calculate it
            # logger.debug('%s not found in quandl=> calculating ' % fundamental_ratio)
            return None
        else:
            # SF0 / PVH_TBVPS_MRY
            return '%s/%s_%s_%s' % (self.frequencyChosen[0], instrument.symbol, fundamental, self.frequencyChosen[1])

    def getFrequencyFundamentalRatio(self, fundamental_ratio):
        if fundamental_ratio.endswith('Y'):
            fundamental_period = FundamentalPeriod.yearly
        if fundamental_ratio.endswith('Q'):
            fundamental_period = FundamentalPeriod.quaterly
        return fundamental_period

    def setFrequencyChosen(self, fundamental_ratio):
        fundamental_period = self.getFrequencyFundamentalRatio(fundamental_ratio)
        if fundamental_period == FundamentalPeriod.yearly:
            self.frequencyChosen = self.QuandlFrequency.annual
        if fundamental_period == FundamentalPeriod.quaterly:
            self.frequencyChosen = self.QuandlFrequency.quaterly

    def downloadBatch(self, instrument, fundamental_ratio_list, fromDate, toDate=None):
        self.setFrequencyChosen(fundamental_ratio_list[0])
        tableNameList = []

        quandl_ratios_list = []
        columnsToRatio = {}
        calculated_ratios_list = []
        for fundamental_ratio in fundamental_ratio_list:
            tableName = self.makeQuandlName(instrument, fundamental_ratio=fundamental_ratio)
            if tableName is not None:
                quandl_ratios_list.append(fundamental_ratio)
                tableNameList.append(tableName)
                columnsToRatio[fundamental_ratio] = tableName
            else:
                calculated_ratios_list.append(fundamental_ratio)
        logger.debug('downloading quandl list: %s ' % tableNameList)
        dataDownloaded = None
        if len(tableNameList) > 0:
            try:
                if fromDate is not None and toDate is not None:
                    dataDownloaded = self.quandl_get(tableNameList, start_date=fromDate.strftime(self.dateFormat),
                                                     end_date=toDate.strftime(self.dateFormat))
                elif fromDate is not None and toDate is None:
                    dataDownloaded = self.quandl_get(tableNameList, start_date=fromDate.strftime(self.dateFormat))
                else:
                    dataDownloaded = self.quandl_get(tableNameList)
            except Exception as e:
                logger.error('error downloading %s for %s  => return none %s' % (tableName, instrument.symbol, e))
                return None
            if dataDownloaded.empty:
                logger.error('Not download anything from quandl %s' % instrument)
                return None
            dataDownloaded = self.formatFundamentalBatch(input=dataDownloaded, columnsToRatio=columnsToRatio)

        #
        # tableNames = list(dataDownloaded.columns)
        # dataDownloadedChanged = dataDownloaded.rename(tableNames, columnsToRatio[tableNames])
        logger.debug('Calculating ratios %s: %s ' % (instrument, calculated_ratios_list))
        for calculated_ratio in calculated_ratios_list:
            dataCalculated = self.calculateRatio(instrument=instrument, fundamental_ratio=calculated_ratio,
                                                 fromDate=fromDate, toDate=toDate, df=dataDownloaded)
            if dataCalculated is not None:
                if isinstance(dataCalculated, pd.DataFrame):
                    series = dataCalculated[Ratio.ratio]
                else:
                    series = dataCalculated

                if dataDownloaded is None or dataDownloaded.empty:
                    dataDownloaded = pd.DataFrame(series.values, columns=[calculated_ratio], index=series.index)
                else:
                    try:
                        dataDownloaded[calculated_ratio] = series.values
                    except Exception as e:
                        logger.error('Error setting %s in %s :%s' % (calculated_ratio, instrument, str(e)))

        # output = self.formatFundamental(dataDownloaded)
        # output = self.setTimeCorrect(dataDownloaded, instrument)

        return dataDownloaded

    def quandl_get(self, table_name_list, start_date=None, end_date=None):
        if self.cacheFunction is True:
            functionTemp = self.cacher.cache(quandl.get)
        else:
            functionTemp = quandl.get
        if start_date is not None and end_date is not None:
            return functionTemp(table_name_list, start_date=start_date, end_date=end_date)
        if start_date is None and end_date is not None:
            return functionTemp(table_name_list, end_date=end_date)
        if start_date is not None and end_date is None:
            return functionTemp(table_name_list, start_date=start_date)
        if start_date is None and end_date is None:
            return functionTemp(table_name_list)

    def download(self, instrument, fundamental_ratio, fromDate=None, toDate=None, df=None):

        self.setFrequencyChosen(fundamental_ratio)

        tableName = self.makeQuandlName(instrument, fundamental_ratio=fundamental_ratio)
        if tableName is None:
            logger.debug('calculating  quandl  %s ' % fundamental_ratio)
            output = self.calculateRatio(instrument, fundamental_ratio, fromDate, toDate, df)
        else:
            logger.debug('downloading quandl %s ' % tableName)
            try:
                if fromDate is not None and toDate is not None:
                    dataDownloaded = self.quandl_get(tableName, start_date=fromDate.strftime(self.dateFormat),
                                                     end_date=toDate.strftime(self.dateFormat))
                elif fromDate is not None and toDate is None:
                    dataDownloaded = self.quandl_get(tableName, start_date=fromDate.strftime(self.dateFormat))
                else:
                    dataDownloaded = self.quandl_get(tableName)
            except Exception as e:
                logger.error('error downloading %s for %s  => return none %s' % (tableName, instrument.symbol, e))
                return None
            # dataDownloaded = quandl.get_table(tableName,authtoken=self.tokenKey)
            if len(dataDownloaded) == 0:
                logger.error('downloading empty  %s for %s  => return none ' % (tableName, instrument.symbol))
                return None
            output = self.formatFundamental(dataDownloaded)
            output = self.setTimeCorrect(output, instrument)
        return output

    def formatTime(self, timeColumn):
        import pandas as pd
        # The price comes from the daily info -
        #  so it would be the price at the end of the day GMT based on the requested TS

        # http://pvlib-python.readthedocs.io/en/latest/timetimezones.html
        originTZ = 'Etc/GMT'

        datetimeSeries = pd.to_datetime(timeColumn)

        return pd.DatetimeIndex(pd.to_datetime(datetimeSeries, unit='ms')).tz_localize(originTZ).tz_convert(
            timezone_setting)

    def formatFundamental(self, input):
        import pandas as pd
        input_df = input.copy().reset_index()

        output = pd.DataFrame(0, columns=Ratio.columns, index=input_df.index)
        for column in output.columns:
            if column == Ratio.time:
                timeProcessed = self.formatTime(input_df[self.columns_ratio_dict[column]])
                output[column] = timeProcessed
            else:
                output[column] = input_df[self.columns_ratio_dict[column]]

        output.set_index(Ratio.index, inplace=True)

        return output

    def formatFundamentalBatch(self, columnsToRatio, input):
        import pandas as pd
        input_df = input.copy().reset_index()
        columnsOutput = list(columnsToRatio.keys()) + [Ratio.time]
        output = pd.DataFrame(0, columns=columnsOutput, index=input_df.index)
        for column in output.columns:
            if column == Ratio.time:
                if 'Date' in list(input_df.columns):
                    timeProcessed = self.formatTime(input_df['Date'])
                    output[column] = timeProcessed
            else:
                indexColumn = columnsToRatio[column] + ' - Value'
                if indexColumn in list(input_df.columns):
                    output[column] = input_df[columnsToRatio[column] + ' - Value']

        output.set_index(Ratio.index, inplace=True)

        return output
