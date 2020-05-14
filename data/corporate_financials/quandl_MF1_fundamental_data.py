import quandl

from tradeasystems_connector.conf.region_settings import timezone_setting
from tradeasystems_connector.fundamental_data.fundamental_data import FundamentalData
from tradeasystems_connector.fundamental_data.quandl_fundamental_calculator import *
from tradeasystems_connector.model.fundamental_period import FundamentalPeriod
from tradeasystems_connector.model.ratio import Ratio


# https://www.quandl.com/databases/MF1/documentation/coverage-and-data-organization
class QuandlMF1FundamentalData(FundamentalData):
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

    class QuandlFrequency:
        quaterly = ['MER/F1', 'MRQ', 60]
        annual = ['MER/F1', 'MRY', 250]

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
        pass

    def makeQuandlName(self, instrument, fundamental_ratio):
        fundamental = None
        tableName = self.frequencyChosen[0]

        # quandl.get_table('MER/F1', compnumber='372', reportdate='2010-12-31,2011-03-31,2011-06-30')

        compnumber = None

        if fundamental_ratio in self.ratio_dict.keys():
            reportid = self.ratio_dict[fundamental_ratio]
        if fundamental is None:
            # calculate it
            logger.debug('%s not found in quandl=> calculating ' % fundamental_ratio)
            return None, None, None
        else:
            return tableName, compnumber, reportid
            # SF0 / PVH_TBVPS_MRY

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

    def calculateRatio(self, instrument, fundamental_ratio, fromDate, toDate):

        fundamental_period = self.getFrequencyFundamentalRatio(fundamental_ratio)
        if fundamental_ratio.startswith('fundamental_net_asset_value'):
            return getNetAssetValue(self, instrument, fundamental_period)

        if fundamental_ratio.startswith('fundamental_market_capitalization'):
            return getCapitalization(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_enterprise_value'):
            return getEnterpriseValue(self, instrument, fundamental_period)

        if fundamental_ratio.startswith('fundamental_operating_assets'):
            return getOperatingAssets(self, instrument, fundamental_period)

        if fundamental_ratio.startswith('fundamental_operating_liabilities'):
            return getOperatingLiabilities(self, instrument, fundamental_period)

        if fundamental_ratio.startswith('fundamental_day_sales_receivables'):
            return getDaySalesReceivables(self, instrument, fundamental_period)

        if fundamental_ratio.startswith('fundamental_sales_growth'):
            return getSalesGrowth(self, instrument, fundamental_period)

        if fundamental_ratio.startswith('fundamental_book_value_liabilities'):
            return getBookValueLiabilities(self, instrument, fundamental_period)

        if fundamental_ratio.startswith('fundamental_adjusted_book_value'):
            return getAdjustedBookValue(self, instrument, fundamental_period)

        if fundamental_ratio.startswith('fundamental_return_over_assets'):
            return getReturnOverAssets(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_return_over_capital'):
            return getReturnOverCapital(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_long_term_debt'):
            return getLongTermDebt(self, instrument, fundamental_period)

        if fundamental_ratio.startswith('fundamental_short_term_debt'):
            return getShortTermDebt(self, instrument, fundamental_period)

        if fundamental_ratio.startswith('fundamental_leverage'):
            return getLeverage(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_accrual'):
            return getAccrual(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_day_sales_receivables_index'):
            return getDaySalesReceivablesIndex(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_gross_margin'):
            return getGrossMargin(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_gross_margin_index'):
            return getGrossMarginIndex(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_asset_quality_index'):
            return getAssetQualityIndex(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_sales_growth_index'):
            return getSalesGrowthIndex(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_depreciation_index'):
            return getDepreciationIndex(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_sgai'):
            return getSGAI(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_lvgi'):
            return getLVGI(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_roa_8_year_avg'):
            return getROAAvg8(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_roc_8_year_avg'):
            return getROCAvg8(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_fcfa'):
            return getFCFA(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_mg'):
            return getMG(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_liquidity'):
            return getLiquidity(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_leverageDiff'):
            return getLeverageDiff(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_roaDiff'):
            return getROADiff(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_fcftaDiff'):
            return getFCFTADiff(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_marginDiff'):
            return getMarginDiff(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_turnDiff'):
            return getTurnDiff(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_ms'):
            return getMS(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_leverage'):
            return getLeverage(self, instrument, fundamental_period, fromDate, toDate)

        # TODO add more ratios

        pass

    def download(self, instrument, fundamental_ratio, fromDate=None, toDate=None):

        self.setFrequencyChosen(fundamental_ratio)
        # uandl.get_table('MER/F1', reportdate='2010-12-31', compnumber='372')
        tableName, compNumber = self.makeQuandlName(instrument, fundamental_ratio=fundamental_ratio)
        if tableName is None:
            logger.debug('calculating  quandl  %s ' % fundamental_ratio)
            output = self.calculateRatio(instrument, fundamental_ratio, fromDate, toDate)
        else:
            logger.debug('downloading quandl %s ' % tableName)
            try:
                if fromDate is not None and toDate is not None:
                    dataDownloaded = quandl.get(tableName, compnumber=compNumber,
                                                reportdate='%s:%s' % (fromDate.strftime(self.dateFormat),
                                                                      toDate.strftime(self.dateFormat)))
                elif fromDate is not None and toDate is None:
                    dataDownloaded = quandl.get(tableName, start_date=fromDate.strftime(self.dateFormat))
                else:
                    dataDownloaded = quandl.get(tableName)
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
