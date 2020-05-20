import good_morning as gm
import joblib

from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.conf.region_settings import timezone_setting
from tradeasystems_connector.fundamental_data.fundamental_data import FundamentalData
from tradeasystems_connector.fundamental_data.morningstar_fundamental_calculator import getEnterpriseValue, \
    getOperatingAssets, getOperatingLiabilities, getDaySalesReceivables, getSalesGrowth, getBookValueLiabilities, \
    getAdjustedBookValue, getReturnOverAssets, getReturnOverCapital, getLongTermDebt, getShortTermDebt, \
    getCapitalization, getAccrual, getDaySalesReceivablesIndex, getGrossMargin, getGrossMarginIndex, \
    getAssetQualityIndex, getSalesGrowthIndex, getDepreciationIndex, getSGAI, getLVGI, getROAAvg8, getROCAvg8, getFCFA, \
    getMG, getLiquidity, getLeverageDiff, getROADiff, getFCFTADiff, getMarginDiff, getTurnDiff, getMS, getLeverage, \
    getBookValue, getCurrentRatio, getDebt, getBookValuePerShare, getFCFPerShare, getWorkingCapital
from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.fundamental_period import FundamentalPeriod
from tradeasystems_connector.model.ratio import Ratio
# http://financials.morningstar.com/
# pip install git+https://github.com/petercerno/good-morning.git
from tradeasystems_connector.util.configuration_keys_util import getTempPath

'''
['Revenue',
 'Interest income',
 'Loans and Leases',
 'Deposits with banks',
 'Other assets',
 'Total interest income',
 'Interest expense',
 'Deposits',
 'Other expense',
 'Total interest expense',
 'Net interest income',
 'Noninterest revenue',
 'Commissions and fees',
 'Lending and deposit-related fees',
 'Securities gains (losses)',
 'Credit card income',
 'Insurance premium',
 'Other income',
 'Total noninterest revenue',
 'Total net revenue',
 'Provisions for credit losses',
 'Noninterest expenses',
 'Compensation and benefits',
 'Tech, communication and equipment',
 'Other special charges',
 'Other expenses',
 'Total noninterest expenses',
 'Income (loss) from cont ops before taxes',
 'Provision (benefit) for taxes',
 'Other income (expense)',
 'Income from discontinued ops',
 'Net income',
 'Preferred dividend',
 'Net income available to common shareholders',
 'Earnings per share',
 'Basic',
 'Diluted',
 'Weighted average shares outstanding',
 'Basic',
 'Diluted',
 'Assets',
 'Cash and due from banks',
 'Trading assets',
 'Derivative assets',
 'Debt securities',
 'Loans',
 'Net loans',
 'Receivables',
 'Premises and equipment',
 'Goodwill',
 'Other intangible assets',
 'Other assets',
 'Total assets',
 "Liabilities and stockholders' equity",
 'Liabilities',
 'Deposits',
 'Trading liabilities',
 'Derivative liabilities',
 'Payables',
 'Long-term debt',
 'Other liabilities',
 'Total liabilities',
 "Stockholders' equity",
 'Common stock',
 'Other Equity',
 'Additional paid-in capital',
 'Retained earnings',
 'Treasury stock',
 'Accumulated other comprehensive income',
 "Total stockholders' equity",
 "Total liabilities and stockholders' equity",
 'Cash Flows From Operating Activities',
 'Provision for credit losses',
 'Depreciation & amortization',
 'Loans',
 'Other assets and liabilities',
 'Other operating activities',
 'Net cash provided by operating activities',
 'Cash Flows From Investing Activities',
 'Sales/maturity of investments',
 'Purchases of investments',
 'Acquisitions and dispositions',
 'Purchases of intangibles, net',
 'Property, and equipments, net',
 'Net cash used for investing activities',
 'Cash Flows From Financing Activities',
 'Long-term debt issued',
 'Long-term debt repayment',
 'Cash dividends paid',
 'Other financing activities',
 'Net cash provided by (used for) financing activities',
 'Effect of exchange rate changes',
 'Net change in cash',
 'Cash at beginning of period',
 'Cash at end of period']
 
 
 ['Revenue',
 'Cost of revenue',
 'Gross profit',
 'Operating expenses',
 'Research and development',
 'Sales, General and administrative',
 'Total operating expenses',
 'Operating income',
 'Interest Expense',
 'Other income (expense)',
 'Income before taxes',
 'Provision for income taxes',
 'Net income from continuing operations',
 'Net income',
 'Net income available to common shareholders',
 'Earnings per share',
 'Basic',
 'Diluted',
 'Weighted average shares outstanding',
 'Basic',
 'Diluted',
 'EBITDA',
 'Assets',
 'Current assets',
 'Cash',
 'Cash and cash equivalents',
 'Short-term investments',
 'Total cash',
 'Receivables',
 'Inventories',
 'Deferred income taxes',
 'Other current assets',
 'Total current assets',
 'Non-current assets',
 'Property, plant and equipment',
 'Gross property, plant and equipment',
 'Accumulated Depreciation',
 'Net property, plant and equipment',
 'Equity and other investments',
 'Goodwill',
 'Intangible assets',
 'Other long-term assets',
 'Total non-current assets',
 'Total assets',
 "Liabilities and stockholders' equity",
 'Liabilities',
 'Current liabilities',
 'Short-term debt',
 'Accounts payable',
 'Taxes payable',
 'Accrued liabilities',
 'Deferred revenues',
 'Other current liabilities',
 'Total current liabilities',
 'Non-current liabilities',
 'Long-term debt',
 'Deferred taxes liabilities',
 'Deferred revenues',
 'Other long-term liabilities',
 'Total non-current liabilities',
 'Total liabilities',
 "Stockholders' equity",
 'Common stock',
 'Additional paid-in capital',
 'Retained earnings',
 'Accumulated other comprehensive income',
 "Total stockholders' equity",
 "Total liabilities and stockholders' equity",
 'Cash Flows From Operating Activities',
 'Net income',
 'Depreciation & amortization',
 'Deferred income taxes',
 'Stock based compensation',
 'Change in working capital',
 'Accounts receivable',
 'Inventory',
 'Accounts payable',
 'Other working capital',
 'Other non-cash items',
 'Net cash provided by operating activities',
 'Cash Flows From Investing Activities',
 'Investments in property, plant, and equipment',
 'Acquisitions, net',
 'Purchases of investments',
 'Sales/Maturities of investments',
 'Purchases of intangibles',
 'Other investing activities',
 'Net cash used for investing activities',
 'Cash Flows From Financing Activities',
 'Debt issued',
 'Debt repayment',
 'Common stock issued',
 'Common stock repurchased',
 'Dividend paid',
 'Other financing activities',
 'Net cash provided by (used for) financing activities',
 'Net change in cash',
 'Cash at beginning of period',
 'Cash at end of period',
 'Free Cash Flow',
 'Operating cash flow',
 'Capital expenditure',
 'Free cash flow']
 
 
'''


class MorningStarFundamentalData(FundamentalData):
    ratio_dict = {
        Ratio.fundamental_current_liabilities_Y: 'Current liabilities',
        Ratio.fundamental_current_assets_Y: 'Current assets',
        Ratio.fundamental_ebitda_Y: 'EBITDA',
        Ratio.fundamental_net_asset_value_Y: None,
        Ratio.fundamental_earnings_per_share_Y: 'Earnings per share',
        Ratio.fundamental_cost_of_goods_sold_Y: 'Cost of revenue',  # 'COGS',
        Ratio.fundamental_current_ratio_Y: None,
        Ratio.fundamental_enterprise_value_Y: None,
        Ratio.fundamental_gross_profit_Y: 'Gross profit',
        Ratio.fundamental_assets_Y: 'Assets',
        Ratio.fundamental_debt_Y: None,
        Ratio.fundamental_liabilities_Y: 'Liabilities',
        Ratio.fundamental_net_income_Y: 'Net income',
        Ratio.fundamental_revenue_Y: 'Revenue',
        Ratio.fundamental_market_capitalization_Y: "Stockholders' equity",  # 'MARKETCAP',
        Ratio.fundamental_shares_Y: 'Weighted average shares outstanding',  # 'SHARESBAS',
        Ratio.fundamental_book_value_per_share_Y: None,
        Ratio.fundamental_free_cashflow_per_share_Y: None,
        Ratio.fundamental_operating_cashflow_Y: 'Cash Flows From Operating Activities',
        Ratio.fundamental_operating_assets_Y: 'Assets',
        Ratio.fundamental_operating_liabilities_Y: 'Liabilities',
        Ratio.fundamental_day_sales_receivables_Y: None,
        Ratio.fundamental_non_current_assets_Y: 'Non-current assets',
        Ratio.fundamental_sales_growth_Y: None,
        Ratio.fundamental_depreciation_Y: 'Depreciation & amortization',
        Ratio.fundamental_working_capital_Y: None,
        Ratio.fundamental_book_value_liabilities_Y: None,
        Ratio.fundamental_cash_and_equivalents_Y: 'Cash and cash equivalents',
        Ratio.fundamental_adjusted_book_value_Y: None,
        Ratio.fundamental_ebit_Y: 'Income before taxes',
        Ratio.fundamental_return_over_assets_Y: None,
        Ratio.fundamental_return_over_capital_Y: None,
        Ratio.fundamental_free_cashflow_Y: 'Free Cash Flow',
        Ratio.fundamental_long_term_debt_Y: 'Long-term debt',
        Ratio.fundamental_short_term_debt_Y: 'Short-term debt',
        Ratio.fundamental_net_equity_issuance_Y: 'Common stock issued',  # todo check it
        Ratio.fundamental_receivables_Y: 'Receivables',
        # Ratio.fundamental_capital_expenditure_Y: 'CAPEX',
        Ratio.fundamental_inventory_Y: 'Inventory',
        Ratio.fundamental_selling_general_administrative_expenses_Y: 'Sales, General and administrative',

    }

    columns_ratio_dict = \
        {

            Ratio.time: 'index',
        }

    dateFormat = "%Y-%m-%d"

    user_settings = None
    financial_downloader = None
    key_ratios_downloader = None
    folders = ['income_statement', 'balance_sheet', 'cash_flow']

    def __init__(self, user_settings):

        self.user_settings = user_settings
        self.financial_downloader = gm.FinancialsDownloader()
        # self.key_ratios_downloader = gm.KeyRatiosDownloader()
        self.temp_dir = getTempPath(userSettings=user_settings)
        self.cacher = joblib.Memory(self.temp_dir)

        # kr = gm.KeyRatiosDownloader()
        pass

    def getMorningStarTicker(self, instrument):
        output = instrument.symbol
        if instrument.asset_type == AssetType.es_equity and '.' in output:
            output = output.split('.')[0]
        return output

    def calculateRatio(self, instrument, fundamental_ratio, fromDate, toDate):

        fundamental_period = FundamentalPeriod.yearly
        if fundamental_ratio.startswith('fundamental_net_asset_value'):
            return getBookValue(self, instrument, fundamental_period)

        if fundamental_ratio.startswith('fundamental_debt'):
            return getDebt(self, instrument, fundamental_period)

        if fundamental_ratio.startswith('fundamental_market_capitalization'):
            return getCapitalization(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_current_ratio'):
            return getCurrentRatio(self, instrument, fundamental_period, fromDate, toDate)

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

        if fundamental_ratio.startswith('fundamental_book_value_per_share'):
            return getBookValuePerShare(self, instrument, fundamental_period)

        if fundamental_ratio.startswith('fundamental_free_cashflow_per_share'):
            return getFCFPerShare(self, instrument, fundamental_period, fromDate, toDate)

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

        if fundamental_ratio.startswith('fundamental_working_capital'):
            return getWorkingCapital(self, instrument, fundamental_period, fromDate, toDate)

        if fundamental_ratio.startswith('fundamental_accrual'):
            return getAccrual(self, instrument, fundamental_period, fromDate, toDate)

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

    def downloadAllFinancials(self, instrument):
        try:
            symbol = self.getMorningStarTicker(instrument)
            functionTemp = self.cacher.cache(self.financial_downloader.download, ignore=['self'])
            return functionTemp(symbol)
        except Exception as e:
            logger.error('Error downloading morningstar data of %s:%s' % (instrument, str(e)))
            return None

    def format_to_df(self, all_fund_data, folder):
        import pandas as pd
        firstRow = 2
        month_fiscal_year_end = all_fund_data['fiscal_year_end']

        ## get columns
        columns = all_fund_data[folder]['title']
        dataframeFormatted = all_fund_data[folder].T

        # get index
        newIndex = []
        indexYears = list(dataframeFormatted.index[firstRow:])
        for period in indexYears:
            year = period.year
            month = month_fiscal_year_end
            day = 1

            while month > 12:
                month = month - 12
                year += 1

            object = pd.datetime(year=year, month=month, day=day)
            newIndex.append(object)

        # create dataframe
        output = pd.DataFrame(dataframeFormatted.values[firstRow:, :], columns=columns, index=newIndex)
        return output

    def getRatio(self, all_fund_data, fundamental_ratio):
        allRatios = list(all_fund_data['income_statement']['title'].values) + list(
            all_fund_data['balance_sheet']['title'].values) + list(all_fund_data['cash_flow']['title'].values)
        if fundamental_ratio in self.ratio_dict.keys():
            for folder in self.folders:
                df = self.format_to_df(all_fund_data, folder)
                if not self.ratio_dict[fundamental_ratio] in df.columns:
                    continue
                else:
                    return df[self.ratio_dict[fundamental_ratio]]
        if fundamental_ratio in self.ratio_dict.keys() and self.ratio_dict[fundamental_ratio] is not None:
            logger.error('%s not found in morningstar   => return 0 ' % fundamental_ratio)
            # TODO return zero df
            df = self.format_to_df(all_fund_data, 'income_statement')
            df['new'] = 0
            return df['new']

        return None

    def download(self, instrument, fundamental_ratio, fromDate=None, toDate=None):

        all_fund_data = self.downloadAllFinancials(instrument)
        if all_fund_data is None:
            logger.error('Cant Download fundamental data for %s in morningstar' % instrument)
            return None

        dataDownloaded = self.getRatio(all_fund_data, fundamental_ratio)

        if dataDownloaded is None:
            # calculate
            dataDownloaded = self.calculateRatio(instrument=instrument,
                                                 fundamental_ratio=fundamental_ratio,
                                                 fromDate=fromDate,
                                                 toDate=toDate)

            if dataDownloaded is None:
                logger.error('Cant download/calculate %s in morningstar for %s' % (fundamental_ratio, instrument))

            return dataDownloaded
        else:
            dataDownloaded = self.formatFundamental(dataDownloaded)
            dataDownloaded = self.setTimeCorrect(dataDownloaded, instrument)
            if fromDate is not None and toDate is not None:
                dataDownloaded = dataDownloaded[fromDate:toDate]
            elif fromDate is not None and toDate is None:
                dataDownloaded = dataDownloaded[fromDate:]

        return dataDownloaded

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

        columnsRatio = list(input_df.columns)
        columnsRatio.remove(self.columns_ratio_dict[Ratio.time])
        if len(columnsRatio) > 1:
            logger.error('Error more 1 column in morningstar: %s' % str(columnsRatio))
            return None
        columnName = columnsRatio[0]

        output = pd.DataFrame(input, columns=Ratio.columns, index=input_df.index)

        for column in output.columns:
            if column == Ratio.time:
                timeProcessed = self.formatTime(input_df[self.columns_ratio_dict[column]])
                output[column] = timeProcessed
            else:
                output[column] = input_df[columnName]

        output.set_index(Ratio.index, inplace=True)

        return output
