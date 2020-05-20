import datetime

from scipy.stats import stats
from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.model.bar import Bar
from tradeasystems_connector.model.fundamental_period import FundamentalPeriod
from tradeasystems_connector.model.period import Period
from tradeasystems_connector.model.ratio import Ratio

periodsToShift = 1


def getCapitalization(fundamental_data, instrument, fundamental_period, fromDate=None, toDate=None):
    import datetime
    from tradeasystems_connector.service.historical_market_data_service import HistoricalMarketDataService

    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_shares_Y
        else:
            ratio = Ratio.fundamental_shares_Q

        shares = fundamental_data.download(instrument, ratio, fromDate, toDate)
        if shares is None or len(shares) == 0:
            logger.error('error downloading %s for %s => return capitalization none' % (ratio, instrument.symbol))
            return None

        marketDataServices = HistoricalMarketDataService(fundamental_data.user_settings)
        if fromDate is None:
            fromDate = shares.index[0].replace(tzinfo=None) - datetime.timedelta(days=3)
        else:
            fromDate = fromDate - datetime.timedelta(days=3)

        if toDate is None:
            toDate = shares.index[-1].replace(tzinfo=None) + datetime.timedelta(days=3)
        else:
            toDate = toDate + datetime.timedelta(days=3)

        barPrices = marketDataServices.getHistoricalData(instrument, period=Period.day, number_of_periods=1,
                                                         force_download=True,
                                                         fromDate=fromDate,
                                                         toDate=toDate
                                                         )

        capitalization = shares.copy()
        closesIndexBar = barPrices.index.searchsorted(capitalization.index) - 1
        closesIndexBar = closesIndexBar[closesIndexBar < barPrices.shape[0]]

        shares[Bar.close] = barPrices[Bar.close][closesIndexBar].values
        capitalization[Ratio.ratio] = shares[Bar.close] * shares[Bar.close]
    except Exception as e:
        logger.error('Error getting capitalization for %s %s => return None' % (instrument.symbol, str(e)))
        capitalization = None
    return capitalization


def getNetAssetValue(fundamental_data, instrument, fundamental_period):
    # return Ratio dataframe
    # Book value is also known as "net book value" and, in the U.K., "net asset value."
    # TODO check
    return getBookValue(fundamental_data, instrument, fundamental_period)
    # if fundamental_period == FundamentalPeriod.yearly:
    #     ratio = Ratio.fundamental_assets_Y
    # else:
    #     ratio = Ratio.fundamental_assets_Q
    # assets = fundamental_data.download(instrument, ratio)
    # 
    # return assets


def getDebt(fundamental_data, instrument, fundamental_period):
    # ratio = Ratio.fundamental_debt_Y
    # output=fundamental_data.download(instrument, ratio)
    output = None
    if output is None:
        ratio_1 = Ratio.fundamental_long_term_debt_Y
        ratio_2 = Ratio.fundamental_short_term_debt_Y
        output_1 = fundamental_data.download(instrument, ratio_1)
        output_2 = fundamental_data.download(instrument, ratio_2)
        if output_1 is not None and output_2 is not None:
            return output_1 + output_2


def getEnterpriseValue(fundamental_data, instrument, fundamental_period):
    # return Ratio dataframe

    # EV = market value of common stock +
    #  market value of preferred equity +
    #  market value of debt +
    # minority interest -
    # cash and investments
    # Enterprise Value (Market Cap + Longterm Debt (excluding Mortgages & Conv Debt) + Current Portion of LT Debt + Preferred Stock (Cash + Marketable Securities)
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_market_capitalization_Y
        else:
            ratio = Ratio.fundamental_market_capitalization_Q
        capitalization = fundamental_data.download(instrument, ratio)
        if capitalization is None:
            logger.error('Cant calculate capitalization! %s ' % instrument.symbol)
            return None
        debt = getDebt(fundamental_data, instrument, FundamentalPeriod.yearly)
        if debt is None or len(debt) == 0:
            logger.error('error downloading %s for %s => return ev none' % (ratio, instrument.symbol))
            return None

        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_cash_and_equivalents_Y
        else:
            ratio = Ratio.fundamental_cash_and_equivalents_Q

        cash = fundamental_data.download(instrument, ratio)
        if cash is None or len(cash) == 0:
            logger.error('error downloading %s for %s => return ev none' % (ratio, instrument.symbol))
            return None

        ev = capitalization + debt - cash
    except Exception as e:
        logger.error('Error getting ev for %s %s => return None' % (instrument.symbol, str(e)))
    return ev


def getOperatingAssets(fundamental_data, instrument, fundamental_period):
    # return Ratio dataframe

    # total assets - cash

    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_assets_Y
        else:
            ratio = Ratio.fundamental_assets_Q
        assets = fundamental_data.download(instrument, ratio)
        if assets is None or len(assets) == 0:
            logger.error('error downloading %s for %s => return operating assets none' % (ratio, instrument.symbol))
            return None
        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_cash_and_equivalents_Y
        else:
            ratio = Ratio.fundamental_cash_and_equivalents_Q

        cash = fundamental_data.download(instrument, ratio)
        if cash is None or len(cash) == 0:
            logger.error('error downloading %s for %s => return operating assets none' % (ratio, instrument.symbol))
            return None
    except Exception as e:
        logger.error('Error getting op assets for %s %s => return None' % (instrument.symbol, str(e)))
        return None

    return assets - cash


def getOperatingLiabilities(fundamental_data, instrument, fundamental_period):
    # total liabilities - cash
    output = None
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_liabilities_Y
        else:
            ratio = Ratio.fundamental_liabilities_Q
        liabilities = fundamental_data.download(instrument, ratio)
        if liabilities is None or len(liabilities) == 0:
            logger.error('error downloading %s for %s => return liabilities none' % (ratio, instrument.symbol))
            return None

        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_cash_and_equivalents_Y
        else:
            ratio = Ratio.fundamental_cash_and_equivalents_Q

        cash = fundamental_data.download(instrument, ratio)
        if cash is None or len(cash) == 0:
            logger.error('error downloading %s for %s => return op liabilities none' % (ratio, instrument.symbol))
            return None
    except Exception as e:
        logger.error('Error getting op liabilities for %s %s => return None' % (instrument.symbol, str(e)))
        return None
    return liabilities - cash


def getDaySalesReceivables(fundamental_data, instrument, fundamental_period):
    import pandas as pd
    import numpy as np
    # recTurnover = sales/ receivabeles
    # daySalesRec = 365/ RecvTrun
    output = None
    try:

        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_revenue_Y
        else:
            ratio = Ratio.fundamental_revenue_Q
        sales = fundamental_data.download(instrument, ratio)
        if sales is None or len(sales) == 0:
            logger.error(
                'error downloading %s for %s => return day sales receivables none' % (ratio, instrument.symbol))
            return None

        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_receivables_Y
        else:
            ratio = Ratio.fundamental_receivables_Q
        receivables = fundamental_data.download(instrument, ratio)
        if receivables is None or len(receivables) == 0:
            logger.error(
                'error downloading %s for %s => return day sales receivables none' % (ratio, instrument.symbol))
            return None
        dataframeTocombine = pd.DataFrame({'sales': sales[Ratio.ratio],
                                           'receivables': receivables[Ratio.ratio]})

        receivables_turnover = np.divide(dataframeTocombine['sales'], dataframeTocombine['receivables'])
        daySalesReceivables = np.divide(365, receivables_turnover)
        output = receivables.copy()
        output[Ratio.ratio] = daySalesReceivables

    except Exception as e:
        logger.error('Error getting day sales receivables for %s %s => return None' % (instrument.symbol, str(e)))
        return None
    return output


def getSalesGrowth(fundamental_data, instrument, fundamental_period):
    # return Ratio dataframe
    import numpy as np
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_revenue_Y
        else:
            ratio = Ratio.fundamental_revenue_Q
        sales = fundamental_data.download(instrument, ratio)

        if sales is None or len(sales) == 0:
            logger.error('error downloading %s for %s => return sales growth none' % (ratio, instrument.symbol))
            return None

        currentSales = sales[Ratio.ratio]
        salesGrowth = sales.copy()
        salesGrowth[Ratio.ratio] = np.divide(currentSales - currentSales.shift(periodsToShift),
                                             currentSales.shift(periodsToShift)) * 100
    except Exception as e:
        logger.error('Error getting sales growth for %s %s => return None' % (instrument.symbol, str(e)))
        return None
    return salesGrowth


def getBookValuePerShare(fundamental_data, instrument, fundamental_period):
    try:
        bv = getBookValue(fundamental_data, instrument, fundamental_period)

        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_shares_Y
        else:
            ratio = Ratio.fundamental_shares_Q
        shares = fundamental_data.download(instrument, ratio)

        output = bv / shares
        return output
    except Exception as e:
        logger.error('Error getting book value per share for %s %s => return None' % (instrument.symbol, str(e)))
        return None


def getBookValueLiabilities(fundamental_data, instrument, fundamental_period):
    # TODO review if something better cand do
    try:

        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_liabilities_Y
        else:
            ratio = Ratio.fundamental_liabilities_Q
        liabilities = fundamental_data.download(instrument, ratio)
    except Exception as e:
        logger.error('Error getting bv liabilities for %s %s => return None' % (instrument.symbol, str(e)))
        return None
    return liabilities


def getBookValue(fundamental_data, instrument, fundamental_period):
    try:

        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_assets_Y
        else:
            ratio = Ratio.fundamental_assets_Q

        assets = fundamental_data.download(instrument, ratio)
        if assets is None or len(assets) == 0:
            logger.error('error downloading %s for %s => return book value none' % (ratio, instrument.symbol))
            return None

        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_liabilities_Y
        else:
            ratio = Ratio.fundamental_liabilities_Q

        liabilities = fundamental_data.download(instrument, ratio)
        if liabilities is None or len(liabilities) == 0:
            logger.error('error downloading %s for %s => return book value none' % (ratio, instrument.symbol))
            return None
        bookValue = assets.copy()
        bookValue[Ratio.ratio] = assets[Ratio.ratio] - liabilities[Ratio.ratio]
    except Exception as e:
        logger.error('Error getting book value for %s %s => return None' % (instrument.symbol, str(e)))
        return None
    return bookValue


def getAdjustedBookValue(fundamental_data, instrument, fundamental_period):
    # # close/bv
    from tradeasystems_connector.service.historical_market_data_service import HistoricalMarketDataService

    import datetime
    try:

        bookValue = getBookValue(fundamental_data, instrument, fundamental_period)

        marketDataServices = HistoricalMarketDataService(fundamental_data.user_settings)
        fromDate = bookValue.index[0].replace(tzinfo=None) - datetime.timedelta(days=3)
        toDate = bookValue.index[-1].replace(tzinfo=None) + datetime.timedelta(days=3)

        barPrices = marketDataServices.getHistoricalData(instrument, period=Period.day, number_of_periods=1,
                                                         force_download=True,
                                                         fromDate=fromDate,
                                                         toDate=toDate
                                                         )

        adjustedBookValue = bookValue.copy()
        closesIndexBar = barPrices.index.searchsorted(adjustedBookValue.index) - 1
        closesIndexBar = closesIndexBar[closesIndexBar < barPrices.shape[0]]

        bookValue[Bar.close] = barPrices[Bar.close][closesIndexBar].values
        adjustedBookValue[Ratio.ratio] = bookValue[Bar.close] / bookValue[Ratio.ratio]

    except Exception as e:
        logger.error('Error getting adj book value for %s %s => return None' % (instrument.symbol, str(e)))
        return None
    return adjustedBookValue


def getReturnOverAssets(fundamental_data, instrument, fundamental_period, fromDate=None, toDate=None):
    # revenue / assets
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_revenue_Y
        else:
            ratio = Ratio.fundamental_revenue_Q

        revenue = fundamental_data.download(instrument, ratio, fromDate, toDate)
        if revenue is None or len(revenue) == 0:
            logger.error('error downloading %s for %s => return roa none' % (ratio, instrument.symbol))
            return None

        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_assets_Y
        else:
            ratio = Ratio.fundamental_assets_Q

        assets = fundamental_data.download(instrument, ratio, fromDate, toDate)
        if assets is None or len(assets) == 0:
            logger.error('error downloading %s for %s => return roa none' % (ratio, instrument.symbol))
            return None

        roa = revenue.copy()
        roa[Ratio.ratio] = revenue[Ratio.ratio] / assets[Ratio.ratio]
    except Exception as e:
        logger.error('Error getting roa for %s %s => return None' % (instrument.symbol, str(e)))
        return None

    return roa


def getReturnOverCapital(fundamental_data, instrument, fundamental_period, fromDate=None, toDate=None):
    # return/capital
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_revenue_Y
        else:
            ratio = Ratio.fundamental_revenue_Q

        revenue = fundamental_data.download(instrument, ratio, fromDate, toDate)
        if revenue is None or len(revenue) == 0:
            logger.error('error downloading %s for %s => return roc none' % (ratio, instrument.symbol))
            return None

        capital = getCapitalization(fundamental_data, instrument, fundamental_period, fromDate, toDate)
        if capital is None or len(capital) == 0:
            logger.error('error downloading %s for %s => return roc none' % (ratio, instrument.symbol))
            return None

        roc = revenue.copy()
        roc[Ratio.ratio] = revenue[Ratio.ratio] / capital[Ratio.ratio]
    except Exception as e:
        logger.error('Error getting roc for %s %s => return None' % (instrument.symbol, str(e)))
        return None

    return roc


def getLongTermDebt(fundamental_data, instrument, fundamental_period):
    # TODO find something

    if fundamental_period == FundamentalPeriod.yearly:
        ratio = Ratio.fundamental_debt_Y
    else:
        ratio = Ratio.fundamental_debt_Q

    debt = fundamental_data.download(instrument, ratio)

    return debt


def getShortTermDebt(fundamental_data, instrument, fundamental_period):
    # TODO find something
    return getLongTermDebt(fundamental_data, instrument, fundamental_period)


##todo implement all!
def getAccrual(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_ebit_Y
            ratio_2 = Ratio.fundamental_assets_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_ebit_Q
            ratio_2 = Ratio.fundamental_assets_Q
            periodsToShift = 4
        # ebit = self.getEBIT(symbol)
        ebit = fundamental_data.download(instrument, ratio_1)
        assets = fundamental_data.download(instrument, ratio_2)
        assetsBefore = assets.shift(1)
        accrual = ebit / assets

        return accrual

    except Exception as e:
        logger.error('Error getting Accrual for %s %s => return None' % (instrument.symbol, str(e)))
        return None


def getDaySalesReceivablesIndex(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_day_sales_receivables_Y
            periodsToShift = 1

        else:
            ratio_1 = Ratio.fundamental_day_sales_receivables_Q
            periodsToShift = 4

        # ebit = self.getEBIT(symbol)
        fromDate = fromDate - datetime.timedelta(days=2 * 365)
        dsr = fundamental_data.download(instrument, ratio_1, fromDate=fromDate)

        dsri = dsr - dsr.shift(periodsToShift)

        return dsri

    except Exception as e:
        logger.error('Error getting day sales receivables index for %s %s => return None' % (instrument.symbol, str(e)))
        return None


def getGrossMargin(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_gross_profit_Y
            ratio_2 = Ratio.fundamental_revenue_Y

        else:
            ratio_1 = Ratio.fundamental_gross_profit_Q
            ratio_2 = Ratio.fundamental_revenue_Q

        # gm = gp/revenue
        gp = fundamental_data.download(instrument, ratio_1, fromDate=fromDate)
        revenue = fundamental_data.download(instrument, ratio_2, fromDate=fromDate)
        gm = gp / revenue

        return gm

    except Exception as e:
        logger.error('Error getting gross margin for %s %s => return None' % (instrument.symbol, str(e)))
        return None


def getGrossMarginIndex(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    # gross margin index
    # ratio of gross margin from t-1 to t
    fromDate = fromDate - datetime.timedelta(days=2 * 365)
    gm = getGrossMargin(fundamental_data, instrument, fundamental_period, fromDate, toDate)
    gmi = gm.diff()
    return gmi


def getAssetQualityIndex(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_non_current_assets_Y
            ratio_2 = Ratio.fundamental_assets_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_non_current_assets_Q
            ratio_2 = Ratio.fundamental_assets_Q
            periodsToShift = 4

        fromDate = fromDate - datetime.timedelta(days=2 * 365)
        non_current_assets = fundamental_data.download(instrument, ratio_1, fromDate=fromDate)
        assets = fundamental_data.download(instrument, ratio_2, fromDate=fromDate)
        aqi_t = non_current_assets / assets  # measure attempts cost deferral as intangible assets

        aqi_t_1 = non_current_assets.shift(periodsToShift) / assets.shift(periodsToShift)
        return aqi_t / aqi_t_1


    except Exception as e:
        logger.error('Error getting gross margin for %s %s => return None' % (instrument.symbol, str(e)))
        return None


def getSalesGrowthIndex(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    # high sgi create expectation form management=> incentive to manipulate
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_sales_growth_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_sales_growth_Q
            periodsToShift = 4

        fromDate = fromDate - datetime.timedelta(days=2 * 365)
        sales_growth = fundamental_data.download(instrument, ratio_1, fromDate=fromDate)
        sgi = sales_growth / sales_growth.shift(periodsToShift)
        return sgi


    except Exception as e:
        logger.error('Error getting sales growth index for %s %s => return None' % (instrument.symbol, str(e)))
        return None


def getDepreciationIndex(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_depreciation_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_depreciation_Q
            periodsToShift = 4

        fromDate = fromDate - datetime.timedelta(days=2 * 365)
        depreciation = fundamental_data.download(instrument, ratio_1, fromDate=fromDate)
        depi = depreciation.shift(periodsToShift) / depreciation
        return depi


    except Exception as e:
        logger.error('Error getting depreciation index for %s %s => return None' % (instrument.symbol, str(e)))
        return None


def getSGAI(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_selling_general_administrative_expenses_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_selling_general_administrative_expenses_Q
            periodsToShift = 4

        fromDate = fromDate - datetime.timedelta(days=2 * 365)
        sga = fundamental_data.download(instrument, ratio_1, fromDate=fromDate)
        sgi = sga / sga.shift(periodsToShift)
        return sgi


    except Exception as e:
        logger.error('Error getting sgai(sales general adminisatrative expense index) for %s %s => return None' % (
            instrument.symbol, str(e)))
        return None


def getLeverage(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_debt_Y
            ratio_2 = Ratio.fundamental_assets_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_debt_Q
            ratio_2 = Ratio.fundamental_assets_Q
            periodsToShift = 4

        debt = fundamental_data.download(instrument, ratio_1, fromDate=fromDate)
        assets = fundamental_data.download(instrument, ratio_2, fromDate=fromDate)
        leverage = debt / assets

        return leverage


    except Exception as e:
        logger.error('Error getting gross margin for %s %s => return None' % (instrument.symbol, str(e)))
        return None


def getWorkingCapital(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_current_assets_Y
            ratio_2 = Ratio.fundamental_current_liabilities_Y
            periodsToShift = 1

        ca = fundamental_data.download(instrument, ratio_1, fromDate=fromDate)
        cl = fundamental_data.download(instrument, ratio_2, fromDate=fromDate)
        wc = ca - cl

        return wc


    except Exception as e:
        logger.error('Error getting working capital for %s %s => return None' % (instrument.symbol, str(e)))
        return None


def getLVGI(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            periodsToShift = 1
        else:
            periodsToShift = 4

        fromDate = fromDate - datetime.timedelta(days=2 * 365)
        leverage = getLeverage(fundamental_data, instrument, fundamental_period, fromDate, toDate)

        lvgi = leverage / leverage.shift(periodsToShift)

        return lvgi


    except Exception as e:
        logger.error('Error getting leverage growth index(lvgi) for %s %s => return None' % (instrument.symbol, str(e)))
        return None


def getROAAvg8(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_return_over_assets_Y
            periodAvg = 8
        else:
            ratio_1 = Ratio.fundamental_return_over_assets_Q
            periodAvg = 8 * 4
        fromDate = fromDate - datetime.timedelta(days=10 * 365)
        roa = getReturnOverAssets(fundamental_data, instrument, fundamental_period, fromDate, toDate)
        roaAvg = roa.rolling(8 * periodsToShift).apply(stats.gmean)

        return roaAvg


    except Exception as e:
        logger.error('Error getting roa avg 8y %s %s => return None' % (instrument.symbol, str(e)))
        return None


def getROCAvg8(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_return_over_assets_Y
            periodAvg = 8
        else:
            ratio_1 = Ratio.fundamental_return_over_assets_Q
            periodAvg = 8 * 4
        fromDate = fromDate - datetime.timedelta(days=10 * 365)
        roc = getReturnOverCapital(fundamental_data, instrument, fundamental_period, fromDate, toDate)
        rocAvg = roc.rolling(8 * periodsToShift).apply(stats.gmean)

        return rocAvg


    except Exception as e:
        logger.error('Error getting roc avg 8y %s %s => return None' % (instrument.symbol, str(e)))
        return None


def getFCFPerShare(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_free_cashflow_Y
            ratio_2 = Ratio.fundamental_shares_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_free_cashflow_Q
            ratio_2 = Ratio.fundamental_shares_Q
            periodsToShift = 4

        fcff = fundamental_data.download(instrument, ratio_1, fromDate=fromDate)
        shares = fundamental_data.download(instrument, ratio_2, fromDate=fromDate)
        output = fcff / shares
        return output

    except Exception as e:
        logger.error('Error getting free cashflow per share for %s %s => return None' % (instrument, str(e)))
        return None


def getFCFA(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    # Sum(eight - yearFCF) / total    assets(t)
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_free_cashflow_Y
            ratio_2 = Ratio.fundamental_assets_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_free_cashflow_Q
            ratio_2 = Ratio.fundamental_assets_Q
            periodsToShift = 4
        fromDate = fromDate - datetime.timedelta(days=10 * 365)
        fcff = fundamental_data.download(instrument, ratio_1, fromDate=fromDate)
        assets = fundamental_data.download(instrument, ratio_2, fromDate=fromDate)

        sum_fcff = fcff.rolling(8 * periodsToShift).sum()
        fcfa = sum_fcff / assets

        return fcfa


    except Exception as e:
        logger.error('Error getting fcfa for %s %s => return None' % (instrument.symbol, str(e)))
        return None


def getMG(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_gross_margin_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_gross_margin_Q

            periodsToShift = 4
        fromDate = fromDate - datetime.timedelta(days=10 * 365)
        gm = fundamental_data.download(instrument, ratio_1, fromDate=fromDate)
        mg = gm.rolling(8 * periodsToShift).apply(stats.gmean)

        return mg


    except Exception as e:
        logger.error('Error getting mg for %s %s => return None' % (instrument.symbol, str(e)))
        return None


def getCurrentRatio(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_current_assets_Y

        else:
            ratio_1 = Ratio.fundamental_current_assets_Q

        if fundamental_period == FundamentalPeriod.yearly:
            ratio_2 = Ratio.fundamental_current_liabilities_Y
        else:
            ratio_2 = Ratio.fundamental_current_liabilities_Q

        fromDate = fromDate - datetime.timedelta(days=2 * 365)
        currentAssetsassets = fundamental_data.download(instrument, ratio_1, fromDate=fromDate)
        currentLiabilities = fundamental_data.download(instrument, ratio_2, fromDate=fromDate)
        currentRatio = currentAssetsassets / currentLiabilities
        return currentRatio

    except Exception as e:
        logger.error('Error getting current Ratio for %s %s => return None' % (instrument.symbol, str(e)))
        return None


def getLiquidity(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_current_ratio_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_current_ratio_Q
            periodsToShift = 4
        fromDate = fromDate - datetime.timedelta(days=2 * 365)
        currentRatio = fundamental_data.download(instrument, ratio_1, fromDate=fromDate)
        liquidity = currentRatio - currentRatio.shift(periodsToShift)
        return liquidity


    except Exception as e:
        logger.error('Error getting liquidity for %s %s => return None' % (instrument.symbol, str(e)))
        return None


def getLeverageDiff(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_leverage_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_leverage_Q
            periodsToShift = 4
        fromDate = fromDate - datetime.timedelta(days=2 * 365)
        leverage = fundamental_data.download(instrument, ratio_1, fromDate=fromDate)
        leverageDiff = leverage - leverage.shift(periodsToShift)
        return leverageDiff


    except Exception as e:
        logger.error('Error getting leverageDiff for %s %s => return None' % (instrument.symbol, str(e)))
        return None


def getROADiff(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_return_over_assets_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_return_over_assets_Q
            periodsToShift = 4
        fromDate = fromDate - datetime.timedelta(days=2 * 365)
        roa = fundamental_data.download(instrument, ratio_1, fromDate=fromDate)
        roaDiff = roa - roa.shift(periodsToShift)
        return roaDiff

    except Exception as e:
        logger.error('Error getting roaDiff for %s %s => return None' % (instrument.symbol, str(e)))
        return None


def getFCFTADiff(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_free_cashflow_Y
            ratio_2 = Ratio.fundamental_assets_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_free_cashflow_Q
            ratio_2 = Ratio.fundamental_assets_Q
            periodsToShift = 4
        fromDate = fromDate - datetime.timedelta(days=2 * 365)
        fcff = fundamental_data.download(instrument, ratio_1, fromDate=fromDate)
        assets = fundamental_data.download(instrument, ratio_2, fromDate=fromDate)
        fcfta = fcff / assets
        fcftaDiff = fcfta - fcfta.shift(periodsToShift)
        return fcftaDiff

    except Exception as e:
        logger.error('Error getting fcftaDiff for %s %s => return None' % (instrument.symbol, str(e)))
        return None


def getMarginDiff(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_gross_margin_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_gross_margin_Q
            periodsToShift = 4
        fromDate = fromDate - datetime.timedelta(days=2 * 365)
        gm = fundamental_data.download(instrument, ratio_1, fromDate=fromDate)
        marginDiff = gm - gm.shift(periodsToShift)
        return marginDiff

    except Exception as e:
        logger.error('Error getting marginDiff for %s %s => return None' % (instrument.symbol, str(e)))
        return None


def getTurnDiff(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_revenue_Y
            ratio_2 = Ratio.fundamental_assets_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_revenue_Q
            ratio_2 = Ratio.fundamental_assets_Q
            periodsToShift = 4
        fromDate = fromDate - datetime.timedelta(days=2 * 365)
        sales = fundamental_data.download(instrument, ratio_1, fromDate=fromDate)
        assets = fundamental_data.download(instrument, ratio_2, fromDate=fromDate)
        turnover = sales / assets
        turnoverDiff = turnover - turnover.shift(periodsToShift)
        return turnoverDiff

    except Exception as e:
        logger.error('Error getting turnoverDiff for %s %s => return None' % (instrument.symbol, str(e)))
        return None


def getMS(fundamental_data, instrument, fundamental_period, fromDate, toDate):
    # ms = 8yrAvgGrossMargin /  8yrSTDGrossMarginSTD
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_gross_margin_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_gross_margin_Q
            periodsToShift = 4

        fromDate = fromDate - datetime.timedelta(days=10 * 365)
        gm = fundamental_data.download(instrument, ratio_1, fromDate=fromDate)
        numerator = gm.rolling(8 * periodsToShift).apply(stats.gmean)
        denominator = gm.rolling(8 * periodsToShift).std()
        ms = numerator / denominator
        return ms


    except Exception as e:
        logger.error('Error getting ms for %s %s => return None' % (instrument.symbol, str(e)))
        return None
