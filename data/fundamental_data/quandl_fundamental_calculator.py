import datetime

import pandas as pd
from scipy.stats import stats

from tradeasystems_connector.conf.log_settings import logger
from tradeasystems_connector.model.bar import Bar
from tradeasystems_connector.model.fundamental_period import FundamentalPeriod
from tradeasystems_connector.model.period import Period
from tradeasystems_connector.model.ratio import Ratio

periodsToShift = 1


def downloadRatio(fundamental_data, instrument, ratio, fromDate=None, toDate=None, df=None):
    if df is not None and ratio in list(df.columns):
        output = df[ratio]
    else:
        output = fundamental_data.download(instrument, ratio, fromDate, toDate, df)
        if output is None:
            logger.error('Cant download ratio %s for %s' % (ratio, instrument))

        else:

            if isinstance(output, pd.Series):
                if df is None:
                    df = pd.DataFrame(output.values, index=output.index, columns=[ratio])
                else:
                    df[ratio] = output.values
            if (isinstance(output, pd.DataFrame)):
                if df is None:
                    df = pd.DataFrame(output[Ratio.ratio].values, index=output.index, columns=[ratio])
                else:
                    df[ratio] = output[Ratio.ratio].values
    if isinstance(output, pd.DataFrame):
        output = output[Ratio.ratio]

    return output


def getCapitalization(fundamental_data, instrument, fundamental_period, fromDate=None, toDate=None, df=None):
    import datetime

    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_shares_Y
        else:
            ratio = Ratio.fundamental_shares_Q

        shares = downloadRatio(fundamental_data=fundamental_data, instrument=instrument, ratio=ratio, fromDate=fromDate,
                               toDate=toDate, df=df)

        if shares is None or len(shares) == 0:
            logger.error('error downloading %s for %s => return capitalization none' % (ratio, instrument.symbol))
            return None

        if fromDate is None:
            fromDate = shares.index[0].replace(tzinfo=None) - datetime.timedelta(days=3)
        else:
            fromDate = fromDate - datetime.timedelta(days=3)

        if toDate is None:
            toDate = shares.index[-1].replace(tzinfo=None) + datetime.timedelta(days=3)
        else:
            toDate = toDate + datetime.timedelta(days=3)

        if df is not None and Ratio.closeTemp in list(df.columns):
            closeSeries = df[Ratio.closeTemp].values
        else:

            closeSeries = (getClose(fundamental_data, instrument, fromDate, toDate, seriesReference=shares)).values
            if df is not None:
                df[Ratio.closeTemp] = closeSeries

        if isinstance(shares, pd.DataFrame):
            shares = shares[Ratio.ratio]
        capitalization = closeSeries * shares
    except Exception as e:
        logger.error('Error getting capitalization for %s %s => return None' % (instrument.symbol, str(e)))
        capitalization = None
    return capitalization


def getNetAssetValue(fundamental_data, instrument, fundamental_period, fromDate=None, toDate=None, df=None):
    # return Ratio dataframe
    # Book value is also known as "net book value" and, in the U.K., "net asset value."
    # TODO check

    return getBookValue(fundamental_data, instrument, fundamental_period, fromDate, toDate, df)


def getEnterpriseValue(fundamental_data, instrument, fundamental_period, fromDate=None, toDate=None, df=None):
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

        capitalization = downloadRatio(fundamental_data=fundamental_data, instrument=instrument, ratio=ratio,
                                       fromDate=fromDate, toDate=toDate, df=df)

        if capitalization is None:
            logger.error('Cant calculate capitalization! %s ' % instrument.symbol)
            return None
        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_debt_Y
        else:
            ratio = Ratio.fundamental_debt_Q

        debt = downloadRatio(fundamental_data=fundamental_data, instrument=instrument, ratio=ratio,
                             fromDate=fromDate, toDate=toDate, df=df)
        if debt is None or len(debt) == 0:
            logger.error('error downloading %s for %s => return ev none' % (ratio, instrument.symbol))
            return None

        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_cash_and_equivalents_Y
        else:
            ratio = Ratio.fundamental_cash_and_equivalents_Q

        cash = downloadRatio(fundamental_data=fundamental_data, instrument=instrument, ratio=ratio,
                             fromDate=fromDate, toDate=toDate, df=df)

        if cash is None or len(cash) == 0:
            logger.error('error downloading %s for %s => return ev none' % (ratio, instrument.symbol))
            return None

        ev = capitalization + debt - cash
    except Exception as e:
        logger.error('Error getting ev for %s %s => return None' % (instrument.symbol, str(e)))
        return None
    return ev


def getOperatingAssets(fundamental_data, instrument, fundamental_period, fromDate=None, toDate=None, df=None):
    # return Ratio dataframe

    # total assets - cash
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_assets_Y
        else:
            ratio = Ratio.fundamental_assets_Q
        assets = downloadRatio(fundamental_data=fundamental_data, instrument=instrument, ratio=ratio, fromDate=fromDate,
                               toDate=toDate, df=df)
        if assets is None or len(assets) == 0:
            logger.error('error downloading %s for %s => return operating assets none' % (ratio, instrument.symbol))
            return None
        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_cash_and_equivalents_Y
        else:
            ratio = Ratio.fundamental_cash_and_equivalents_Q

        cash = downloadRatio(fundamental_data=fundamental_data, instrument=instrument, ratio=ratio, fromDate=fromDate,
                             toDate=toDate, df=df)
        if cash is None or len(cash) == 0:
            logger.error('error downloading %s for %s => return operating assets none' % (ratio, instrument.symbol))
            return None
    except Exception as e:
        logger.error('Error getting op assets for %s %s => return None' % (instrument.symbol, str(e)))
        return None
    return assets - cash


def getOperatingLiabilities(fundamental_data, instrument, fundamental_period, fromDate=None, toDate=None, df=None):
    # total liabilities - cash
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_liabilities_Y
        else:
            ratio = Ratio.fundamental_liabilities_Q
        liabilities = downloadRatio(fundamental_data=fundamental_data, instrument=instrument, ratio=ratio,
                                    fromDate=fromDate, toDate=toDate, df=df)
        if liabilities is None or len(liabilities) == 0:
            logger.error('error downloading %s for %s => return liabilities none' % (ratio, instrument.symbol))
            return None

        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_cash_and_equivalents_Y
        else:
            ratio = Ratio.fundamental_cash_and_equivalents_Q

        cash = downloadRatio(fundamental_data=fundamental_data, instrument=instrument, ratio=ratio, fromDate=fromDate,
                             toDate=toDate, df=df)
        if cash is None or len(cash) == 0:
            logger.error('error downloading %s for %s => return op liabilities none' % (ratio, instrument.symbol))
            return None
    except Exception as e:
        logger.error('Error getting op liabilities for %s %s => return None' % (instrument.symbol, str(e)))
        return None
    return liabilities - cash


def getDaySalesReceivables(fundamental_data, instrument, fundamental_period, fromDate=None, toDate=None, df=None):
    import pandas as pd
    import numpy as np
    # recTurnover = sales/ receivabeles
    # daySalesRec = 365/ RecvTrun
    try:

        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_revenue_Y
        else:
            ratio = Ratio.fundamental_revenue_Q
        sales = downloadRatio(fundamental_data=fundamental_data, instrument=instrument, ratio=ratio, fromDate=fromDate,
                              toDate=toDate, df=df)
        if sales is None or len(sales) == 0:
            logger.error(
                'error downloading %s for %s => return day sales receivables none' % (ratio, instrument.symbol))
            return None

        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_receivables_Y
        else:
            ratio = Ratio.fundamental_receivables_Q
        receivables = downloadRatio(fundamental_data=fundamental_data, instrument=instrument, ratio=ratio,
                                    fromDate=fromDate, toDate=toDate, df=df)
        if receivables is None or len(receivables) == 0:
            logger.error(
                'error downloading %s for %s => return day sales receivables none' % (ratio, instrument.symbol))
            return None
        dataframeTocombine = pd.DataFrame({'sales': sales,
                                           'receivables': receivables})

        receivables_turnover = np.divide(dataframeTocombine['sales'], dataframeTocombine['receivables'])
        daySalesReceivables = np.divide(365, receivables_turnover)
        output = receivables.copy()
        output = daySalesReceivables

    except Exception as e:
        logger.error('Error getting day sales receivables for %s %s => return None' % (instrument.symbol, str(e)))
        return None
    return output


def getSalesGrowth(fundamental_data, instrument, fundamental_period, fromDate=None, toDate=None, df=None):
    # return Ratio dataframe
    import numpy as np
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_revenue_Y
        else:
            ratio = Ratio.fundamental_revenue_Q
        sales = downloadRatio(fundamental_data=fundamental_data, instrument=instrument, ratio=ratio, fromDate=fromDate,
                              toDate=toDate, df=df)

        if sales is None or len(sales) == 0:
            logger.error('error downloading %s for %s => return sales growth none' % (ratio, instrument.symbol))
            return None

        currentSales = sales
        salesGrowth = sales.copy()
        salesGrowth = np.divide(currentSales - currentSales.shift(periodsToShift),
                                currentSales.shift(periodsToShift)) * 100
    except Exception as e:
        logger.error('Error getting sales growth for %s %s => return None' % (instrument.symbol, str(e)))
        return None
    return salesGrowth


def getBookValueLiabilities(fundamental_data, instrument, fundamental_period, fromDate=None, toDate=None, df=None):
    # TODO review if something better cand do
    try:

        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_liabilities_Y
        else:
            ratio = Ratio.fundamental_liabilities_Q
            # (fundamental_data, instrument, ratio, fromDate=None, toDate=None, df=None)
        liabilities = downloadRatio(fundamental_data=fundamental_data, instrument=instrument, ratio=ratio,
                                    fromDate=fromDate, toDate=toDate, df=df)
    except Exception as e:
        logger.error('Error getting bv liabilities for %s %s => return None' % (instrument.symbol, str(e)))
        return None
    return liabilities


def getBookValue(fundamental_data, instrument, fundamental_period, fromDate=None, toDate=None, df=None):
    try:

        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_book_value_per_share_Y
        else:
            ratio = Ratio.fundamental_book_value_per_share_Q

        bvPerShare = downloadRatio(fundamental_data=fundamental_data, instrument=instrument, ratio=ratio,
                                   fromDate=fromDate, toDate=toDate, df=df)
        if bvPerShare is None or len(bvPerShare) == 0:
            logger.error('error downloading %s for %s => return adj book value none' % (ratio, instrument.symbol))
            return None

        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_shares_Y
        else:
            ratio = Ratio.fundamental_shares_Q

        shares = downloadRatio(fundamental_data=fundamental_data, instrument=instrument, ratio=ratio, fromDate=fromDate,
                               toDate=toDate, df=df)
        if shares is None or len(shares) == 0:
            logger.error('error downloading %s for %s => return book value none' % (ratio, instrument.symbol))
            return None
        bookValue = bvPerShare.copy()
        bookValue = shares * bvPerShare
    except Exception as e:
        logger.error('Error getting book value for %s %s => return None' % (instrument.symbol, str(e)))
        return None
    return bookValue


def getBookValue2(fundamental_data, instrument, fundamental_period, fromDate=None, toDate=None, df=None):
    try:

        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_assets_Y
        else:
            ratio = Ratio.fundamental_assets_Q

        assets = downloadRatio(fundamental_data=fundamental_data, instrument=instrument, ratio=ratio, fromDate=fromDate,
                               toDate=toDate, df=df)
        if assets is None or len(assets) == 0:
            logger.error('error downloading %s for %s => return adj book value none' % (ratio, instrument.symbol))
            return None

        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_liabilities_Y
        else:
            ratio = Ratio.fundamental_liabilities_Q

        liabilities = downloadRatio(fundamental_data=fundamental_data, instrument=instrument, ratio=ratio,
                                    fromDate=fromDate, toDate=toDate, df=df)
        if liabilities is None or len(liabilities) == 0:
            logger.error('error downloading %s for %s => return book value none' % (ratio, instrument.symbol))
            return None
        bookValue = assets.copy()
        bookValue = assets - liabilities
    except Exception as e:
        logger.error('Error getting book value for %s %s => return None' % (instrument.symbol, str(e)))
        return None
    return bookValue


def getAdjustedBookValue(fundamental_data, instrument, fundamental_period, fromDate=None, toDate=None, df=None):
    # # close/bv

    try:

        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_book_value_per_share_Y
        else:
            ratio = Ratio.fundamental_book_value_per_share_Q

        bvPerShare = downloadRatio(fundamental_data=fundamental_data, instrument=instrument, ratio=ratio,
                                   fromDate=fromDate, toDate=toDate, df=df)
        adjustedBookValue = 1 / bvPerShare


    except Exception as e:
        logger.error('Error getting adj book value for %s %s => return None' % (instrument.symbol, str(e)))
        return None
    return adjustedBookValue


def getClose(fundamental_data, instrument, fromDate, toDate, seriesReference):
    from tradeasystems_connector.service.historical_market_data_service import HistoricalMarketDataService

    marketDataServices = HistoricalMarketDataService(fundamental_data.user_settings)

    barPrices = marketDataServices.getHistoricalData(instrument, period=Period.day, number_of_periods=1,
                                                     force_download=True,
                                                     fromDate=fromDate,
                                                     toDate=toDate
                                                     )

    # adjustedBookValue = seriesReference.copy()
    # closesIndexBar = barPrices.index.searchsorted(adjustedBookValue.index) - 1
    # closesIndexBar = closesIndexBar[closesIndexBar < barPrices.shape[0]]
    # closeSeries = barPrices[Bar.close][closesIndexBar].values

    from tradeasystems_connector.util.date_util import convertSerieClosestTimeIndex
    closeSeries = convertSerieClosestTimeIndex(serie=barPrices[Bar.close], datetimeIndex=seriesReference.index, shift=0)
    return closeSeries


def getReturnOverAssets(fundamental_data, instrument, fundamental_period, fromDate=None, toDate=None, df=None):
    # revenue / assets
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_revenue_Y
        else:
            ratio = Ratio.fundamental_revenue_Q

        revenue = downloadRatio(fundamental_data, instrument, ratio, fromDate, toDate, df=df)
        if revenue is None or len(revenue) == 0:
            logger.error('error downloading %s for %s => return roa none' % (ratio, instrument.symbol))
            return None

        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_assets_Y
        else:
            ratio = Ratio.fundamental_assets_Q

        assets = downloadRatio(fundamental_data, instrument, ratio, fromDate, toDate, df=df)
        if assets is None or len(assets) == 0:
            logger.error('error downloading %s for %s => return roa none' % (ratio, instrument.symbol))
            return None

        roa = revenue.copy()
        roa = revenue / assets
    except Exception as e:
        logger.error('Error getting roa for %s %s => return None' % (instrument.symbol, str(e)))
        return None
    return roa


def getBookMarket(fundamental_data, instrument, fundamental_period, fromDate=None, toDate=None, df=None):
    # revenue / assets
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_book_value_per_share_Y
        else:
            ratio = Ratio.fundamental_book_value_per_share_Q

        bvps = downloadRatio(fundamental_data, instrument, ratio, fromDate, toDate, df=df)
        if bvps is None or len(bvps) == 0:
            logger.error('error downloading %s for %s => return bm none' % (ratio, instrument.symbol))
            return None
        bm = bvps.copy()
        # BM=1./ARQ_PB; TODO review price book ratio
        bm = 1 / bm
        return bm
    except Exception as e:
        logger.error('Error getting bm for %s %s => return None' % (instrument.symbol, str(e)))
        return None


def getReturnOverEquity(fundamental_data, instrument, fundamental_period, fromDate=None, toDate=None, df=None):
    # revenue / assets
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_earnings_per_share_Y
        else:
            ratio = Ratio.fundamental_earnings_per_share_Q

        eps = downloadRatio(fundamental_data, instrument, ratio, fromDate, toDate, df=df)
        if eps is None or len(eps) == 0:
            logger.error('error downloading %s for %s => return roe none' % (ratio, instrument.symbol))
            return None

        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_book_value_per_share_Y
        else:
            ratio = Ratio.fundamental_book_value_per_share_Q

        bvps = downloadRatio(fundamental_data, instrument, ratio, fromDate - datetime.timedelta(days=300), toDate,
                             df=df)
        if bvps is None or len(bvps) == 0:
            logger.error('error downloading %s for %s => return roe none' % (ratio, instrument.symbol))
            return None
        lag_bvps = bvps.shift(1)

        roe = eps.copy()
        # ROE=1+earningsInc./bvpershr_lag;
        roe = (1 + eps) / lag_bvps

    except Exception as e:
        logger.error('Error getting roe for %s %s => return None' % (instrument.symbol, str(e)))
        return None
    return roe


def getReturnOverCapital(fundamental_data, instrument, fundamental_period, fromDate=None, toDate=None, df=None):
    # return/capital
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_revenue_Y
        else:
            ratio = Ratio.fundamental_revenue_Q

        revenue = downloadRatio(fundamental_data, instrument, ratio, fromDate, toDate, df=df)
        if revenue is None or len(revenue) == 0:
            logger.error('error downloading %s for %s => return roc none' % (ratio, instrument.symbol))
            return None
        if fundamental_period == FundamentalPeriod.yearly:
            ratio = Ratio.fundamental_market_capitalization_Y
        else:
            ratio = Ratio.fundamental_market_capitalization_Q

        capital = downloadRatio(fundamental_data, instrument, ratio, fromDate, toDate, df=df)
        if capital is None or len(capital) == 0:
            logger.error('error downloading %s for %s => return roc none' % (ratio, instrument.symbol))
            return None

        roc = revenue.copy()
        roc = revenue / capital
    except Exception as e:
        logger.error('Error getting roc for %s %s => return None' % (instrument.symbol, str(e)))

    return roc


def getLongTermDebt(fundamental_data, instrument, fundamental_period, fromDate=None, toDate=None, df=None):
    # TODO find something

    if fundamental_period == FundamentalPeriod.yearly:
        ratio = Ratio.fundamental_debt_Y
    else:
        ratio = Ratio.fundamental_debt_Q

    debt = downloadRatio(fundamental_data=fundamental_data, instrument=instrument, ratio=ratio, fromDate=fromDate,
                         toDate=toDate, df=df)

    return debt


def getShortTermDebt(fundamental_data, instrument, fundamental_period, fromDate=None, toDate=None, df=None):
    # TODO find something
    return getLongTermDebt(fundamental_data, instrument, fundamental_period, fromDate, toDate, df)


##todo implement all!
def getAccrual(fundamental_data, instrument, fundamental_period, fromDate, toDate, df=None):
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
        ebit = downloadRatio(fundamental_data, instrument, ratio_1, fromDate=fromDate, toDate=toDate, df=df)
        assets = downloadRatio(fundamental_data, instrument, ratio_2, fromDate=fromDate, toDate=toDate, df=df)
        assetsBefore = assets.shift(1)
        accrual = ebit / assets

        return accrual

    except Exception as e:
        logger.error('Error getting Accrual for %s %s => return None' % (instrument.symbol, str(e)))


def getDaySalesReceivablesIndex(fundamental_data, instrument, fundamental_period, fromDate, toDate, df=None):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_day_sales_receivables_Y
            periodsToShift = 1

        else:
            ratio_1 = Ratio.fundamental_day_sales_receivables_Q
            periodsToShift = 4

        # ebit = self.getEBIT(symbol)
        fromDate = fromDate - datetime.timedelta(days=2 * 365)
        dsr = downloadRatio(fundamental_data, instrument, ratio_1, fromDate=fromDate, toDate=toDate, df=df)

        dsri = dsr - dsr.shift(periodsToShift)

        return dsri

    except Exception as e:
        logger.error('Error getting day sales receivables index for %s %s => return None' % (instrument.symbol, str(e)))


def getGrossMargin(fundamental_data, instrument, fundamental_period, fromDate, toDate, df=None):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_gross_profit_Y
            ratio_2 = Ratio.fundamental_revenue_Y

        else:
            ratio_1 = Ratio.fundamental_gross_profit_Q
            ratio_2 = Ratio.fundamental_revenue_Q

        # gm = gp/revenue
        gp = downloadRatio(fundamental_data, instrument, ratio_1, fromDate=fromDate, toDate=toDate, df=df)
        revenue = downloadRatio(fundamental_data, instrument, ratio_2, fromDate=fromDate, toDate=toDate, df=df)
        gm = gp / revenue

        return gm

    except Exception as e:
        logger.error('Error getting gross margin for %s %s => return None' % (instrument.symbol, str(e)))


def getGrossMarginIndex(fundamental_data, instrument, fundamental_period, fromDate, toDate, df=None):
    # gross margin index
    # ratio of gross margin from t-1 to t
    fromDate = fromDate - datetime.timedelta(days=2 * 365)
    gm = getGrossMargin(fundamental_data, instrument, fundamental_period, fromDate, toDate, df)
    gmi = gm.diff()
    return gmi


def getAssetQualityIndex(fundamental_data, instrument, fundamental_period, fromDate, toDate, df=None):
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
        non_current_assets = downloadRatio(fundamental_data, instrument, ratio_1, fromDate=fromDate, toDate=toDate,
                                           df=df)
        assets = downloadRatio(fundamental_data, instrument, ratio_2, fromDate=fromDate, toDate=toDate, df=df)
        aqi_t = non_current_assets / assets  # measure attempts cost deferral as intangible assets

        aqi_t_1 = non_current_assets.shift(periodsToShift) / assets.shift(periodsToShift)
        return aqi_t / aqi_t_1


    except Exception as e:
        logger.error('Error getting gross margin for %s %s => return None' % (instrument.symbol, str(e)))


def getSalesGrowthIndex(fundamental_data, instrument, fundamental_period, fromDate, toDate, df=None):
    # high sgi create expectation form management=> incentive to manipulate
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_sales_growth_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_sales_growth_Q
            periodsToShift = 4

        fromDate = fromDate - datetime.timedelta(days=2 * 365)
        sales_growth = downloadRatio(fundamental_data, instrument, ratio_1, fromDate=fromDate, toDate=toDate, df=df)
        sgi = sales_growth / sales_growth.shift(periodsToShift)
        return sgi


    except Exception as e:
        logger.error('Error getting sales growth index for %s %s => return None' % (instrument.symbol, str(e)))


def getDepreciationIndex(fundamental_data, instrument, fundamental_period, fromDate, toDate, df=None):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_depreciation_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_depreciation_Q
            periodsToShift = 4

        fromDate = fromDate - datetime.timedelta(days=2 * 365)
        depreciation = downloadRatio(fundamental_data, instrument, ratio_1, fromDate=fromDate, toDate=toDate, df=df)
        depi = depreciation.shift(periodsToShift) / depreciation
        return depi


    except Exception as e:
        logger.error('Error getting depreciation index for %s %s => return None' % (instrument.symbol, str(e)))


def getSGAI(fundamental_data, instrument, fundamental_period, fromDate, toDate, df=None):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_selling_general_administrative_expenses_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_selling_general_administrative_expenses_Q
            periodsToShift = 4

        fromDate = fromDate - datetime.timedelta(days=2 * 365)
        sga = downloadRatio(fundamental_data, instrument, ratio_1, fromDate=fromDate, toDate=toDate, df=df)
        sgi = sga / sga.shift(periodsToShift)
        return sgi


    except Exception as e:
        logger.error('Error getting sgai(sales general adminisatrative expense index) for %s %s => return None' % (
            instrument.symbol, str(e)))


def getLeverage(fundamental_data, instrument, fundamental_period, fromDate, toDate, df=None):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_debt_Y
            ratio_2 = Ratio.fundamental_assets_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_debt_Q
            ratio_2 = Ratio.fundamental_assets_Q
            periodsToShift = 4

        debt = downloadRatio(fundamental_data, instrument, ratio_1, fromDate=fromDate, toDate=toDate, df=df)
        assets = downloadRatio(fundamental_data, instrument, ratio_2, fromDate=fromDate, toDate=toDate, df=df)
        leverage = debt / assets

        return leverage


    except Exception as e:
        logger.error('Error getting gross margin for %s %s => return None' % (instrument.symbol, str(e)))


def getLVGI(fundamental_data, instrument, fundamental_period, fromDate, toDate, df=None):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            periodsToShift = 1
        else:
            periodsToShift = 4

        fromDate = fromDate - datetime.timedelta(days=2 * 365)
        leverage = getLeverage(fundamental_data, instrument, fundamental_period, fromDate, toDate, df)

        lvgi = leverage / leverage.shift(periodsToShift)

        return lvgi


    except Exception as e:
        logger.error('Error getting leverage growth index(lvgi) for %s %s => return None' % (instrument.symbol, str(e)))


def getROAAvg8(fundamental_data, instrument, fundamental_period, fromDate, toDate, df=None):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_return_over_assets_Y
            periodAvg = 8
        else:
            ratio_1 = Ratio.fundamental_return_over_assets_Q
            periodAvg = 8 * 4
        fromDate = fromDate - datetime.timedelta(days=10 * 365)
        roa = getReturnOverAssets(fundamental_data, instrument, fundamental_period, fromDate, toDate, df)
        roaAvg = roa.rolling(8 * periodsToShift).apply(stats.gmean)

        return roaAvg


    except Exception as e:
        logger.error('Error getting roa avg 8y %s %s => return None' % (instrument.symbol, str(e)))


def getROCAvg8(fundamental_data, instrument, fundamental_period, fromDate, toDate, df=None):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_return_over_assets_Y
            periodAvg = 8
        else:
            ratio_1 = Ratio.fundamental_return_over_assets_Q
            periodAvg = 8 * 4
        fromDate = fromDate - datetime.timedelta(days=10 * 365)
        roc = getReturnOverCapital(fundamental_data, instrument, fundamental_period, fromDate, toDate, df)
        rocAvg = roc.rolling(8 * periodsToShift).apply(stats.gmean)

        return rocAvg


    except Exception as e:
        logger.error('Error getting roc avg 8y %s %s => return None' % (instrument.symbol, str(e)))


def getFCFA(fundamental_data, instrument, fundamental_period, fromDate, toDate, df=None):
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
        fcff = downloadRatio(fundamental_data, instrument, ratio_1, fromDate=fromDate, toDate=toDate, df=df)
        assets = downloadRatio(fundamental_data, instrument, ratio_2, fromDate=fromDate, toDate=toDate, df=df)

        sum_fcff = fcff.rolling(8 * periodsToShift).sum()
        fcfa = sum_fcff / assets

        return fcfa


    except Exception as e:
        logger.error('Error getting fcfa for %s %s => return None' % (instrument.symbol, str(e)))


def getMG(fundamental_data, instrument, fundamental_period, fromDate, toDate, df=None):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_gross_margin_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_gross_margin_Q

            periodsToShift = 4
        fromDate = fromDate - datetime.timedelta(days=10 * 365)
        gm = downloadRatio(fundamental_data, instrument, ratio_1, fromDate=fromDate, toDate=toDate, df=df)
        mg = gm.rolling(8 * periodsToShift).apply(stats.gmean)

        return mg


    except Exception as e:
        logger.error('Error getting mg for %s %s => return None' % (instrument.symbol, str(e)))


def getLiquidity(fundamental_data, instrument, fundamental_period, fromDate, toDate, df=None):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_current_ratio_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_current_ratio_Q
            periodsToShift = 4
        fromDate = fromDate - datetime.timedelta(days=2 * 365)
        currentRatio = downloadRatio(fundamental_data, instrument, ratio_1, fromDate=fromDate, toDate=toDate, df=df)
        liquidity = currentRatio - currentRatio.shift(periodsToShift)
        return liquidity


    except Exception as e:
        logger.error('Error getting liquidity for %s %s => return None' % (instrument.symbol, str(e)))


def getLeverageDiff(fundamental_data, instrument, fundamental_period, fromDate, toDate, df=None):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_leverage_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_leverage_Q
            periodsToShift = 4
        fromDate = fromDate - datetime.timedelta(days=2 * 365)
        leverage = downloadRatio(fundamental_data, instrument, ratio_1, fromDate=fromDate, toDate=toDate, df=df)
        leverageDiff = leverage - leverage.shift(periodsToShift)
        return leverageDiff


    except Exception as e:
        logger.error('Error getting leverageDiff for %s %s => return None' % (instrument.symbol, str(e)))


def getROADiff(fundamental_data, instrument, fundamental_period, fromDate, toDate, df=None):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_return_over_assets_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_return_over_assets_Q
            periodsToShift = 4
        fromDate = fromDate - datetime.timedelta(days=2 * 365)
        roa = downloadRatio(fundamental_data, instrument, ratio_1, fromDate=fromDate, toDate=toDate, df=df)
        roaDiff = roa - roa.shift(periodsToShift)
        return roaDiff

    except Exception as e:
        logger.error('Error getting roaDiff for %s %s => return None' % (instrument.symbol, str(e)))


def getFCFTADiff(fundamental_data, instrument, fundamental_period, fromDate, toDate, df=None):
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
        fcff = downloadRatio(fundamental_data, instrument, ratio_1, fromDate=fromDate, toDate=toDate, df=df)
        assets = downloadRatio(fundamental_data, instrument, ratio_2, fromDate=fromDate, toDate=toDate, df=df)
        fcfta = fcff / assets
        fcftaDiff = fcfta - fcfta.shift(periodsToShift)
        return fcftaDiff

    except Exception as e:
        logger.error('Error getting fcftaDiff for %s %s => return None' % (instrument.symbol, str(e)))


def getMarginDiff(fundamental_data, instrument, fundamental_period, fromDate, toDate, df=None):
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_gross_margin_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_gross_margin_Q
            periodsToShift = 4
        fromDate = fromDate - datetime.timedelta(days=2 * 365)
        gm = downloadRatio(fundamental_data, instrument, ratio_1, fromDate=fromDate, toDate=toDate, df=df)
        marginDiff = gm - gm.shift(periodsToShift)
        return marginDiff

    except Exception as e:
        logger.error('Error getting marginDiff for %s %s => return None' % (instrument.symbol, str(e)))


def getTurnDiff(fundamental_data, instrument, fundamental_period, fromDate, toDate, df=None):
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
        sales = downloadRatio(fundamental_data, instrument, ratio_1, fromDate=fromDate, toDate=toDate, df=df)
        assets = downloadRatio(fundamental_data, instrument, ratio_2, fromDate=fromDate, toDate=toDate, df=df)
        turnover = sales / assets
        turnoverDiff = turnover - turnover.shift(periodsToShift)
        return turnoverDiff

    except Exception as e:
        logger.error('Error getting turnoverDiff for %s %s => return None' % (instrument.symbol, str(e)))


def getMS(fundamental_data, instrument, fundamental_period, fromDate, toDate, df=None):
    # ms = 8yrAvgGrossMargin /  8yrSTDGrossMarginSTD
    try:
        if fundamental_period == FundamentalPeriod.yearly:
            ratio_1 = Ratio.fundamental_gross_margin_Y
            periodsToShift = 1
        else:
            ratio_1 = Ratio.fundamental_gross_margin_Q
            periodsToShift = 4

        fromDate = fromDate - datetime.timedelta(days=10 * 365)
        gm = downloadRatio(fundamental_data, instrument, ratio_1, fromDate=fromDate, toDate=toDate, df=df)
        numerator = gm.rolling(8 * periodsToShift).apply(stats.gmean)
        denominator = gm.rolling(8 * periodsToShift).std()
        ms = numerator / denominator

        return ms


    except Exception as e:
        logger.error('Error getting ms for %s %s => return None' % (instrument.symbol, str(e)))
