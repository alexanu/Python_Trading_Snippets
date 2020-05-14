# Source: https://github.com/thoriuchi0531/adagio

from collections import namedtuple
from enum import IntEnum, Enum
import os

futures_info = namedtuple("futures_info",
                          ["full_name", "asset_class", "start_from",
                           "denominator", "tick_size", "contract_ccy",
                           "roll_schedule",
                           "first_notice_date", "last_trade_date"])


class FutureContractMonth(IntEnum):
    F = 1
    G = 2
    H = 3
    J = 4
    K = 5
    M = 6
    N = 7
    Q = 8
    U = 9
    V = 10
    X = 11
    Z = 12


class AssetClass(Enum):
    EQUITY_FUT = "equity_futures"
    VOL_INDEX_FUT = "volatility_index_futures"
    GOVT_FUT = "government_bond_futures"
    MM_FUT = "money_market_futures"
    FX_FUT = "fx_futures"
    COMDTY_FUT = "commodity_futures"


class Denominator(Enum):
    GOVT_FUT = 'government_bond_futures'
    MM_FUT = 'money_market_futures'


class FuturesInfo(Enum):
    CME_ES = futures_info("E-mini S&P 500 Index", AssetClass.EQUITY_FUT.value,
                          "Z1997", None, 0.25, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Fri+1Fri+2Fri")
    CME_SP = futures_info("Full-size S&P 500 Index",
                          AssetClass.EQUITY_FUT.value,
                          "M1982", None, 0.1, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Fri+1Fri+2Fri")
    CME_NQ = futures_info("E-mini NASDAQ 100 Index",
                          AssetClass.EQUITY_FUT.value,
                          "U1999", None, 0.25, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Fri+1Fri+2Fri")
    CME_ND = futures_info("Full-size NASDAQ 100 Index",
                          AssetClass.EQUITY_FUT.value,
                          "H1998", None, 0.25, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Fri+1Fri+2Fri")
    CME_DJ = futures_info("Full-size Dow Jones", AssetClass.EQUITY_FUT.value,
                          "H1998", None, 1, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Fri+1Fri+2Fri")
    CME_YM = futures_info("E-mini Dow Jones Futures", AssetClass.EQUITY_FUT.value,
                          "H2012", None, 1, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Fri+1Fri+2Fri")
    CME_MD = futures_info("S&P 400 MidCap Index", AssetClass.EQUITY_FUT.value,
                          "H1992", None, 0.05, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Fri+1Fri+2Fri")
    ICE_RF = futures_info("Russell 1000", AssetClass.EQUITY_FUT.value,
                          "U2008", None, 0.1, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Fri+1Fri+2Fri")
    ICE_TF = futures_info("Russell Small-Cap", AssetClass.EQUITY_FUT.value,
                          "H2007", None, 0.1, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Fri+1Fri+2Fri")
    ICE_RV = futures_info("Russell Value", AssetClass.EQUITY_FUT.value,
                          "M2010", None, 0.1, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Fri+1Fri+2Fri")
    ICE_RG = futures_info("Russell Growth", AssetClass.EQUITY_FUT.value,
                          "M2010", None, 0.1, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Fri+1Fri+2Fri")
    SGX_NK = futures_info("Nikkei 225 Index", AssetClass.EQUITY_FUT.value,
                          "Z2013", None, 5, "JPY",
                          ["H", "M", "U", "Z"],
                          None, "-Thu+Thu+Thu-2bd")  # sometimes prices are missing
    CME_NK = futures_info("Nikkei 225 Index USD", AssetClass.EQUITY_FUT.value,
                          "Z1990", None, 5, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-Fri+Fri+Fri-1bd")
    EUREX_FESX = futures_info("EURO STOXX 50", AssetClass.EQUITY_FUT.value,
                              "U1998", None, 1, "EUR",
                              ["H", "M", "U", "Z"],
                              None, "-1Fri+1Fri+2Fri")
    EUREX_FDAX = futures_info("DAX", AssetClass.EQUITY_FUT.value,
                              "H1997", None, 0.5, "EUR",
                              ["H", "M", "U", "Z"],
                              None, "-1Fri+1Fri+2Fri")
    EUREX_FSMI = futures_info("SMI", AssetClass.EQUITY_FUT.value,
                              "Z2013", None, 1, "CHF",
                              ["H", "M", "U", "Z"],
                              None, "-1Fri+1Fri+2Fri")
    LIFFE_FCE = futures_info("CAC40", AssetClass.EQUITY_FUT.value,
                             "H1999", None, 0.5, "EUR",
                             ["H", "M", "U", "Z"],
                             None, "-1Fri+1Fri+2Fri")
    LIFFE_Z = futures_info("FTSE 100", AssetClass.EQUITY_FUT.value,
                           "M1984", None, 0.5, "GBP",
                           ["H", "M", "U", "Z"],
                           None, "-1Fri+1Fri+2Fri")
    LIFFE_FTI = futures_info("AEX", AssetClass.EQUITY_FUT.value,
                             "H2014", None, 0.05, "EUR",
                             ["H", "M", "U", "Z"],
                             None, "-1Fri+1Fri+2Fri")
    # HKEX_HSI = futures_info("Hong Kong Hang Seng", AssetClass.EQUITY_FUT.value,
    #                         "U1997", None, 0.5, "HKD",
    #                         ["H", "M", "U", "Z"],
    #                         None, "+BMonthEnd-1bd")
    CME_IBV = futures_info("Ibovespa", AssetClass.EQUITY_FUT.value,
                           "Z2012", None, 5, "USD",
                           ["G", "J", "M", "Q", "V", "Z"],
                           None, "+14d+Wed-Wed-1bd+1bd")
    CFFEX_IF = futures_info("CSI 300", AssetClass.EQUITY_FUT.value,
                            "Z2010", None, 0.2, "CNY",
                            [m.name for m in FutureContractMonth],
                            None, "-1Fri+1Fri+2Fri")
    MX_SXF = futures_info("S&P/TSX 60 Index", AssetClass.EQUITY_FUT.value,
                          "M2011", None, 0.1, "CAD",
                          ["H", "M", "U", "Z"],
                          None, "-1Fri+1Fri+2Fri-1bd")
    SGX_IN = futures_info("Nifty Index", AssetClass.EQUITY_FUT.value,
                          "Z2013", None, 0.5, "USD",
                          ["H", "M", "U", "Z"],
                          None, "+BMonthEnd+Thu-Thu")
    LIFFE_BXF = futures_info("BEL 20 Index", AssetClass.EQUITY_FUT.value,
                             "V2013", None, 0.5, "EUR",
                             [m.name for m in FutureContractMonth],
                             None, "-1Fri+1Fri+2Fri")
    LIFFE_PSI = futures_info("PSI 20 Index", AssetClass.EQUITY_FUT.value,
                             "Z2013", None, 1, "EUR",
                             ["H", "M", "U", "Z"],
                             None, "-1Fri+1Fri+2Fri")
    ASX_AP = futures_info("Australia SPI 200 Index",
                          AssetClass.EQUITY_FUT.value,
                          "Z2013", None, 1, "AUD",
                          ["H", "M", "U", "Z"],
                          None, "-1Thu+1Thu+2Thu")
    SGX_CN = futures_info("FTSE China A50 Index", AssetClass.EQUITY_FUT.value,
                          "V2013", None, 1, "USD",
                          [m.name for m in FutureContractMonth],
                          None, "+BMonthEnd-1bd")
    SGX_ID = futures_info("MSCI Indonesia Index", AssetClass.EQUITY_FUT.value,
                          "V2013", None, 5, "USD",
                          [m.name for m in FutureContractMonth],
                          None, "+BMonthEnd-1bd")
    SGX_SG = futures_info("MSCI Singapore Index", AssetClass.EQUITY_FUT.value,
                          "V2013", None, 0.05, "USD",
                          [m.name for m in FutureContractMonth],
                          None, "+BMonthEnd-1bd")
    SGX_TW = futures_info("MSCI Taiwan Index", AssetClass.EQUITY_FUT.value,
                          "V2013", None, 0.1, "USD",
                          [m.name for m in FutureContractMonth],
                          None, "+BMonthEnd-1bd")
    EUREX_FMWO = futures_info("MSCI World Index", AssetClass.EQUITY_FUT.value,
                             "U2013", None, 1, "USD",
                              ["H", "M", "U", "Z"],
                             None, "-1Fri+1Fri+2Fri")
    EUREX_FMEU = futures_info("MSCI Europe Index", AssetClass.EQUITY_FUT.value,
                             "U2013", None, 0.05, "EUR",
                              ["H", "M", "U", "Z"],
                             None, "-1Fri+1Fri+2Fri")
    EUREX_FMMX = futures_info("MSCI Mexico Index", AssetClass.EQUITY_FUT.value,
                             "U2013", None, 0.1, "USD",
                              ["H", "M", "U", "Z"],
                             None, "-1Fri+1Fri+2Fri")
    EUREX_FMCN = futures_info("MSCI China Free Index", AssetClass.EQUITY_FUT.value,
                             "U2013", None, 0.1, "USD",
                              ["H", "M", "U", "Z"],
                             None, "-1Fri+1Fri+2Fri")
    EUREX_FMJP = futures_info("MSCI Japan Index", AssetClass.EQUITY_FUT.value,
                             "U2013", None, 1, "USD",
                              ["H", "M", "U", "Z"],
                             None, "-1Fri+1Fri+2Fri")
    EUREX_FMRS = futures_info("MSCI Russia Index", AssetClass.EQUITY_FUT.value,
                             "U2013", None, 0.1, "USD",
                              ["H", "M", "U", "Z"],
                             None, "-1Fri+1Fri+2Fri")
    EUREX_FMZA = futures_info("MSCI South Africa Index", AssetClass.EQUITY_FUT.value,
                             "U2013", None, 0.1, "USD",
                              ["H", "M", "U", "Z"],
                             None, "-1Fri+1Fri+2Fri")
    EUREX_FMTH = futures_info("MSCI Thailand Index", AssetClass.EQUITY_FUT.value,
                             "U2013", None, 0.5, "USD",
                              ["H", "M", "U", "Z"],
                             None, "-1Fri+1Fri+2Fri")
    EUREX_FMMY = futures_info("MSCI Malaysia Index", AssetClass.EQUITY_FUT.value,
                             "U2013", None, 0.1, "USD",
                              ["H", "M", "U", "Z"],
                             None, "-1Fri+1Fri+2Fri")
    EUREX_FMEA = futures_info("MSCI Emerging Markets Asia Index", AssetClass.EQUITY_FUT.value,
                             "U2013", None, 0.1, "USD",
                              ["H", "M", "U", "Z"],
                             None, "-1Fri+1Fri+2Fri")
    EUREX_FMEM = futures_info("MSCI Emerging Markets Index", AssetClass.EQUITY_FUT.value,
                             "U2013", None, 0.1, "USD",
                              ["H", "M", "U", "Z"],
                             None, "-1Fri+1Fri+2Fri")
    EUREX_FMEL = futures_info("MSCI Emerging Markets Latin America Index", AssetClass.EQUITY_FUT.value,
                             "U2013", None, 0.1, "USD",
                              ["H", "M", "U", "Z"],
                             None, "-1Fri+1Fri+2Fri")
    EUREX_FMEE = futures_info("MSCI Emerging Markets EMEA Index", AssetClass.EQUITY_FUT.value,
                             "U2013", None, 0.1, "USD",
                              ["H", "M", "U", "Z"],
                             None, "-1Fri+1Fri+2Fri")
    CBOE_VX = futures_info("VIX Futures", AssetClass.VOL_INDEX_FUT.value,
                           "K2004", None, 0.05, "USD",
                           [m.name for m in FutureContractMonth],
                           None, "+MonthBegin-1Fri+1Fri+2Fri-30d+1bd-1bd")
    EUREX_FVS = futures_info("VSTOXX Futures", AssetClass.VOL_INDEX_FUT.value,
                             "U2013", None, 0.05, "EUR",
                             [m.name for m in FutureContractMonth],
                             None, "+MonthBegin-1Fri+1Fri+2Fri-30d+1bd-1bd")
    # Government bond futures - CME
    # https://www.cmegroup.com/education/files/
    # understanding-treasury-futures.pdf
    CME_TU = futures_info("2-year Treasury Note", AssetClass.GOVT_FUT.value,
                          "U1990", Denominator.GOVT_FUT.value, 1.0 / 128, "USD",
                          ["H", "M", "U", "Z"],
                          "-BMonthEnd", "+BMonthEnd")
    CME_FV = futures_info("5-year Treasury Note", AssetClass.GOVT_FUT.value,
                          "U1988", Denominator.GOVT_FUT.value, 1.0 / 128, "USD",
                          ["H", "M", "U", "Z"],
                          "-BMonthEnd", "+BMonthEnd")
    CME_TY = futures_info("10-year Treasury Note", AssetClass.GOVT_FUT.value,
                          "M1990", Denominator.GOVT_FUT.value, 1.0 / 64, "USD",
                          ["H", "M", "U", "Z"],
                          "-BMonthEnd", "+BMonthEnd-7bd")
    CME_US = futures_info("30-year Treasury Bond", AssetClass.GOVT_FUT.value,
                          "Z1977", Denominator.GOVT_FUT.value, 1.0 / 32, "USD",
                          ["H", "M", "U", "Z"],
                          "-BMonthEnd", "+BMonthEnd-7bd")
    CME_UL = futures_info("Ultra Treasury Bond", AssetClass.GOVT_FUT.value,
                          "Z2012", Denominator.GOVT_FUT.value, 1.0 / 32, "USD",
                          ["H", "M", "U", "Z"],
                          "-BMonthEnd", "+BMonthEnd-7bd")
    EUREX_FGBS = futures_info("Euro-Schatz", AssetClass.GOVT_FUT.value,
                              "M1997", Denominator.GOVT_FUT.value, 0.005, "EUR",
                              ["H", "M", "U", "Z"],
                              None, "+9d-1bd+1bd-2bd")
    EUREX_FGBM = futures_info("Euro-Bobl", AssetClass.GOVT_FUT.value,
                              "U1998", Denominator.GOVT_FUT.value, 0.01, "EUR",
                              ["H", "M", "U", "Z"],
                              None, "+9d-1bd+1bd-2bd")
    EUREX_FGBL = futures_info("Euro-Bund", AssetClass.GOVT_FUT.value,
                              "H1991", Denominator.GOVT_FUT.value, 0.01, "EUR",
                              ["H", "M", "U", "Z"],
                              None, "+9d-1bd+1bd-2bd")
    EUREX_FGBX = futures_info("Euro-Buxl", AssetClass.GOVT_FUT.value,
                              "Z2013", Denominator.GOVT_FUT.value, 0.02, "EUR",
                              ["H", "M", "U", "Z"],
                              None, "+9d-1bd+1bd-2bd")
    EUREX_FBTS = futures_info("Short-term Euro-BTP", AssetClass.GOVT_FUT.value,
                              "U2013", Denominator.GOVT_FUT.value, 0.01, "EUR",
                              ["H", "M", "U", "Z"],
                              None, "+9d-1bd+1bd-2bd")
    EUREX_FBTP = futures_info("Long-term Euro-BTP", AssetClass.GOVT_FUT.value,
                              "Z2009", Denominator.GOVT_FUT.value, 0.01, "EUR",
                              ["H", "M", "U", "Z"],
                              None, "+9d-1bd+1bd-2bd")
    EUREX_FOAT = futures_info("Euro-OAT", AssetClass.GOVT_FUT.value,
                              "H2013", Denominator.GOVT_FUT.value, 0.01, "EUR",
                              ["H", "M", "U", "Z"],
                              None, "+9d-1bd+1bd-2bd")
    EUREX_CONF = futures_info("Swiss CONF", AssetClass.GOVT_FUT.value,
                              "U2013", Denominator.GOVT_FUT.value, 0.01, "CHF",
                              ["H", "M", "U", "Z"],
                              None, "+9d-1bd+1bd-2bd")
    MX_CGB = futures_info("10-year Canadian Bond", AssetClass.GOVT_FUT.value,
                          "H2009", Denominator.GOVT_FUT.value, 0.01, "CAD",
                          ["H", "M", "U", "Z"],
                          "-3bd", "+BMonthEnd-7bd")
    LIFFE_G = futures_info("Short Gilt", AssetClass.GOVT_FUT.value,
                           "Z2013", Denominator.GOVT_FUT.value, 0.01, "GBP",
                           ["H", "M", "U", "Z"],
                           "-2bd", "+BMonthEnd-2bd")
    LIFFE_H = futures_info("Medium Gilt", AssetClass.GOVT_FUT.value,
                           "Z2013", Denominator.GOVT_FUT.value, 0.01, "GBP",
                           ["H", "M", "U", "Z"],
                           "-2bd", "+BMonthEnd-2bd")
    LIFFE_R = futures_info("Long Gilt", AssetClass.GOVT_FUT.value,
                           "U1990", Denominator.GOVT_FUT.value, 0.01, "GBP",
                           ["H", "M", "U", "Z"],
                           "-2bd", "+BMonthEnd-2bd")
    # SGX JGB ceases one bd before OSE contract
    # sometimes data on last trading data are missing.
    SGX_JB = futures_info("10-year Mini Japanese Government Bond",
                          AssetClass.GOVT_FUT.value,
                          "Z2013", Denominator.GOVT_FUT.value, 0.01, "JPY",
                          ["H", "M", "U", "Z"],
                          None, "+19d-1bd+1bd-5bd-5bd")
    # Money market futures
    CME_ED = futures_info("3-month Eurodollar Futures", AssetClass.MM_FUT.value,
                          "H1982", Denominator.MM_FUT.value, 0.0025, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Wed+1Wed+2Wed-2bd")
    LIFFE_L = futures_info("Short Sterling Futures", AssetClass.MM_FUT.value,
                           "H1990", Denominator.MM_FUT.value, 0.005, "GBP",
                           ["H", "M", "U", "Z"],
                           None, "-1Wed+1Wed+2Wed")
    LIFFE_I = futures_info("3-month EURIBOR Futures", AssetClass.MM_FUT.value,
                           "H1999", Denominator.MM_FUT.value, 0.005, "EUR",
                           ["H", "M", "U", "Z"],
                           None, "-1Wed+1Wed+2Wed-2bd")
    LIFFE_S = futures_info("EUROSWISS Interest Rate Futures",
                           AssetClass.MM_FUT.value,
                           "H1991", Denominator.MM_FUT.value, 0.01, "CHF",
                           ["H", "M", "U", "Z"],
                           None, "-1Wed+1Wed+2Wed-2bd")
    TFX_JBA = futures_info("Tokyo 3-month Euroyen Futures",
                           AssetClass.MM_FUT.value,
                           "U1992", Denominator.MM_FUT.value, 0.005, "JPY",
                           ["H", "M", "U", "Z"],
                           None, "-1Wed+1Wed+2Wed-2bd")
    # FX - CME
    # https://www.cmegroup.com/education/files/understanding-fx-futures.pdf
    CME_EC = futures_info("Euro FX", AssetClass.FX_FUT.value,
                          "H1999", None, 0.00005, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Wed+1Wed+2Wed-2bd")
    CME_JY = futures_info("Japanese Yen", AssetClass.FX_FUT.value,
                          "H1977", None, 0.005 * 100, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Wed+1Wed+2Wed-2bd")
    CME_BP = futures_info("British Pound", AssetClass.FX_FUT.value,
                          "U1975", None, 0.01 / 100, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Wed+1Wed+2Wed-2bd")
    CME_SF = futures_info("Swiss Franc", AssetClass.FX_FUT.value,
                          "U1975", None, 0.01 / 100, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Wed+1Wed+2Wed-2bd")
    CME_CD = futures_info("Canadian Dollar", AssetClass.FX_FUT.value,
                          "M1977", None, 0.005 / 100, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Wed+1Wed+2Wed-2bd")
    CME_AD = futures_info("Australian Dollar", AssetClass.FX_FUT.value,
                          "H1987", None, 0.01 / 100, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Wed+1Wed+2Wed-2bd")
    CME_NE = futures_info("New Zealand Dollar", AssetClass.FX_FUT.value,
                          "H2004", None, 0.01 / 100, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Wed+1Wed+2Wed-2bd")
    CME_MP = futures_info("Mexican Peso", AssetClass.FX_FUT.value,
                          "M1995", None, 0.001 * 10000, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Wed+1Wed+2Wed-2bd")
    CME_RU = futures_info("Russian Ruble", AssetClass.FX_FUT.value,
                          "Z2011", None, 0.0005 * 10000, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Wed+1Wed+2Wed-2bd")
    CME_BR = futures_info("Brazilian Real", AssetClass.FX_FUT.value,
                          "H1996", None, 0.005 / 100, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Wed+1Wed+2Wed-2bd")
    CME_RA = futures_info("South African Rand", AssetClass.FX_FUT.value,
                          "H2014", None, 0.0025 * 10000, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Wed+1Wed+2Wed-2bd")
    CME_PZ = futures_info("Polish Zloty", AssetClass.FX_FUT.value,
                          "H2014", None, 0.00002, "USD",
                          ["H", "M", "U", "Z"],
                          None, "-1Wed+1Wed+2Wed-2bd")
    CME_TRY = futures_info("Turkish Lira", AssetClass.FX_FUT.value,
                           "H2014", None, 0.0001, "TRY",
                           ["H", "M", "U", "Z"],
                           None, "-1Wed+1Wed+2Wed-2bd")
    CME_CNH = futures_info("Standard-size USD/Offshore RMB (CNH)",
                           AssetClass.FX_FUT.value,
                           "H2014", None, 0.0001, "CNH",
                           ["H", "M", "U", "Z"],
                           None, "-1Wed+1Wed+2Wed-2bd")
    # CME - Commodity
    # Grains https://www.cmegroup.com/trading/agricultural/files/
    # AC-268_Grains_FC_FINAL_SR.pdf
    CME_GI = futures_info("S&P GSCI", AssetClass.COMDTY_FUT.value,
                         "Q1992", None, 0.05, "USD",
                         [m.name for m in FutureContractMonth],
                         None, "+10bd")
    CME_C = futures_info("Chicago Corn", AssetClass.COMDTY_FUT.value,
                         "H1960", None, 0.25, "USD",
                         ["H", "K", "N", "U", "Z"],
                         None, "+14d-1bd")
    CME_W = futures_info("Chicago Wheat", AssetClass.COMDTY_FUT.value,
                         "Z1959", None, 0.25, "USD",
                         ["H", "K", "N", "U", "Z"],
                         None, "+14d-1bd")
    CME_S = futures_info("Chicago Soybeans", AssetClass.COMDTY_FUT.value,
                         "F1970", None, 0.25, "USD",
                         ["F", "H", "K", "N", "Q", "U", "X"],
                         None, "+14d-1bd")
    CME_KW = futures_info("KC HRW Wheat", AssetClass.COMDTY_FUT.value,
                          "N1976", None, 0.25, "USD",
                          ["H", "K", "N", "U", "Z"],
                          None, "+14d-1bd")
    ICE_RS = futures_info('Canola',
                          AssetClass.COMDTY_FUT.value,
                          'F1981', None, 0.1, 'USD',
                          ['F', 'H', 'K', 'N', 'X'],
                          None, '+14d+1bd-1bd')
    LIFFE_EBM = futures_info('Milling Wheat',
                          AssetClass.COMDTY_FUT.value,
                          'H2013', None, 0.01, 'EUR',
                          ['F', 'H', 'K', 'U', 'X', 'Z'],
                          None, '+9d-1bd+1bd')
    LIFFE_ECO = futures_info('Rapeseed',
                          AssetClass.COMDTY_FUT.value,
                          'X2013', None, 0.25, 'EUR',
                          ['G', 'K', 'Q', 'X'],
                          None, '-BMonthEnd')
    MGEX_MW = futures_info('Hard Red Spring Wheat',
                          AssetClass.COMDTY_FUT.value,
                          'H1989', None, 0.25, 'USD',
                          ['H', 'K', 'N', 'U', 'X'],
                          None, '+14d+1bd-1bd')
    # Gold https://www.cmegroup.com/trading/metals/files/
    # MT-055E_GoldFuturesOptions.pdf
    CME_GC = futures_info("COMEX Gold", AssetClass.COMDTY_FUT.value,
                          "G1975", None, 0.1, "USD",
                          ["G", "J", "M", "Q", "V", "Z"],
                          "+0bd", "+MonthEnd-3bd")
    # Silver https://www.cmegroup.com/trading/metals/files/
    # cme-micro-silver-article.pdf
    CME_SI = futures_info("COMEX Silver", AssetClass.COMDTY_FUT.value,
                          "H1964", None, 0.005, "USD",
                          ["F", "H", "K", "N", "U", "Z"],
                          "+0bd", "+MonthEnd-3bd")
    # Platinum https://www.cmegroup.com/trading/metals/files/
    # platinum-and-palladium-futures-and-options.pdf
    CME_PL = futures_info("Platinum", AssetClass.COMDTY_FUT.value,
                          "F1970", None, 0.1, "USD",
                          ["F", "J", "N", "V"],
                          "+0bd", "+MonthEnd-2bd")
    CME_PA = futures_info("Palladium", AssetClass.COMDTY_FUT.value,
                          "H1977", None, 0.05, "USD",
                          ["H", "M", "U", "Z"],
                          "+0bd", "+MonthEnd-2bd")
    # Copper https://www.cmegroup.com/trading/metals/files/
    # copper-futures-and-options.pdf
    CME_HG = futures_info("Copper CME", AssetClass.COMDTY_FUT.value,
                          "Z1959", None, 0.05 / 100, "USD",
                          ["H", "K", "N", "U", "Z"],
                          "+0bd", "+MonthEnd-3bd")
    # Crude https://www.cmegroup.com/trading/energy/files/
    # light-sweet-crude-oil-futures-options.pdf
    CME_CL = futures_info("WTI Crude Oil", AssetClass.COMDTY_FUT.value,
                          "M1983", None, 0.01, "USD",
                          [m.name for m in FutureContractMonth],
                          None, "-1m+24d+1bd-4bd")
    # https://www.cmegroup.com/trading/energy/files/
    # EN-171_EnergyRetailBrochure_LowRes.pdf
    CME_HO = futures_info("Heating Oil", AssetClass.COMDTY_FUT.value,
                          "F1980", None, 0.01 / 100, "USD",
                          [m.name for m in FutureContractMonth],
                          None, "-BMonthEnd")
    CME_RB = futures_info("Gasoline", AssetClass.COMDTY_FUT.value,
                          "F2006", None, 0.01 / 100, "USD",
                          [m.name for m in FutureContractMonth],
                          None, "-BMonthEnd")
    # Natural gas https://www.cmegroup.com/education/files/
    # PM310_Natural_Gas_Futures.pdf
    CME_NG = futures_info("Natural Gas", AssetClass.COMDTY_FUT.value,
                          "M1990", None, 0.001, "USD",
                          [m.name for m in FutureContractMonth],
                          None, "-3bd")
    CME_N9 = futures_info('PJM Western Hub Real-Time Off-Peak',
                          AssetClass.COMDTY_FUT.value,
                          'G2014', None, 0.05, 'USD',
                          [m.name for m in FutureContractMonth],
                          None, '-BMonthEnd')
    CME_B6 = futures_info('PJM Northern Illinois Hub Real-Time Off-Peak',
                          AssetClass.COMDTY_FUT.value,
                          'G2014', None, 0.05, 'USD',
                          [m.name for m in FutureContractMonth],
                          None, '-BMonthEnd')
    CME_E4 = futures_info('PJM Western Hub Day-Ahead Off-Peak',
                          AssetClass.COMDTY_FUT.value,
                          'G2014', None, 0.05, 'USD',
                          [m.name for m in FutureContractMonth],
                          None, '-BMonthEnd-1bd')
    CME_D2 = futures_info('NYISO Zone G Day-Ahead Off-Peak',
                          AssetClass.COMDTY_FUT.value,
                          'G2014', None, 0.05, 'USD',
                          [m.name for m in FutureContractMonth],
                          None, '-BMonthEnd-1bd')
    CME_L3 = futures_info('PJM Northern Illinois Hub Day-Ahead Off-Peak',
                          AssetClass.COMDTY_FUT.value,
                          'G2014', None, 0.05, 'USD',
                          [m.name for m in FutureContractMonth],
                          None, '-BMonthEnd-1bd')
    CME_CU = futures_info('Chicago Ethanol',
                          AssetClass.COMDTY_FUT.value,
                          'G2014', None, 0.0001, 'USD',
                          [m.name for m in FutureContractMonth],
                          None, '-1bd')
    ICE_B = futures_info("Brent Crude Oil", AssetClass.COMDTY_FUT.value,
                         "F1993", None, 0.01, "USD",
                         [m.name for m in FutureContractMonth],
                         None, "-2BMonthEnd")
    ICE_G = futures_info("Gasoil", AssetClass.COMDTY_FUT.value,
                         "F1990", None, 0.25, "USD",
                         [m.name for m in FutureContractMonth],
                         None, "+13d-2bd")
    ICE_C = futures_info('EUA',
                          AssetClass.COMDTY_FUT.value,
                          'Z2005', None, 0.01, 'EUR',
                          ['H', 'M', 'U', 'Z'],
                          None, '+MonthEnd+Mon-2Mon+1bd-1bd')  # for convenience
    ICE_M = futures_info('UK Natural Gas',
                          AssetClass.COMDTY_FUT.value,
                          'H1997', None, 0.01, 'GBP',
                         [m.name for m in FutureContractMonth],
                          None, '-2bd')
    # Softs
    ICE_SB = futures_info("Sugar No. 11", AssetClass.COMDTY_FUT.value,
                          "H1964", None, 0.01, "USD",
                          ["H", "K", "N", "V"],
                          None, "-BMonthEnd")
    ICE_KC = futures_info("Coffee C", AssetClass.COMDTY_FUT.value,
                          "Z1973", None, 0.05, "USD",
                          ["H", "K", "N", "U", "Z"],
                          None, "+BMonthEnd-8bd")
    ICE_CT = futures_info("Cotton", AssetClass.COMDTY_FUT.value,
                          "K1972", None, 0.01, "USD",
                          ["H", "K", "N", "V", "Z"],
                          "-1bd+1bd-5bd", "+BMonthEnd-16bd")
    ICE_CC = futures_info("Cocoa", AssetClass.COMDTY_FUT.value,
                          "H1970", None, 1, "USD",
                          ["H", "K", "N", "U", "Z"],
                          "-1bd+1bd-5bd", "+BMonthEnd-10bd-1bd")
    ICE_OJ = futures_info('Orange Juice',
                          AssetClass.COMDTY_FUT.value,
                          'K1967', None, 0.05, 'USD',
                          ['F', 'H', 'K', 'N', 'U', 'X'],
                          None, '+BMonthEnd-14bd')
    LIFFE_W = futures_info('White Sugar',
                          AssetClass.COMDTY_FUT.value,
                          'V1993', None, 0.1, 'USD',
                          ['H', 'K', 'Q', 'V', 'Z'],
                          None, '-15d+1bd-1bd')
    # https://www.cmegroup.com/trading/agricultural/files/
    # fact-card-cattle-futures-options.pdf
    CME_LC = futures_info("Live Cattle", AssetClass.COMDTY_FUT.value,
                          "J1965", None, 0.025, "USD",
                          ["G", "J", "M", "Q", "V", "Z"],
                          "-Mon+Mon", "+BMonthEnd")  # FIXME to check
    CME_FC = futures_info("Feeder Cattle", AssetClass.COMDTY_FUT.value,
                          "H1974", None, 0.01, "USD",
                          ["F", "H", "J", "K", "Q", "U", "V", "Z"],
                          None, "+BMonthEnd-Thu")
    # https://www.cmegroup.com/trading/agricultural/files/
    # Lean-Hog-Futures-Options.pdf
    CME_LN = futures_info("Lean Hogs", AssetClass.COMDTY_FUT.value,
                          "G1970", None, 0.025, "USD",
                          ["G", "J", "M", "N", "Q", "V", "Z"],
                          None, "-1bd+1bd+9bd")
    CME_DA = futures_info("Milk", AssetClass.COMDTY_FUT.value,
                          "F2011", None, 0.01, "USD",
                          ["F", "J", "M", "N", "Q", "V", "Z"],
                          None, "-1bd+1bd+9bd")
    # http://www.hedgebroker.com/documents/education/
    # CME_GrainAndOilseedFuturesAndOptions.pdf
    CME_BO = futures_info("Soybean Oil", AssetClass.COMDTY_FUT.value,
                          "F1960", None, 0.01, "USD",
                          ["F", "H", "K", "N", "Q", "U", "V", "Z"],
                          "-BMonthEnd", "+14d-1bd")  # FIXME to check
    CME_SM = futures_info("Soybean meat", AssetClass.COMDTY_FUT.value,
                          "F1964", None, 0.01, "USD",
                          ["F", "H", "K", "N", "Q", "U", "V", "Z"],
                          "-BMonthEnd", "+14d-1bd")  # FIXME to check


class PriceSkipDates(Enum):
    ICE_RV = ["2014-04-15"]
    ICE_RG = ["2014-04-15"]
    LIFFE_BXF = ["2015-08-05"]
    SGX_NK = ['2018-01-26', '2018-01-29', '2018-01-30']
    SGX_IN = ['2018-01-26']
    SGX_CN = ['2018-01-26']
    EUREX_FBTP = ['2009-09-01']
    SGX_ID = ['2018-01-26']


class ReturnSkipDates(Enum):
    CME_RU = ["2014-07-14"]
    CME_BR = ["1999-12-14", "2000-01-03", "2000-03-28", "2000-11-24","2000-12-01"]
