"""Technical, portfolio, risk analysis APIs for your quantitative trading algorithms."""

from datetime import datetime
import pandas as pd
import talib
import numpy as np
from pyti.hull_moving_average import hull_moving_average as hma
from pyti.function_helper import fill_for_noncomputable_vals
from pyti.vertical_horizontal_filter import vertical_horizontal_filter as vhf
from scipy.signal import argrelmin, argrelmax
import scipy.stats as scstat
import empyrical
from tabulate import tabulate
import ta


#Accumulation/Distribution Index (ADI)
def accumulation_distribution_index(high, low, close, volume, fillna=False):
    try:
        adi_data = ta.volume.acc_dist_index(high,
                                            low,
                                            close,
                                            volume,
                                            fillna=fillna)
        return adi_data
    except Exception as e:
        raise (e)


#Awesome Oscillator
def awesome_oscillator(high, low, short_period=5, long_period=34,
                       fillna=False):
    try:
        ao_data = ta.momentum.ao(high, low, short_period, long_period, fillna)
        return ao_data
    except Exceptions as e:
        raise (e)


#Money Flow Index
def money_flow_index(high, low, close, volume, n, fillna=False):
    try:
        mfi_data = ta.momentum.money_flow_index(high, low, close, volume, n,
                                                fillna)
        return mfi_data
    except Exception as e:
        raise (e)


#Relative Strength Index
def relative_strength_index(close, n, fillna=False):
    try:
        rsi_data = ta.momentum.rsi(close, n, fillna)
        return rsi_data
    except Exception as e:
        raise (e)


#Stochastic Oscillator
def stochastic_oscillator(high, low, close, n, fillna=False):
    try:
        stoch_data = ta.momentum.stoch(high, low, close, n, fillna)
        return stoch_data
    except Exception as e:
        raise (e)


#True Strength Index
def true_strength_index(close, high_period, low_period, fillna=False):
    try:
        tsi_data = ta.momentum.tsi(close, high_period, low_period, fillna)
        return tsi_data
    except Exception as e:
        raise (e)


#Ultimate Oscillator
def ultimate_oscillator(high, low, close, short_period, medium_period,
                        long_period):
    try:
        uo_data = talib.ULTOSC(high,
                               low,
                               close,
                               timeperiod1=short_period,
                               timeperiod2=medium_period,
                               timeperiod3=long_period)
        return uo_data
    except Exception as e:
        raise (e)


#Absoulte Price Oscillator
def absolute_price_oscillator(close, short_period, long_period, matype=0):
    try:
        apo_data = talib.APO(close, short_period, long_period, matype=matype)
        return apo_data
    except Exception as e:
        raise (e)


#Aroon
def aroon(high, low, n):
    try:
        aroondown, aroonup = talib.AROON(high, low, timeperiod=n)
        df = pd.DataFrame()
        df['aroondown'] = aroondown
        df['aroonup'] = aroonup
        df['aroon_oscillator'] = talib.AROONOSC(high, low, timeperiod=n)
        return df
    except Exception as e:
        raise (e)


#Aroon
def aroon_oscillator(high, low, n):
    try:
        aroon_osc_data = talib.AROONOSC(high, low, timeperiod=n)
        return aroon_osc_data
    except Exception as e:
        raise (e)


#Average Directional Movement Index
def average_directional_moving_index(high, low, close, n):
    try:
        admi_data = talib.ADX(high, low, close, timeperiod=n)
        return admi_data
    except Exception as e:
        raise (e)


#Average Directional Movement Index Rating
def average_directional_moving_index_rating(high, low, close, n):
    try:
        admir_data = talib.ADXR(high, low, close, timeperiod=n)
        return admir_data
    except Exception as e:
        raise (e)


#Average Price
def average_price(open, high, low, close):
    try:
        avgprice_data = talib.AVGPRICE(open, high, low, close)
        return avgprice_data
    except Exception as e:
        raise (e)


#Average True Range
def average_true_range(high, low, close, n):
    try:
        atr_data = talib.ATR(high, low, close, timeperiod=n)
        return atr_data
    except Exception as e:
        rasie(e)


#Normalized Average True Range
def normalized_average_true_range(high, low, close, n):
    try:
        natr_data = talib.NATR(high, low, close, timeperiod=n)
        return natr_data
    except Exception as e:
        raise (e)


#True Range
def true_range(high, low, close):
    try:
        tr_data = talib.TRANGE(high, low, close)
        return tr_data
    except Exception as e:
        raise (e)


#Balance of Power
def balance_of_power(open, high, low, close):
    try:
        bop_data = talib.BOP(open, high, low, close)
        return bop_data
    except Exception as e:
        raise (e)


#Bollinger Bands
def bollinger_bands(close, n=20, nbdevup=2, nbdevdn=2, matype=0):
    try:
        upperband, middleband, lowerband = talib.BBANDS(close,
                                                        timeperiod=n,
                                                        nbdevup=nbdevup,
                                                        nbdevdn=nbdevdn,
                                                        matype=matype)
        df = pd.DataFrame()
        df['upperband'] = upperband
        df['middleband'] = middleband
        df['lowerband'] = lowerband
        return df
    except Exception as e:
        raise (e)


#Chaiking Money Flow
def chaikin_money_flow(high, low, close, volume, n, fillna=False):
    try:
        cmf_data = ta.volume.chaikin_money_flow(high,
                                                low,
                                                close,
                                                volume,
                                                n,
                                                fillna=fillna)
        return cmf_data
    except Exception as e:
        raise (e)


#Chande Moemntum oscillator
def chande_momentum_oscillator(close, n):
    try:
        cmo_data = talib.CMO(close, timeperiod=n)
        return cmo_data
    except Exception as e:
        raise (e)


#Commodity Channel Index
def commodity_channel_index(high, low, close, n):
    try:
        cci_data = talib.CCI(high, low, close, timeperiod=n)
        return cci_data
    except Exception as e:
        raise (e)


#Correlation Coefficient
def correlation_coefficient(high, low, n):
    try:
        cor_co_data = talib.CORREL(high, low, timeperiod=n)
        return cor_co_data
    except Exception as e:
        raise (e)


#Cumulative Returns
def cumulative_returns(close, fillna=False):
    try:
        cr_data = ta.others.cumulative_return(close, fillna=fillna)
        return cr_data
    except Exception as e:
        raise (e)


#Daily Returns
def daily_returns(close, fillna=False):
    try:
        dr_data = ta.others.daily_return(close, fillna)
        return dr_data
    except Exception as e:
        raise (e)


#Daily Log Return
def daily_log_returns(close, fillna=False):
    try:
        dr_data = ta.others.daily_log_return(close, fillna)
        return dr_data
    except Exception as e:
        raise (e)


#Detrended Price Oscillator
def detrended_price_oscillator(close, n, fillna=False):
    try:
        dpo_data = ta.trend.dpo(close, n, fillna)
        return dpo_data
    except Exception as e:
        raise (e)


#Directional Movement Index
def directional_movement_index(high, low, close, n):
    try:
        dmo_data = talib.DX(high, low, close, timeperiod=n)
        return dmo_data
    except Exception as e:
        raise (e)


#Donchian Channels
def donchian_channel(close, n, fillna=False):
    try:
        dc_high_band = ta.volatility.donchian_channel_hband(close, n, fillna)
        dc_high_band_indicator = ta.volatility.donchian_channel_hband_indicator(
            close, n, fillna)
        dc_low_band = ta.volatility.donchian_channel_lband(close, n, fillna)
        dc_low_band_indicator = ta.volatility.donchian_channel_lband_indicator(
            close, n, fillna)
        df = pd.DataFrame()
        df['dc_high_band'] = dc_high_band
        df['dc_high_band_indicator'] = dc_high_band_indicator
        df['dc_low_band'] = dc_low_band
        df['dc_low_band_indicator'] = dc_low_band_indicator
        return df
    except Exception as e:
        raise (e)


#Double EMA
def double_ema(close, n):
    try:
        dema_data = talib.DEMA(close, n)
        return dema_data
    except Exception as e:
        raise (e)


#Ease of movement
def ease_of_movement(high, low, close, volume, n, fillna=False):
    try:
        eom_data = ta.volume.ease_of_movement(high, low, close, volume, n,
                                              fillna)
        return eom_data
    except Exception as e:
        raise (e)


#Force Index
def force_index(close, volume, n, fillna=False):
    try:
        fi_data = ta.volume.force_index(close, volume, n, fillna)
        return fi_data
    except Exception as e:
        raise (e)


#EMA
def ema(close, n):
    try:
        ema_data = talib.EMA(close, timeperiod=n)
        return ema_data
    except Exception as e:
        raise (e)


#Ichimoku Cloud
def ichimoku_cloud(high,
                   low,
                   short_period,
                   medium_period,
                   long_period,
                   fillna=False):
    try:
        ichimoku_cloud_a = ta.trend.ichimoku_a(high, low, short_period,
                                               medium_period, fillna)
        ichimoku_cloud_b = ta.trend.ichimoku_b(high, low, medium_period,
                                               long_period, fillna)
        df = pd.DataFrame()
        df['ichimoku_cloud_a'] = ichimoku_cloud_a
        df['ichimoku_cloud_b'] = ichimoku_cloud_b
        return df
    except Exception as e:
        raise (e)


#Kaufman Adaptive Moving Average
def kaufman_adaptive_moving_average(close, n):
    try:
        kama_data = talib.KAMA(close, timeperiod=n)
        return kama_data
    except Exception as e:
        raise (e)


#Keltner Channels
def keltner_channels(high, low, close, n, fillna=False):
    try:
        df = pd.DataFrame()
        df['keltner_channel_central'] = ta.volatility.keltner_channel_central(
            high, low, close, n, fillna)
        df['keltner_channel_highband'] = ta.volatility.keltner_channel_hband(
            high, low, close, n, fillna)
        df['keltner_channel_highband_indicator'] = ta.volatility.keltner_channel_hband_indicator(
            high, low, close, n, fillna)
        df['keltner_channel_lowband'] = ta.volatility.keltner_channel_lband(
            high, low, close, n, fillna)
        df['keltner_channel_lowband_indicator'] = ta.volatility.keltner_channel_lband_indicator(
            high, low, close, n, fillna)
        return df
    except Exception as e:
        raise (e)


#Know Sure Thing
def know_sure_thing(close, r1, r2, r3, r4, n1, n2, n3, n4, nsig, fillna=False):
    try:
        kst_data = ta.trend.kst_sig(close, r1, r2, r3, r4, n1, n2, n3, n4,
                                    nsig, fillna)
        return kst_data
    except Exception as e:
        raise (e)


#Linear Regression
def linear_regression(close, n):
    try:
        lr_data = talib.LINEARREG(close, timeperiod=n)
        return lr_data
    except Exception as e:
        raise (e)


#Linear Regression Angle
def linear_regression_angle(close, n):
    try:
        lra_data = talib.LINEARREG_ANGLE(close, timeperiod=n)
        return lra_data
    except Exception as e:
        raise (e)


#Linear Regression Intercept
def linear_regression_intercept(close, n):
    try:
        lri_data = talib.LINEARREG_INTERCEPT(close, timeperiod=n)
        return lri_data
    except Exception as e:
        raise (e)


#Linear Regression Slope
def linear_regression_slope(close, n):
    try:
        lrs_data = talib.LINEARREG_SLOPE(close, timeperiod=n)
        return lrs_data
    except Exception as e:
        raise (e)


#Moving Average Convergence Divergence
def macd(close, n_sign, short_period=12, long_period=26, fillna=False):
    try:
        df = pd.DataFrame()
        df['macd'] = ta.trend.macd(close, short_period, long_period, fillna)
        df['macd_signal'] = ta.trend.macd_signal(close, short_period,
                                                 long_period, n_sign, fillna)
        df['macd_difference'] = ta.trend.macd_diff(close, short_period,
                                                   long_period, n_sign, fillna)
        return df
    except Exception as e:
        raise (e)


#Mass Index
def mass_index(high, low, short_period, long_period, fillna=False):
    try:
        mi_data = ta.trend.mass_index(high, low, short_period, long_period,
                                      fillna)
        return mi_data
    except Exception as e:
        raise (e)


#Midpoint over period
def midpoint_over_period(close, n):
    try:
        mop = talib.MIDPOINT(close, timeperiod=n)
        return mop
    except Exception as e:
        raise (e)


#Midpoint Price over period
def midpoint_price_over_period(high, low, n):
    try:
        mpop = talib.MIDPRICE(high, low, timeperiod=n)
        return mpop
    except Exception as e:
        raise (e)


#Minus Directional Indicator
def minus_directional_indicator(high, low, close, n):
    try:
        mdi_data = talib.MINUS_DI(high, low, close, timeperiod=n)
        return mdi_data
    except Exception as e:
        raise (e)


#Minus Directional Movement
def minus_directional_movement(high, low, close, n):
    try:
        mdm_data = talib.MINUS_DM(high, low, timeperiod=n)
        return mdm_data
    except Exception as e:
        raise (e)


#Median Price
def median_price(high, low):
    try:
        mp_data = talib.MEDPRICE(high, low)
        return mp_data
    except Exception as e:
        rasie(e)


#Momentum
def momentum(close, n):
    try:
        momentum_data = talib.MOM(close, timeperiod=n)
        return momentum_data
    except Exception as e:
        rasie(e)


#Moving Average
def ma(close, n, matype=0):
    try:
        ma_data = talib.MA(close, timeperiod=n, matype=matype)
        return ma_data
    except Exception as e:
        raise (e)


#Negative Volume Index
def negative_volume_index(close, volume, fillna=False):
    try:
        nvi_data = ta.volume.negative_volume_index(close,
                                                   volume,
                                                   fillna=fillna)
        return nvi_data
    except Exception as e:
        raise (e)


#Hilbert Transform - Instantaneous Trendline
def hilbert_transform_inst_trendline(close):
    try:
        htit_data = talib.HT_TRENDLINE(close)
        return htit_data
    except Exception as e:
        raise (e)


#Hilbert Transform - Dominant Cycle Period
def hilbert_transform_dom_cyc_per(close):
    try:
        htdcp_data = talib.HT_DCPERIOD(close)
        return htdcp_data
    except Exception as e:
        raise (e)


#Hilbert Transform - Dominant Cycle Phase
def hilbert_transform_dom_cyc_phase(close):
    try:
        htdcp_data = talib.HT_DCPHASE(close)
        return htdcp_data
    except Exception as e:
        raise (e)


#Hilbert Transform - Phasor Components
def hilbert_transform_phasor_components(close):
    try:
        inphase_data, quadrature_data = talib.HT_PHASOR(close)
        df = pd.DataFrame()
        df['inphase'] = inphase_data
        df['quadrature'] = quadrature_data
        return df
    except Exception as e:
        raise (e)


#Hilbert Transform - Sine Wave
def hilbert_transform_sine_wave(close):
    try:
        sine_data, leadsine_data = talib.HT_SINE(close)
        df = pd.DataFrame()
        df['sine'] = sine_data
        df['leadsine'] = leadsine_data
        return df
    except Exception as e:
        raise (e)


#Hilbert Transform - Trend vs cycle mode
def hilbert_transform_trend_vs_cycle_mode(close):
    try:
        httc_data = talib.HT_TRENDMODE(close)
        return httc_data
    except Exception as e:
        raise (e)


#On Balance Volume
def on_balance_volume(close, volume):
    try:
        obv_data = talib.OBV(close, volume)
        return obv_data
    except Exception as e:
        raise (e)


#Parabolic SAR
def parabolic_sar(high, low, acceleration=0,
                  maximum=0):  #Acceleration and max 0 by default
    try:
        ps_data = talib.SAR(high, low, acceleration, maximum)
        return ps_data
    except Exception as e:
        raise (e)


#Percentage Price Oscillator
def percentage_price_oscillator(close, short_period, long_period, matype=0):
    try:
        ppo_data = talib.PPO(close, short_period, long_period, matype)
        return ppo_data
    except Exception as e:
        raise (e)


#Plus Directional Indicator
def plus_directional_indicator(high, low, close, n):
    try:
        pdi_data = talib.PLUS_DI(high, low, close, timeperiod=n)
        return pdi_data
    except Exception as e:
        rasie(e)


#Plus Directional Movement
def plus_directional_movement(high, low, close, n):
    try:
        pdm_data = talib.PLUS_DM(high, low, timeperiod=n)
        return pdm_data
    except Exception as e:
        rasie(e)


#Rate of Change
def rate_of_change(close, n):
    try:
        df = pd.DataFrame()
        df['rate_of_change'] = talib.ROC(close, timeperiod=n)
        df['rate_of_change_precentage'] = talib.ROCP(close, timeperiod=n)
        df['rate_of_change_ratio'] = talib.ROCR(close, timeperiod=n)
        df['rate_of_change_ratio_100_scale'] = talib.ROCR100(close,
                                                             timeperiod=n)
        return df
    except Exception as e:
        rasie(e)


#SMA
def sma(close, n):
    try:
        sma_data = talib.SMA(close, timeperiod=n)
        return sma_data
    except Exception as e:
        raise (e)


#Standard Deviation
def standard_deviation(close, n, nbdev=1):
    try:
        sd_data = talib.STDDEV(close, timeperiod=n, nbdev=nbdev)
        return sd_data
    except Exception as e:
        raise (e)


#Stochastic RSI
def stochastic_rsi(close, n, fast_period1=5, fast_period2=3, fastd_matype=0):
    try:
        fastk_data, fastd_data = talib.STOCHRSI(close, n, fast_period1,
                                                fast_period2, fastd_matype)
        df = pd.DataFrame()
        df['fast_period1'] = fastk_data
        df['fast_period2'] = fastd_data
        return df
    except Exception as e:
        raise (e)


#Time Series Forecast
def time_series_forecast(close, n):
    try:
        tsf_data = talib.TSF(close, timeperiod=n)
        return tsf_data
    except Exception as e:
        rasie(e)


#Trix
def trix(close, n):
    try:
        trix_data = talib.TRIX(close, timeperiod=n)

        return trix_data
    except Exception as e:
        raise (e)


#Triangular Moving average
def triangular_ma(close, n):
    try:
        tma_data = talib.TRIMA(close, timeperiod=n)
        return tma_data
    except Exception as e:
        raise (e)


#Triple Exponential Moving Average
def triple_ema(close, n):
    try:
        tema_data = talib.TEMA(close, timeperiod=n)
        return tema_data
    except Exception as e:
        raise (e)


#Typical Price
def typical_price(high, low, close):
    try:
        tp_data = talib.TYPPRICE(high, low, close)
        return tp_data
    except Exception as e:
        raise (e)


#variance
def variance(data, n, nbdev=1):
    try:
        var_data = talib.VAR(data, timeperiod=n, nbdev=nbdev)
        return var_data
    except Exception as e:
        raise (e)


#Vortex Indicator
def vortex_indicator(high, low, close, n, fillna=False):
    try:
        df = pd.DataFrame()
        df['vortex_inidicator_positive'] = ta.trend.vortex_indicator_pos(
            high, low, close, n, fillna)
        df['vortex_indicator_negative'] = ta.trend.vortex_indicator_neg(
            high, low, close, n, fillna)
        return df
    except Exception as e:
        raise (e)


#Weighted Close Price
def weighted_close_price(high, low, close):
    try:
        wcp_data = talib.WCLPRICE(high, low, close)
        return wcp_data
    except Exception as e:
        raise (e)


#Williams' %R
def williams_r(high, low, close, n):
    try:
        wr_data = talib.WILLR(high, low, close, timeperiod=n)
        return wr_data
    except Exception as e:
        raise (e)


#Weighted Moving Average
def wma(close, n):
    try:
        wma_data = talib.WMA(close, timeperiod=n)
        return wma_data
    except Exception as e:
        raise (e)


#Two Crows
def two_crows(open, high, low, close):
    try:
        tc_data = talib.CDL2CROWS(open, high, low, close)
        return tc_data
    except Exception as e:
        raise (e)


#Annual Return (in percent)
def annual_return(close):
    if len(close) < 250 or len(close) > 257:
        return "Close data has more or less entries than one year should have. Please use returns() function to calculate returns for n number of days."

    try:
        trading_days = len(close)
        df = pd.DataFrame()
        df['daily_returns'] = daily_returns(close)
        mean_daily_returns = df['daily_returns'].mean()
        annual_return = mean_daily_returns * trading_days
        return annual_return
    except Exception as e:
        raise (e)


#Returns for any number of days
def returns(close):
    try:
        trading_days = len(close)
        df = pd.DataFrame()
        df['daily_returns'] = daily_returns(close)
        mean_daily_returns = df['daily_returns'].mean()
        returns_data = mean_daily_returns * trading_days
        return returns_data
    except Exception as e:
        raise (e)


#Daily Volatility
def daily_volatility(close):
    try:
        dr_data = daily_returns(close)
        dv_data = np.std(dr_data)
        return dv_data
    except Exception as e:
        raise (e)


#Annual Volatility
def annual_volatility(close):
    if len(close) < 250 or len(close) > 260:
        return "Close data has more or less entries than one year should have. Please use volatility() function to calculate volatility for n number of days."
    try:
        trading_days = len(close)
        dr_data = daily_returns(close)
        av_data = np.std(dr_data) * np.sqrt(trading_days)
        return av_data
    except Exception as e:
        raise (e)


#Volatility for any number of days
def volatility(close):
    try:
        trading_days = len(close)
        dr_data = daily_returns(close)
        av_data = np.std(dr_data) * np.sqrt(trading_days)
        return av_data
    except Exception as e:
        raise (e)


#Moving volatility
def moving_volatility(close, n):
    try:
        df = pd.DataFrame()
        drs = daily_returns(close)
        df['moving_volatility'] = drs.rolling(n).std() * np.sqrt(n)
        return df
    except Exception as e:
        raise (e)


#Chaikin Oscillator
def chaikin_oscillator(high,
                       low,
                       close,
                       volume,
                       short_period=3,
                       long_period=10):
    try:
        df = pd.DataFrame()
        df['chaikin_ad_line'] = talib.AD(high, low, close, volume)
        df['chaikin_oscillator'] = talib.ADOSC(high,
                                               low,
                                               close,
                                               volume,
                                               fastperiod=short_period,
                                               slowperiod=long_period)
        return df
    except Exception as e:
        raise (e)


#Coppock Curve
def coppock_curve(close, n):
    try:
        M = close.diff(int(n * 11 / 10) - 1)
        N = close.shift(int(n * 11 / 10) - 1)
        ROC1 = M / N
        M = close.diff(int(n * 14 / 10) - 1)
        N = close.shift(int(n * 14 / 10) - 1)
        ROC2 = M / N
        Copp = pd.Series((ROC1 + ROC2).ewm(span=n, min_periods=n).mean(),
                         name='coppock_curve')
        df = pd.DataFrame(Copp)
        return df
    except Exception as e:
        raise (e)


#Hull moving average
def hull_moving_average(close, n):
    try:
        df = pd.DataFrame()
        df['hull_mavg'] = hma(close, n)
        return df
    except Exception as e:
        raise (e)


#Volume Price Trend
def volume_price_trend(close, volume, fillna=False):
    try:
        vpt = volume * ((close - close.shift(1)) / close.shift(1))
        df = pd.DataFrame()
        df['volume_price_trend'] = vpt.shift(1) + vpt
        return df
    except Exception as e:
        raise (e)


#Volume adjusted moving average
def volume_adjusted_moving_average(close, volume, n):
    try:
        avg_vol = np.mean(volume)
        vol_incr = avg_vol * 0.67
        vol_ratio = [val / vol_incr for val in volume]
        close_vol = np.array(close) * vol_ratio
        vama = [
            sum(close_vol[idx + 1 - n:idx + 1]) / n
            for idx in range(n - 1, len(close))
        ]
        vama = fill_for_noncomputable_vals(close, vama)
        df = pd.DataFrame()
        df['value_adjusted_mavg'] = vama
        return df
        return df
    except Exception as e:
        raise (e)


#Sharpe Ratio
def sharpe_ratio(close):
    try:
        trading_days = len(close)
        sqrt = np.sqrt(trading_days)
        avg_returns = np.mean(daily_returns(close))
        std_returns = np.std(daily_returns(close))
        sharpe_ratio = (sqrt * avg_returns) / std_returns
        return sharpe_ratio
    except Exception as e:
        raise (e)


#Annual Sharpe Ratio
def annual_sharpe_ratio(close):
    if len(close) < 250 or len(close) > 255:
        return (
            "For annual sharpe ratio close data should constitue data only for 1 year. To calculate sharpe ratio for n number of days use sharpe_ratio"
        )
    try:
        sqrt = np.sqrt(len(close))
        avg_returns = np.mean(daily_returns(close))
        std_returns = np.std(daily_returns(close))
        sharpe_ratio = (sqrt * avg_returns) / std_returns
        return sharpe_ratio
    except Exception as e:
        raise (e)


#VWAP
def vwap(high, low, close, volume):
    try:
        df = pd.DataFrame()
        df['VWAP'] = np.cumsum(volume * (high + low) / 2) / np.cumsum(volume)
        return df
    except Exception as e:
        raise (e)


#Sortino Ratio
def sortino_ratio(close, target_return):
    try:
        ret = returns(close)
        dr_data = downside_risk(close)
        sr_data = (ret - target_return) / dr_data
        return sr_data
    except Exception as e:
        raise (e)


#Calmar Ratio
def calmar_ratio(close):
    try:
        rets = np.array(daily_returns(close))
        rets = rets[~np.isnan(rets)]
        mins = argrelmin(rets)
        maxs = argrelmax(rets)
        extrema = np.concatenate((mins, maxs))
        extrema.sort()
        calmar = -rets.mean() / np.diff(rets[extrema]).min()
        return calmar
    except Exception as e:
        raise (e)


#Skew
def skewness(data):
    try:
        return scstat.skew(data)
    except Exception as e:
        raise (e)


#Kurtosis
def kurtosis(data):
    try:
        return scstat.kurtosis(data)
    except Exception as e:
        raise (e)


#Omega Ratio
def omega_ratio(close, risk_free=0.0, required_return=0.0, trading_days=252):
    try:
        rets = daily_returns(close)
        omr_data = empyrical.omega_ratio(rets,
                                         risk_free=risk_free,
                                         required_return=required_return,
                                         annualization=trading_days)
        return omr_data
    except Exception as e:
        raise (e)


#Tail Ratio
def tail_ratio(close):
    try:
        rets = daily_returns(close)
        tr_data = empyrical.tail_ratio(rets)
        return tr_data
    except Exception as e:
        raise (e)


#Alpha
def alpha(close,
          benchmark_close,
          risk_free=0.0,
          period='daily',
          annualization=None,
          _beta=None):
    try:
        rets = daily_returns(close)
        benchmark_rets = daily_returns(benchmark_close)
        alpha_data = empyrical.alpha(rets,
                                     benchmark_rets,
                                     risk_free=risk_free,
                                     period=period,
                                     annualization=annualization,
                                     _beta=_beta)
        return alpha_data
    except Exception as e:
        raise (e)


#Beta
def beta(close, benchmark_close, risk_free=0.0):
    try:
        rets = daily_returns(close)
        benchmark_rets = daily_returns(benchmark_close)
        beta_data = empyrical.beta(rets, benchmark_rets, risk_free=risk_free)
        return beta_data
    except Exception as e:
        raise (e)


#Adjusted returns
def adjusted_returns(returns, adjustment_factor):
    if isinstance(adjustment_factor, (float, int)) and adjustment_factor == 0:
        return returns.copy()
    return returns - adjustment_factor


#Information Ratio
def information_ratio(daily_returns, daily_benchmark_returns):
    try:
        if len(daily_returns) != len(daily_benchmark_returns):
            return 'Length of returns and benchmar_returns is not equal'
        trading_days = len(daily_returns)
        rets = daily_returns.mean() * trading_days
        benchmark_rets = daily_benchmark_returns.mean() * trading_days
        return_difference = rets - benchmark_rets
        volatility = daily_returns.std() * np.sqrt(trading_days)
        information_ratio = return_difference / volatility
        return information_ratio
    except Exception as e:
        raise (e)


#CAGR(in percent)
def cagr(start_value, end_value, period_in_years):
    try:

        cagr_data = (end_value / start_value)**(1 / period_in_years) - 1
        return cagr_data * 100
    except Exception as e:
        raise (e)


#Downside Risk
def downside_risk(close, required_return=0, period='daily',
                  annualization=None):
    try:
        rets = daily_returns(close)
        dr_data = empyrical.downside_risk(rets,
                                          required_return=required_return,
                                          period=period,
                                          annualization=annualization)
        return dr_data
    except Exception as e:
        raise (e)


#Value at risk
def value_at_risk(close, tabular=True):
    try:
        rets = daily_returns(close)
        mean = np.mean(rets)
        std_dev = np.std(rets)
        var_90 = scstat.norm.ppf(1 - 0.9, mean, std_dev)
        var_95 = scstat.norm.ppf(1 - 0.95, mean, std_dev)
        var_99 = scstat.norm.ppf(1 - 0.99, mean, std_dev)
        if tabular == True:
            data = tabulate(
                [["90%", var_90], ["95%", var_95], ["99%", var_99]],
                headers=["Confidence Level", "Value at Risk"])
            return data
        if tabular == False:
            data = {"90%": var_90, "95%": var_95, "99%": var_99}
            return data
        else:
            return 'Invalid output type'
    except Exception as e:
        raise (e)
