
def pips_to_price(instrument, pips):
    """
    Given an instrument and price, convert price to pips
    Examples:
        pips_to_price('USD_JPY', 100) == 1
    Returns: decimal or None
    """
    if instrument in currency_pair_conversions:
        return (currency_pair_conversions[instrument] * pips);
    else:
        return None

def price_to_pips(instrument, price):
    """
    """
    if not instrument in currency_pair_conversions:
        return None
    else:
        # TODO Why the check for zero?
        if price != 0:
            return (price / currency_pair_conversions[instrument])
        return 0

currency_pair_conversions = {
'AU200_AUD':0.1
,'AUD_CAD':0.0001
,'AUD_CHF':0.0001
,'AUD_HKD':0.0001
,'AUD_JPY':0.01
,'AUD_NZD':0.0001
,'AUD_SGD':0.0001
,'AUD_USD':0.0001
,'BCO_USD':0.01
,'CAD_CHF':0.0001
,'CAD_HKD':0.0001
,'CAD_JPY':0.01
,'CAD_SGD':0.0001
,'CH20_CHF':0.1
,'CHF_HKD':0.0001
,'CHF_JPY':0.01
,'CHF_ZAR':0.0001
,'CORN_USD':0.01
,'DE10YB_EUR':0.01
,'DE30_EUR':0.1
,'EU50_EUR':0.1
,'EUR_AUD':0.0001
,'EUR_CAD':0.0001
,'EUR_CHF':0.0001
,'EUR_CZK':0.0001
,'EUR_DKK':0.0001
,'EUR_GBP':0.0001
,'EUR_HKD':0.0001
,'EUR_HUF':0.01
,'EUR_JPY':0.01
,'EUR_NOK':0.0001
,'EUR_NZD':0.0001
,'EUR_PLN':0.0001
,'EUR_SEK':0.0001
,'EUR_SGD':0.0001
,'EUR_TRY':0.0001
,'EUR_USD':0.0001
,'EUR_ZAR':0.0001
,'FR40_EUR':0.1
,'GBP_AUD':0.0001
,'GBP_CAD':0.0001
,'GBP_CHF':0.0001
,'GBP_HKD':0.0001
,'GBP_JPY':0.01
,'GBP_NZD':0.0001
,'GBP_PLN':0.0001
,'GBP_SGD':0.0001
,'GBP_USD':0.0001
,'GBP_ZAR':0.0001
,'HK33_HKD':0.1
,'HKD_JPY':0.0001
,'JP225_USD':0.1
,'NAS100_USD':0.1
,'NATGAS_USD':0.01
,'NL25_EUR':0.01
,'NZD_CAD':0.0001
,'NZD_CHF':0.0001
,'NZD_HKD':0.0001
,'NZD_JPY':0.01
,'NZD_SGD':0.0001
,'NZD_USD':0.0001
,'SG30_SGD':0.1
,'SGD_CHF':0.0001
,'SGD_HKD':0.0001
,'SGD_JPY':0.01
,'SOYBN_USD':0.01
,'SPX500_USD':0.1
,'SUGAR_USD':0.0001
,'TRY_JPY':0.01
,'UK100_GBP':0.1
,'UK10YB_GBP':0.01
,'US2000_USD':0.01
,'US30_USD':0.1
,'USB02Y_USD':0.01
,'USB05Y_USD':0.01
,'USB10Y_USD':0.01
,'USB30Y_USD':0.01
,'USD_CAD':0.0001
,'USD_CHF':0.0001
,'USD_CNH':0.0001
,'USD_CNY':0.0001
,'USD_CZK':0.0001
,'USD_DKK':0.0001
,'USD_HKD':0.0001
,'USD_HUF':0.01
,'USD_INR':0.01
,'USD_JPY':0.01
,'USD_MXN':0.0001
,'USD_NOK':0.0001
,'USD_PLN':0.0001
,'USD_SAR':0.0001
,'USD_SEK':0.0001
,'USD_SGD':0.0001
,'USD_THB':0.01
,'USD_TRY':0.0001
,'USD_TWD':0.0001
,'USD_ZAR':0.0001
,'WHEAT_USD':0.01
,'WTICO_USD':0.01
,'XAG_AUD':0.0001
,'XAG_CAD':0.0001
,'XAG_CHF':0.0001
,'XAG_EUR':0.0001
,'XAG_GBP':0.0001
,'XAG_HKD':0.0001
,'XAG_JPY':0.1
,'XAG_NZD':0.0001
,'XAG_SGD':0.0001
,'XAG_USD':0.0001
,'XAU_AUD':0.01
,'XAU_CAD':0.01
,'XAU_CHF':0.01
,'XAU_EUR':0.01
,'XAU_GBP':0.01
,'XAU_HKD':0.01
,'XAU_JPY':10
,'XAU_NZD':0.01
,'XAU_SGD':0.01
,'XAU_USD':0.01
,'XAU_XAG':0.01
,'XCU_USD':0.0001
,'XPD_USD':0.01
,'XPT_USD':0.01
,'ZAR_JPY':0.01
}



