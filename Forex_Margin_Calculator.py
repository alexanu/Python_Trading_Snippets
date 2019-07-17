import re
from collections import OrderedDict

class Margin_Calculator(object):
    # EX: 1.15000 sell eurusd 25.00 50:1 194,779 
    def calculate(self, **kwargs):
        # Organize Key word arguments
        kwargs = OrderedDict(kwargs)
        entry, direction, pair, lot_size, capital, risk_of_trade, margin, stop_loss = 0,0,0,0,0,0,0,0
        for key, value in kwargs.items():            
            if key == 'entry':
                entry = value
                continue
            elif entry == 0 and list(kwargs.keys())[-1]:
                raise ValueError('Entry Position is required: e.g. 1.15000')
            
            if key == 'direction':
                direction = value
                continue
            elif direction == 0 and list(kwargs.keys())[-1]:
                raise ValueError('Direction is required: e.g. "buy","Sell","Long","short"')
            
            if key == 'pair':
                if 'USD' in value:
                    pair = value
                    continue
                else:
                    raise ValueError('USD only pairs at this time')
            elif pair == 0 and list(kwargs.keys())[-1]:
                raise ValueError('pair is required: e.g. "EUR/USD" "USD/JPY"')
            
            if key == 'lot_size':
                lot_size = value
                continue
            elif lot_size == 0 and list(kwargs.keys())[-1]:
                raise ValueError("lot_size is required: e.g. 1.0 or 0.05")
            
            if key == 'capital':
                capital = value
                continue
            elif capital == 0 and list(kwargs.keys())[-1]:
                raise ValueError("capital is required: e.g 5000")
            
            if key == 'risk':
                risk_of_trade = value
                continue
            elif risk_of_trade == 0 and kwargs.keys()[-1]:
                raise ValueError("Risk is required (percent in decimal form) e.g. 0.02")
            
            if key == 'margin':
                margin = value
                continue
    
        # Static Margin Requirements from OANDA AUG 2018 
        # need to look at the pair that is being traded if the USD is not in the second place then the counter currency will be the measure of the pip
        margin_of_pairs = {'AUD/CAD':3.0,'AUD/CHF':3.0,'AUD/HKD':5.0,'AUD/JPY':4.0,'AUD/NZD':3.0,'AUD/SGD':5.0,'AUD/USD':3.0,'CAD/CHF':3.0,'CAD/HKD':5.0,'CAD/JPY':4.0,
        'CAD/SGD':5.0,'CHF/HKD':5.0,'CHF/JPY':4.0,'CHF/ZAR':5.0,'EUR/AUD':3.0,'EUR/CAD':2.0,'EUR/CHF':3.0,'EUR/CZK':5.0,'EUR/DKK':2.0,'EUR/GBP':5.0,'EUR/HKD':5.0,
        'EUR/HUF':5.0,'EUR/JPY':4.0,'EUR/NOK':3.0,'EUR/NZD':3.0,'EUR/PLN':5.0,'EUR/SEK':3.0,'EUR/SGD':5.0,'EUR/TRY':5.0,'EUR/USD':2.0,'EUR/ZAR':5.0,'GBP/AUD':5.0,
        'GBP/CAD':5.0,'GBP/CHF':5.0,'GBP/HKD':5.0,'GBP/JPY':5.0,'GBP/NZD':5.0,'GBP/PLN':5.0,'GBP/SGD':5.0,'GBP/USD':5.0,'GBP/ZAR':5.0,'HKD/JPY':5.0,'NZD/CAD':3.0,
        'NZD/CHF':3.0,'NZD/HKD':5.0,'NZD/JPY':4.0,'NZD/SGD':5.0,'NZD/USD':3.0,'SGD/CHF':5.0,'SGD/HKD':5.0,'SGD/JPY':5.0,'TRY/JPY':5.0,'USD/CAD':2.0,'USD/CHF':3.0,
        'USD/CNH':5.0,'USD/CZK':5.0,'USD/DKK':2.0,'USD/HKD':5.0,'USD/HUF':5.0,'USD/JPY':4.0,'USD/MXN':8.0,'USD/NOK':3.0,'USD/PLN':5.0,'USD/SAR':5.0,'USD/SEK':3.0,
        'USD/SGD':5.0,'USD/THB':5.0,'USD/TRY':5.0,'USD/ZAR':5.0,'ZAR/JPY':5.0}

        # List of Pairs that are not to the 5th decimal place (extremely weak)
        different_decimal_pairs = ['JPY','ZAR','SEK','HUF']
        decimal_change = False
        for i in different_decimal_pairs:
            if i in pair:
                decimal_change = True
                break


        # 100,000*0.01 (the 2nd decimal) /99.735≈$10.03 how to determine the pip price
        # if the margin is in another currency then you have to convert back to that currency

        # Determine the Counter Currency
        for name, percent in margin_of_pairs.items():
            decimal_check = re.compile('^\d+\.\d{0,2}$')
            # Make sure that the pair exists
            if str(name) == str(pair):

                # Determine Margin
                if margin == 0:
                    # use margin dict
                    margin = (percent/100)
                    break
                    # convert margin
                elif decimal_check.match(margin): 
                    margin = (margin/100)
                    break
                else:
                    raise ValueError('Margin in wrong format e.g. 10% = 10.0')
            # Remove the pair does not exist and no margin is given
            if name != pair and name == 'ZAR/JPY':
                raise ValueError("Pair does not match. e.g. EUR/USD")
            
        # Determine the total amount willing to risk
        cost_of_margin = margin * (lot_size*100000)
        remaining_capital = capital - cost_of_margin
        loss_risk = remaining_capital  * risk_of_trade
        # Dollar is base currency
        if '/USD' in pair:
            number_of_pips = loss_risk/ lot_size

        # Counter Currency where the dollar is not the base currency
        if 'USD/' in pair:
            # find the location of the decimal
            decimal = str(entry).find('.')
            # get the number of decimals
            number_of_decimals = len(str(entry)[decimal+1:])
            # only two cases I know currently
            if number_of_decimals == 5:
                multipler = 0.0001
            if number_of_decimals == 3:
                multipler = 0.01
            # add the number of 0's before the one
            movement_per_pip = (10000*multipler)/entry
            number_of_pips = int(loss_risk/movement_per_pip)

        
        # Determine the movement up or down
        if direction == 'sell' or direction == 'Sell' or direction == 'short' or direction == 'Short':
            if decimal_change:
                stop_loss = (number_of_pips/1000) + entry 
            else:
                stop_loss = (number_of_pips/100000) + entry 
        if direction == 'Buy' or direction == 'buy' or direction =='long' or direction =='Long':
            if decimal_change:
                stop_loss =  entry - (number_of_pips/1000)
            else:
                stop_loss =  entry - (number_of_pips/100000)   
        close_out_capital = capital - loss_risk
        return {'stop_loss': stop_loss,
                'potential_loss': loss_risk,
                'close_out_capital': close_out_capital}




# Works in current configuration   
print(Margin_Calculator().calculate(entry=111.067, direction='Sell',pair='USD/JPY',lot_size=1.00, capital=5000, risk=0.02))

print(Margin_Calculator().calculate(entry=1.15000, direction='Sell',pair='EUR/USD',lot_size=0.05, capital=5000, risk=0.02))