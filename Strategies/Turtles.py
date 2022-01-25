# Turtles
# trend followings: looking for price breakouts (closing highs or lows over a given lookback period) to buy an instrument (in the case of highs) 
# or short (in the case of lows) it. 
# 2 systems: 
# S2 is slower than S1:
# S2 watches for a 55-day breakout for an entry and a 20-day breakout in the opposite direction to sell (which I’ll refer to as a breakdown). 
# For example, if the system goes long after a breakout (i.e., when the closing price of the stock exceeds the previous 55-day high), 
# then it will close the position when the price hits the lowest close over the past 20 trading days.
# S1 looks for a 20-day breakout for an entry and 10-day breakdown for an exit. T
# he other slight wrinkle for S1 is a filter that causes it to trade every other successful breakout. 
# This means if some signal turns into a profitable trade, then the next time that signal comes up, it skips it.

Turtles were given the freedom to allocate whatever percentage of their capital to either system.
# The Turtles used a volatility-based position sizing method to normalize their risk across contracts and instruments. 
# The position size was calculated using the 20-day simple moving average (SMA-20) of the True Range (TR) of the price: ATR(20). Higher ATR means a higher volatility and increased risk. 
