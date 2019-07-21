class RateInfoRecord:
    def __init__(self, close, ctm, ctmString, high, low, open, vol, digits=0):
        """ Rate information record
        :param close {float} Value of close price (shift from open price)
        :param ctm	{timestamp}	Candle start time in CET / CEST time zone (see Daylight Saving Time, DST)
                - since epoch in milliseconds
        :param ctmString {string} String representation of the 'ctm' field
                - Ex: "Jan 10, 2014 3:04:00 PM"
        :param high	{float} Highest value in the given period (shift from open price)
        :param low	{float} Lowest value in the given period (shift from open price)
        :param open	{float} Open price (in base currency * 10 to the power of digits)
        :param vol	{float} Volume in lots
        :param digits {int}
        """
        self.close = close
        self.ctm = ctm
        self.ctm_string = ctmString
        self.high = high
        self.low = low
        self.open = open
        self.volume = vol
        self.digits = digits

    def __repr__(self):
        return '{}'.format(self.__dict__)
