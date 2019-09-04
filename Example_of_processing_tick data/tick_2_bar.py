'''
This class is designed to be used in conjunction with a main script that receives
a stream of tick data in real time.  Please read comments thouroughly as there
are a few things you must implement in your main script for this to work.
HOW TO USE
-Assumes time_bars is a dictionary of {'Symbol' : TimeBar objects}
-Assumes sym is the symbol.ticker string of the stock
In main script:
    -Initialize TimeBar object for each symbol
            On Symbol Update/ Tick Received
                -Call new_print to append the print object
                    -time_bars[sym].new_print(symbol.last_print)
	-Create bar_timer() in main script (see below)
		This function is what calls the bar_closed()
	-Start the bar_timer thread
                    
USING THE BARS
In main script
    -bar_list = time_bar[ticker].get_bar_list()
    -bar_list[-1] = most recent closed bar
    -bar_list[-1]['high'] = high of most recent closed bar
    -bar attributes are 'open', 'high', 'low', 'close', 'close_time'
    -bar close time is the real local time that a new print came in and told
    this class to close the bar (round off seconds if needed)
	
LIMITATIONS
*Need seperate TimeBar Object for each symbol
*Bar interval can be minutes only for now (1-30)
###########################
** bar_timer function **
This is the thread function that should handle all timing responsibilties and run in main script:
REQUIRES global variables:
BAR_INTERVAL = int
TIME_BAR_DICT = dictionary if string : TimeBar object
optional - is python logger setup (remove statements with logging if you dont want)
def bar_timer():
    ''''''
	Called in seperate thread, called after initializes all the bars.
    Will call bar_close() in every time bar.  This function requires a global
    BAR_INTERVAL which is the n mintues of the time bars, and the TIME_BAR_DICT
    ''''''
    def set_first_bar_close():
        ''''''run on intialization, uses system time and rounds off seconds to 
        set global next bar close time.  This was used mainly in older time
        bar classes - not needed with the thread timer''''''
        now = datetime.now() + timedelta(minutes=1)
        while now.minute % BAR_INTERVAL != 0:
            #increment by a minute until its proper interval
            now += timedelta(minutes=1)
        return now.replace(second=0, microsecond=0)
        
    def increment_next_bar_close_time(previous_time):
        ''''''will increment the property next bar close time by the bar interval''''''
        previous_time += timedelta(minutes=BAR_INTERVAL)
        return previous_time
    logging.info("Bar time thread starting..")
    #Sleep until start of first bar - so start with full bar
    seconds_to_sleep = 0
    now = datetime.now()
    target_time = set_first_bar_close() #First bar close
    logging.info("Initial target time: " + str(target_time))
    delta = target_time - now
    seconds_to_sleep = delta.total_seconds()
    sleep(seconds_to_sleep)
    #TODO - WARNIONG - the first bar will ahve ticks from the previous fraction bar here
    #TODO - WARNING - this is where I can loop through and clear every symbols tick_list before starting the full bar
    #TODO- WARNING - but since it will be premarket I dont think it is sucha big deal to have fraction of bar here
    #Now loop through every full interval and build bars
    while True:
        target_time = increment_next_bar_close_time(target_time)
        logging.info("Next target time: " + str(target_time))
        now = datetime.now()
        delta = target_time - now
        sleep(delta.total_seconds())
        #time it here to end of for loop, how long to close all bars?
        for ticker in TIME_BAR_DICT:
            TIME_BAR_DICT[ticker].bar_closed()
bar_timer_thread = Thread(target=bar_timer)
bar_timer_thread.start()
### END ###
'''
import copy
from datetime import datetime
from datetime import timedelta
from time import sleep
import numpy as np
import logging

class TimeBar(object):
    
    def __init__(self, ticker, minutes=1, vwap_interval=60):
        '''(string, int, int)'''
        self.__bar_interval = minutes #must be in minutes
        self.__tick_list = [] #list of tuples of (price, volume)
        self.__bar_list = []  #OHLC, VWAP
        self.__symbol = ticker
        self.__vwap_interval = vwap_interval  #number of BAR INTERVALS for vwap
        self.previous_length_of_bar_list = 0 #used for checking if new bar
        self.track_vwap = False #haven't tested with True, just moved this to eliminate some code

    def check_if_new_bar(self):
        '''(self) -> bool
        
        Called from main script, checks the length of bar list and returns
        True if current bar list is bigger than previous time it returned True.
        '''
        if len(self.__bar_list) > self.previous_length_of_bar_list:
            #New Bar
            self.previous_length_of_bar_list = len(self.__bar_list)
            return True
        else:
            return False       
        
        
    def bar_closed(self):
        '''(None) -> None
		
		When bar closes this runs through tick list to build and append the new
        OHLC bar.'''
        close_time = datetime.now()
        volume = self.sum_tick_vol()
        try:
            open_price = self.__tick_list[0][0]
            high_price = max(self.__tick_list)[0]
            low_price = min(self.__tick_list)[0]
            close_price = self.__tick_list[-1][0]
        except:
            try:
                #no prints, using last close
                open_price = self.__bar_list[-1]['close']
                high_price = self.__bar_list[-1]['close']
                low_price = self.__bar_list[-1]['close']
                close_price = self.__bar_list[-1]['close']
            except:
                #no prints and no bars set to 0.0
                open_price = 0.0
                high_price = 0.0
                low_price = 0.0
                close_price = 0.0        
        if self.track_vwap:
            product_vol_price = self.sum_product_tick_vol_price() #Math to make it easier for VWAP calculation
            try:
                vwap = self.get_current_vwap()  #vwap of this bar an preceding bars not to exceed interval
            except:
                print "ERROR GETTING VWAP: " + self.__symbol
                logging.info("ERROR GETTING VWAP: " + self.__symbol)
                vwap = 0
            self.__bar_list.append({'close_time':close_time, 'open':open_price, 
                                'high':high_price, 'low':low_price, 'close':close_price,
                                'volume':volume, 'product_vol_price':product_vol_price,
                                'vwap':vwap})
        else:
            self.__bar_list.append({'close_time':close_time, 'open':open_price, 
                                'high':high_price, 'low':low_price, 'close':close_price,
                                'volume':volume})
        #Reset Tick List
        self.__tick_list = []
                                
    def sum_tick_vol(self):
        '''(None) -> int
		
		Called when bar closed - sums volume from all ticks in tick list.
		'''
        total_vol = 0
        for t in self.__tick_list:
            total_vol += t[1] #tick list is list of [price, volume]
        return total_vol
    
    def sum_product_tick_vol_price(self):
        '''(None) -> float
		
		Called when bar closed - sums (volume * product) from all ticks in tick list
		'''
        total_product = 0
        for t in self.__tick_list:
            total_product += (t[0] * t[1])
        return round(total_product,3)
                   
    def get_current_vwap(self):
        '''(None) -> float
		
		Uses the bar info of volume, volume * price for each bar from current
        bar up until the bar that is included in the vwap interval to calculate
        current vwap.  if have incomplete interval, then will use all available bars.
		'''
        #At this point in time this function is called, the bar closing right now has not been appended to bar list
        current_bar_tick_vol = self.sum_tick_vol()
        current_bar_product_tick_vol_price = self.sum_product_tick_vol_price()
        if current_bar_product_tick_vol_price <= 0:
            #Bar probably jsut closed (unless this is fisrt bar then no prints yet)
            try:
                logging.info(self.__symbol + " failed to get VWAP, trying previous bar")
                print self.__symbol + " failed to get VWAP, trying previous bar"
                logging.info(datetime.now())
                print datetime.now()
                logging.info("Tick List Length: " + str(len(self.__tick_list)))
                print "Tick List Length: " + str(len(self.__tick_list))
                logging.info("current bar tick vol = " + str(current_bar_tick_vol))
                print "current bar tick vol = " + str(current_bar_tick_vol)
                logging.info("current bar product = " + str(current_bar_product_tick_vol_price))
                print "current bar product = " + str(current_bar_product_tick_vol_price)
                last_vwap = self.get_last_vwap()
                logging.info("Last Vwap: " + str(last_vwap))
                print "Last Vwap: " + str(last_vwap)
                return last_vwap
            except:
                logging.info("MAJOR ERROR - UNABLE TO GET LAST BAR VWAP! - returning 0")
                print "MAJOR ERROR - UNABLE TO GET LAST BAR VWAP! - returning 0"
                return 0
        #If len of bar list < vwap interval then do all
        if len(self.__bar_list) < self.__vwap_interval:
            #not enough bars for full vwap so do for all bars
            total_volume = current_bar_tick_vol
            total_product = current_bar_product_tick_vol_price
            for bar in self.__bar_list:
                total_volume += bar['volume']
                total_product += bar['product_vol_price']
            vwap = total_product / total_volume
            return round(vwap, 3)
        else:
            #have enough data - take only bars for vwap interval
            total_volume = current_bar_tick_vol
            total_product = current_bar_product_tick_vol_price
            target_index = self.__vwap_interval - 1  #since the closing bar isnt in list yet, and using it -
            for bar in self.__bar_list[-1:-target_index:-1]: #last element, to middle, counting backwards
                total_volume += bar['volume']
                total_product += bar['product_vol_price']
            vwap = total_product / total_volume
            return round(vwap, 3)
    
    def convert_unix_to_datetime(self, unix_time):
        '''(datetime) -> datetime
		
		Converts unix timestamp from Takion, returns datetime object.
		'''
        return datetime.utcfromtimestamp(unix_time) 
    
    def new_print(self, sym_print):
        '''(print_object) -> None
		
		Called from main script every time a new tick/ print is received.  Pass the 
		print object.  The conditions I use below are certain trade conditions I chose
		to ignore (things like odd lot prints, block trades, etc.).
		
		This function will probably need to be reworked based on the format of your
		prints.  Modify 2 places in this function so your print format will work.
		sym_print.condtion is optional, you can ignore that if you want to use all ticks.
		
		sym_print.price - expects a float of the last print
		sym_print.quantity - expects a int of the # of shares in last print
		'''       
        #TODO add more filtering logic for pritns 0.15% from nbbo? more ?
        bad_conditions = "IBWCHLMNPQRUVZ47T"
        
        for cond in bad_conditions:
            if cond in sym_print.condition:
                #Print to ignore
                return
                
        self.__tick_list.append([sym_print.price, sym_print.quantity])
        
    def get_bar_field_as_series(self, field, bars_back):
        '''(string, int) -> numpy array 
        
        Takes a field ('open, high, low, close, etc') and returns a np array of
        only that field from all the bars in the bar list.  For use with TA-Lib.
        '''
        data_list = []
        for bar in self.__bar_list[-(bars_back+1):]:
            data_list.append(bar[field])
        return np.asarray(data_list)
        
    '''### Bar Series Functions ###'''    
    def print_last_bar(self):
        print self.__bar_list[-1]
        
    def get_bar_list(self):
        '''returns copy of bar_list'''
        bar_list_copy = copy.deepcopy(self.__bar_list)
        return bar_list_copy
        
    def get_last_bar(self):
        return self.__bar_list[-1]
        
    def get_last_vwap(self):
        '''returns vwap from last bar'''
        return self.__bar_list[-1]['vwap']
        
    def get_current_tick_list(self):
return self.__tick_list
