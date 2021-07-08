import sys

""" Adds the API directory to the import path. This is the more explicit and straightforward way for me but it can be done 
    in a different way depending on your python and API library installation """
sys.path.append('/home/bruno/ib_api/9_73/IBJts/source/pythonclient')

from threading import Thread
import logging
import time

from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.contract import *

def get_stock_contract(symbol):
    contract = Contract()
    contract.currency = "USD"
    contract.exchange = "SMART"
    contract.secType = "STK"
    contract.symbol = symbol
    #### Certain stocks/etfs tickers needs the exchange specified in the contract
    if symbol in ("GLD", "GDX", "GDXJ"):
        contract.exchange = "ARCA"
    elif symbol in ("MSFT", "INTC", "CSCO"):
        contract.exchange = "ISLAND"
    ############################################################################
    return contract


""" We are creating our IBExample class inheriting from both EClient and EWrapper.
    This way IBExample inherits all methods from Eclient and EWrapper """
class IBExample(EClient, EWrapper):
    def __init__(self):
        
        """ Calls __init__ method (constructor) on EClient parent class and assigns itself as the wrapper.
            This way the API knows we are using this class (self) as the wrapper too.
            Another way would be: 
                super().__init__(wrapper = self)
            but using EClient directly is more clear that we are calling EClient's constructor.
            Check EClient's __init__ method. on the API source code """
        EClient.__init__(self, wrapper = self)

        """ This variable stores the id of the current request. Every time a method from the EClient class is called, 
            it needs to have an id, this id is later received by the callbacks from the EWrapper class to know which EClass method 
            fired that callback. """
        self.req_id = 0

        """ This variable stores a map of request ids to a specific ticker. This way based on a request id that is sent by an 
            Ewrapper callback, we know which ticker a specific EWrapper callback is sending data from. """
        self.req_id_to_stock_ticker_map = {}


        """ This is the method from EClient that starts the connection with the server. It needs 3 arguments:
            1. localhost address
            2. specific listening port to connect, which should be configured in TWS or IB gateway, 
                I think 7496 is the default.
            3. application id. If there are several apps that connect to TWS or IB gateway on the same PC, 
                each application should use a different number
            Documentation for this method can be checked online or in the client.py module from the API. """
        self.connect("127.0.0.1", 7496, 0)

        """ The run() method is inherited from EClient. This method starts the message loop.
            self.run() never returns cause it has an infinite loop inside with the message queue.
            One way to continue program execution is to run it inside a thread, like it is done here.
            Another way is to run the application code inside one of the callbacks of EWrapper, I think this way is
            how it is intended to be, but the advantage of creating a thread is that it is clearer how to separate
            application logic with the API logic.
            Got the thread idea from: https://qoppac.blogspot.com.uy/2017/03/interactive-brokers-native-python-api.html """
        self.message_loop = Thread(target = self.run)
        self.message_loop.start()


    """ This method is a wrapper I made around reqMktData from EClient, it gets the next request id available,
        puts it on req_id_to_stock_ticker_map dictionary variable, and
        call EClient's reqMktData method with established default parameters.
        Check the documentation or the API source code comments for the available parameters of reqMktData """
    def request_market_data(self, ticker):
        req_id = self.get_next_req_id()
        self.req_id_to_stock_ticker_map[req_id] = ticker
        self.reqMktData(req_id, get_stock_contract(ticker), "", False, False, [])


    """ This method belongs to EWrapper. It's a callback method (like all EWrapper methods) that responds to a method call in 
        EClient, in this case, to reqMktData. Here it is being overridden to print the live quotes.
        Remember the usual workflow is to override methods from EWrapper to handle the requests
        made with methods from EClient.
        This method is called multiple times by the API with different tickTypes every time the price change (see
            parameter eplanation below)
        It receives 4 parameters with the requested information:
        1. reqId: is the request id sent with reqMktData.
        2. tickType: specifies what price is the callback associated with. A tickType of 1 means
            the price parameter is the bid price. A tickType of 2 means the price parameter is the ask price.
            So it is called one time for the bid price and one time for the ask price.
            Check the documentation or the API source for more details.
        3. price: is the price associated with the tickType, in this case for the ask price.
        4. attrib: canAutoExecute attribute. We are not using it here. """
    def tickPrice(self, reqId, tickType, price:float, attrib):
        """ Calling original method before overriding. Original method only logs data  """
        super().tickPrice(reqId, tickType, price, attrib)

        if tickType == 2:
            print(f"{time.strftime('%H:%M:%S')} : {self.req_id_to_stock_ticker_map[reqId]} => {format(price, '.2f')}")


    """ This method belongs to EClient and is being overridden here. When Ctrl + c is clicked, this method is 
        automatically called by the API. In this case it is overridden to clear market data streaming and disconnect. 
        Check clear_all definition in the private methods section. """
    def keyboardInterrupt(self):
        self.clear_all()


    ##########################################################################
    ################## Private
    ##########################################################################

    """ This is a custom method that goes over all request ids used in the session, cancels market data for all of them,
        and disconnects the client. """
    def clear_all(self):
        print("Canceling market data...")
        for req_id in self.req_id_to_stock_ticker_map.keys():
            self.cancelMktData(req_id) # This method belongs to EClient
        self.disconnect() # This method belongs to EClient
        print("Finished canceling market data.")


    """ Just returns an incremental number. Used for different request ids for the API. """
    def get_next_req_id(self):
        self.req_id += 1
        return self.req_id

#------------------------------------------------------------------------


if __name__ == "__main__":

    """ Creating basic logging file """
    logging.basicConfig(filename='./example.log', level=logging.INFO)
    
    try:
        """ Create an instance of the class connecting to the API and call the request_market_data method for 
            different tickers sent by command line parameters """
        ib_example = IBExample()
        for ticker in sys.argv[1:]:
            ib_example.request_market_data(ticker)

        """ Waiting indefinitely to keep the main thread running and catch the program termination exception """
        time.sleep(1000000000)
    except (KeyboardInterrupt, SystemExit) as e:
        ib_example.clear_all()
        print("Program stopped")
    except:
        ib_example.clear_all()
        raise