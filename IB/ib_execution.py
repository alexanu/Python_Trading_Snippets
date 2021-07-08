#!/usr/bin/python
# -*- coding: utf-8 -*-

#==========================================================
#==================== ib_execution.py =====================
#==========================================================

# Purpose
#----------------------------------------------------------
# In the back testing suite we used a naive ExecutionHandler to simulate taking
# in an order event, determining the market price, and creating an FillEvent
# with that price mimicking  trading against a broker as result to send back to
# the portfolio object. However this clearly isn't how we'll execute live
# trading. This file will encompass the more sophisticated handling of hooking
# up with the popular Interactive Brokers execution platform via its API. The
# IBExecutionHandler class will receive OrderEvent instances from the events
# queue and then execute them directly against the Interactive API using the
# IbPy library. The class will also handle the "Server Response" messages
# returned from the API ultimately creating a FillEvent instance to be sent
# back to the events queue to manage.
#
# This class could easily become quite complex with execution optimisation
# logic and thorough error handling logic, however similar to the other classes
# on a first time around basis we will go for breadth and up and running
# functionality as opposed to sophistication and depth.

from __future__ import print_function

import datetime as dt
import time

from ib.ext.Contract import Contract
from ib.ext.Order import Order
from ib.opt import ibConnection, message

from event import FillEvent, OrderEvent
from exectution import ExecutionHandler

class IBExecutionHandler(ExecutionHandler):
    """
    Handles order execution via the Interactive Brokers API, for use against
    accounts when trading live directly.

    The default order routing will be SOR (Smart Order Routing) and the default
    ccy will be USD.
    """

    def __init__(self, events, order_routing="SMART", currency="USD"):
        """
        Initializes the IBExecutionHandler instance

        Parameters:
            events - The Queue of Event objects
        """

        self.events = events
        self.order_routing = order_routing
        self.currency = currency
        self.fill_dict = {}

        self.tws_conn self.create_tws_connection()
        self.order_id = self.create_initial_order_id()
        self.register_handlers()

    def _error_handler(self, msg):
        """
        Handles the capturing of error messages. Simply prints to the terminal.
        Further error handling functionality should be implemented.
        """

        print("Server Error: %s" % msg)

    def _reply_handler(self, msg):
        """
        Handles server replies
        """

        # Handle open order orderID processing
        if msg.typeName == "openOrder" and  msg.orderID == self.order_id and \
            not self.fill_dict.has_key(msg.orderID):
            self.create_fill_dict_entry(msg)
        # Handle Fills
        if msg.typeName == "orderStatus" and msg.status == "Filled" and \
            self.fill_dict[msg.orderID]["filled"] == False:
            self.create_fill(msg)
        print("Server Response: %s, %s\n" % (msg.typeName, msg))

    def create_tws_connection(self):
        """
        Connect to the Trader Workstation (TWS, IB API using the IbPy ibConnection
        object) using the standard port of 7496, with a clientId of 10.

        The clientId is chosen by the user and we will need separate IDs for
        both the execution connection and the market data connection, if the
        latter is also used elsewhere.
        """

        tws_conn = ibConnection()
        tws_conn.connect()
        return tws_conn

    def create_initial_order_id(self):
        """
        Creates the initial order ID used for Interactive Brokers to keep track
        of submitted orders.

        There is certainly scope for more sophisticated logic here. For example
        a more correct approach would be to query IB for the latest next
        available ID.
        """

        return 1

    def register_handlers(self):
        """
        Register the error and server reply message handling functions
        """

        # Assign the error handling function defined above to the TWS
        # connection
        self.tws_conn.register(self._error_handler, 'Error')

        # Assign all of the server reply messages to the reply_handler function
        # defined above
        self.tws_conn.registerAll(self._reply_handler)

    def create_contract(self, symbol, sec_type, exch, prim_exch, curr):
        """
        In order to actually transact a trade it is necessary to create an IbPy
        Contract instance and then pair it with an IbPy Order instance which
        will be sent to the IB API.

        This method creates a Contract object defining what will be purchased, at which
        exchange and in which currency.

        Parameters:
            symbol - The ticker symbol for the contract
            sec_type - The security type for the contract ('STK' is 'stock')
            exch - The exchange to carry out the contract on
            prim_exch - The primary exchange to carry out the contract on
            curr - The currency in which to purchase the contract
        """

        contract = Contract()
        contract.m_symbol = symbol
        contract.m_secType = sec_type
        contract.m_exchange = exch
        contract.m_primaryExch = prim_exch
        contract.m_currenct = curr
        return contract

    def create_order(self, order_type, quantity, action):
        """
        Creates the second component of the pair, the Order object
        (Market/Limit) to go long/short.

        Parameters
            order_type - 'MKT' or 'LMT' for Market or Limit orders respectively
            quantity - An integral number of assets to order
            action - 'BUY' or 'SELL'
        """

        order = Order()
        order.m_orderType = order_type
        order.m_totalQuantity = quantity
        order.m_action = action
        return order

    def create_fill_dict_entry(self, msg):
        """
        In order to avoid duplicating FillEvent instances for a particular
        order ID, we will utilize a dictionary called fill_dict to store keys
        that match particular order IDs. When a fill has been generated the
        "filled" key of an entry for a particular order ID will be set to True.
        Any subsequent "Server Response" messages recieved will not lead to a
        new fill.

        TLDR: Creates an entry in the Fill Dictionary that lists orderIds and
        provides security information. This is needed for the event-driven
        behaviour of the IB server message behaviour.
        """

        self.fill_dict[msg.orderId] = {
            "symbol" : msg.contract.m_symbol,
            "exchange" : msg.contract.m_exchange,
            "direction" : msg.order.m_action,
            "filled": False
        }

    def create_fill(self, msg):
        """
        Handles the creation of the FillEvent that will be placed onto the
        events queue subsequent to an order being filled
        """

        fd = self.fill_dict[msg.orderId]

        # Prepare the fill data
        symbol = fd["symbol"]
        exchange = fd["exchange"]
        filled = msg.filled
        direction = fd["direction"]
        fill_cost = msg.avgFillPrice

        # Create a fill event object
        fill = FillEvent(dt.datetime.utcnow(), symbol, exchange, filled,
                direction, fill_cost)

        # Make sure that multiple messages don't create additional fills
        self.fill_dict[msg.orderID]["filled"] = True

        # Place the fill event onto the event queue
        self.events.put(fill_event)

    def execute_order(self, event):
        """
        All methods are available to create the final execute_order method
        which overrides the method from the ExecutionHandler abstract base
        class.

        First check the event being received is actually an OrderEvent, then
        prepare the Contract and Order objects with their respective
        parameters. Lastly use the IbPy method placeOrder to establish
        connection and place the order with an associated order_id.

        The results are then queried in order to generate a corresponding Fill
        object, which is placed back into the event queue.

        Parameters:
            event - Contains an Event object with order information
        """

        if event.type == 'ORDER':
            # Prepare the parameters for the asset order
            asset = event.symbol
            asset_type = "STK"
            order_type = event.order_type
            quantity = event.quantity
            direction = event.direction

            # Create the Interactive Brokers contract via the passed Order
            # event
            ib_contract = sefl.create_contract(
                asset, asset_type, self.order_routing, self.order_routing,
                self.currency
                )
            # Create the Interactive Brokers order via the passed Order event
            ib_order = self.create_order(
                order_type, quantity, direction
                )

            # Use the connection to send the order to IB
            self.tws_conn.placeOrder(
                self.order_id, ib_contract, ib_order
            )

            # NOTE: The following line is essential to ensure that orders
            # connect and collect server responses appropriately. In essence a
            # one second delay between filling and returning order details
            # ensures that each order processes optimally. Without this I've
            # witnessed the process crash.
            time.sleep(1)

            # Increment the order ID for this ordering session
            self.order_id += 1

