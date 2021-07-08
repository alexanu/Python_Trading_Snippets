import sqlite3
from sqlite3 import Error
  
class StockInfo:
    """
    store info for each stock holding
    """
    def __init__(self, symbol, num_holdings, price, avg_cost, date):
        self.symbol = symbol
        self.num_holdings = num_holdings
        self.price = price
        self.avg_cost = avg_cost
        self.date = date

class Order:
    """
    store info for each order
    """
    def __init__(self, symbol, num, price, date):
        self.symbol = symbol
        self.num = num
        self.price = price
        self.date = date
        
class Holdings:
    """
    class for interacting with the database
    """
    def __init__(self, opt, stock_mat, db_file = "portfolio.db"):
        self.holdings = {}
        self.db_file = db_file
        #now = datetime.datetime.now()
        #self.current_date = now.strftime("%Y-%m-%d")
        self.opt = opt
        self.stock_mat = stock_mat
        self.orders = []
 
        try:
            self.conn = sqlite3.connect(self.db_file)
            print(sqlite3.version)
        except Error as e:
            print(e)       

    def create_tables(self):
        """ 
        create asset_values_record and holdings_record tables
        """
        sql_create_asset_values_record_table = """CREATE TABLE IF NOT EXISTS asset_values_record (
                                                    date date PRIMARY KEY,
                                                    asset_value numeric NOT NULL,
                                                    trade_count integer NOT NULL,
                                                    num_symbols integer NOT NULL                                               
                                                );"""       
        
        sql_create_holdings_record_table = """ CREATE TABLE IF NOT EXISTS holdings_record (
                                                id integer PRIMARY KEY,
                                                symbol text NOT NULL,
                                                num_holdings float NOT NULL,
                                                price float NOT NULL,
                                                avg_cost float NOT NULL,
                                                date date NOT NULL,
                                                FOREIGN KEY (date) REFERENCES asset_values (date)
                                            ); """
         
        sql_create_current_holdings_table = """ CREATE TABLE IF NOT EXISTS current_holdings (
                                                symbol text PRIMARY KEY,
                                                num_holdings float NOT NULL,
                                                price float NOT NULL,
                                                avg_cost float NOT NULL,
                                                date date NOT NULL
                                            ); """ 
        
        sql_create_orders_record_table = """ CREATE TABLE IF NOT EXISTS orders_record (
                                                id integer PRIMARY KEY,
                                                symbol text NOT NULL,
                                                num float NOT NULL,
                                                price float NOT NULL,
                                                date date NOT NULL
                                            ); """         
        try:
            c = self.conn.cursor()
            c.execute(sql_create_asset_values_record_table)
            c.execute(sql_create_holdings_record_table)
            c.execute(sql_create_current_holdings_table)
            c.execute(sql_create_orders_record_table)
        except Error as e:
            print(e)
            

    def insert_asset_value(self, av_record):
        """
        Insert a new stock info into the asset_values_record table
        """
        sql = ''' INSERT INTO asset_values_record(date, asset_value, trade_count, num_symbols)
                  VALUES(?,?,?,?) '''
        try:
            cur = self.conn.cursor()
            cur.execute(sql, av_record)
        except Error as e:
            print(e)
        return cur.lastrowid

    def insert_stock(self, stock_record):
        """
        Insert a new stock info into the holdings_record table
        """
        sql = ''' INSERT INTO holdings_record(symbol, num_holdings, price, avg_cost, date)
                  VALUES(?,?,?,?,?) '''
        try:
            cur = self.conn.cursor()
            cur.execute(sql, stock_record)
        except Error as e:
            print(e)            
        return cur.lastrowid

    def insert_order(self, order):
        """
        Insert a new order info into the orders table
        """
        sql = ''' INSERT INTO orders_record(symbol, num, price, date)
                  VALUES(?,?,?,?) '''
        try:
            cur = self.conn.cursor()
            cur.execute(sql, (order.symbol, order.num, order.price, order.date))
        except Error as e:
            print(e)            
        return cur.lastrowid
            
    def update_tables(self, date=''):
        """
        update asset_values_record and holdings_record tables based on edited holdings
        """
        if not date:
            date = self.opt.date#self.current_date
        
        # get current holdings
        self.get_current_holdings()
        
        # update asset value record
        total_assets = 0
        for symbol, stock in self.holdings.items():
            total_assets += stock.price * stock.num_holdings
        
        # get number of orders
        sql = '''SELECT * FROM orders_record'''
        try:
            cur = self.conn.cursor()
            cur.execute(sql)
            temp_orders = cur.fetchall()
        except Error as e:
            print(e)        
        num_orders = len(temp_orders)
        
        # get number of symbols
        num_symbols = len(self.holdings.keys())-1
        
        self.insert_asset_value((date, total_assets, num_orders, num_symbols))   

        # update holdings record
        for symbol, stock in self.holdings.items():      
            self.insert_stock((stock.symbol, stock.num_holdings, stock.price, stock.avg_cost, stock.date))
            
    def get_current_holdings(self):
        """
        obtain current holdings from the database
        """
#        sql = '''select * from holdings_record where date=(select max(date) from holdings_record)'''
        sql = '''SELECT * FROM current_holdings'''
        try:
            cur = self.conn.cursor()
            cur.execute(sql)
            temp_holdings = cur.fetchall()
        except Error as e:
            print(e)
            
        self.holdings = {}
        latest_price = self.stock_mat.loc[:self.opt.date].iloc[-1,:]
        
        for stock in temp_holdings:
            # skip if the number of holdings is 0
            if stock[1] == 0:
                continue

            # update price    
            price = stock[2]
            if stock[0] != 'cash':             
                price = latest_price.loc[stock[0]]
            self.holdings[stock[0]] = StockInfo(stock[0], stock[1], price, stock[3], self.opt.date)
            
        # update database
        cur.execute('delete from current_holdings')
        for symbol, stock in self.holdings.items():
            self.update_single_current_holding(stock)   

        return 1
    
    def update_single_current_holding(self, stock):
        """
        update or insert one entry of the current_holdings table
        """
        sql = '''INSERT OR REPLACE INTO current_holdings (symbol, num_holdings, price, avg_cost, date) 
                    VALUES (?,?,?,?,?)'''
        try:
            cur = self.conn.cursor()
            cur.execute(sql, (stock.symbol, stock.num_holdings, stock.price, stock.avg_cost, stock.date))
        except Error as e:
            print(e)
        return cur.lastrowid
    
    def update_holdings(self):
        """
        update holdings based on received orders
        """
        self.get_current_holdings()
        trade_buffer = self.opt.trade_buffer    
        
        for order in self.orders:
            if order.symbol in self.holdings.keys():
                if order.symbol == 'cash':
                    self.holdings[order.symbol].price += order.price
                    self.holdings[order.symbol].date = order.date
                    self.holdings[order.symbol].avg_cost += self.holdings[order.symbol].price
                else:
                    self.holdings[order.symbol].price = order.price                
                    avg_cost = self.holdings[order.symbol].avg_cost
                    num_holdings = self.holdings[order.symbol].num_holdings
                    if order.num >= 0: 
                        # if buy, update average cost; notice that if sell, avg_cost dosen't change 
                        avg_cost = (avg_cost * num_holdings + order.price * order.num) / (num_holdings + order.num)
                        self.holdings[order.symbol].avg_cost = avg_cost
                        trade_buffer = self.opt.trade_buffer
                    else:
                        trade_buffer = self.opt.trade_buffer * (-1)
                        
                    # update number of holdings
                    self.holdings[order.symbol].num_holdings += order.num
                    assert(self.holdings[order.symbol].num_holdings >= 0)
                    self.holdings[order.symbol].date = order.date
            else:
                # new stock holding
                assert(order.num >= 0)
                self.holdings[order.symbol] = StockInfo(order.symbol, order.num, order.price, order.price, order.date)
 
            # update cash
            if order.symbol != 'cash':
                self.holdings['cash'].price += (order.price * (1 + trade_buffer)) * (order.num * -1)
                
            # insert order into orders_record database    
            self.insert_order(order)
            
        for symbol, stock in self.holdings.items():
            self.update_single_current_holding(stock)

        #clear orders
        self.orders = []
        print('Holdings are updated')
        return 1
    
if __name__ == '__main__':      
    hs = Holdings('test_db.db')  
    hs.create_tables()   
    
    cur = hs.conn.cursor()
    cur.execute('delete from holdings_record')
    cur.execute('delete from asset_values_record')
    cur.execute('delete from current_holdings')
    cur.execute('delete from orders_record')
    
    hs.orders.append(Order('cash', 1, 100000.0, '2019-01-01'))
    hs.update_holdings()

    cur.execute('select * from current_holdings')
    print('0', cur.fetchall())
    
    hs.orders = []
    hs.orders.append(Order('NVDA', 10, 160.0, '2019-01-01'))
    hs.orders.append(Order('MSFT', 3, 120.0, '2019-01-01'))
    hs.orders.append(Order('APPL', 5, 90.0, '2019-01-01'))
    
    hs.update_holdings()

    cur.execute('select * from current_holdings')
    print('1', cur.fetchall())

    cur.execute('select * from orders_record')
    print('2', cur.fetchall())

    hs.orders = []
    hs.orders.append(Order('NVDA', 1, 180.0, '2019-01-02'))    
    hs.orders.append(Order('APPL', -2, 100.0, '2019-01-02'))
    
    hs.update_holdings()
    hs.update_tables()   

    cur.execute('select * from current_holdings')
    print('3', cur.fetchall())

    cur.execute('select * from orders_record')
    print('4', cur.fetchall())
    
    cur.execute('select * from holdings_record')
    print('5', cur.fetchall())
    
    cur.execute('select * from asset_values_record')
    print('6', cur.fetchall())
    
    cur.execute('''select * from holdings_record where date=(select max(date) from holdings_record)''')
    print('7', cur.fetchall())
    
    
    hs.get_current_holdings()
    for symbol, stock in hs.holdings.items():
        print(stock.symbol, stock.num_holdings, stock.price, stock.avg_cost, stock.date)
    
hs.conn.close()



def get_record(self):
        """
        get the asset values record of our own trading strategy
        """
        self.asset_values = []
        cur = self.holdings.conn.cursor()
        cur.execute('select * from asset_values_record')
        temp_asset_values = cur.fetchall()
        for av in temp_asset_values:
          self.asset_values.append(av[1])
