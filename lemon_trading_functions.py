import os
import json
from keys_config import *
from lemon import api

from helpers import EmailSenderSendgrid

class TradingVenue():

    def __init__(self):
        self.client = api.create(
            market_data_api_token=LEMON_MARKET_DATA_KEY,
            trading_api_token=LEMON_PAPER_TRADING_KEY,
            env='paper'  # or env='live'
    )

    def send_out_email(self):
        response = self.client.trading.positions.get()
        email_text = json.dumps(response)
        subject = "Your positions at market close"
        EmailSenderSendgrid(email_text, subject)

    def place_order_with_email(self):
        venue = self.client.market_data.venues.get(mic=LEMON_EXCHANGE_MIC).results[0]
        if not venue.is_open:
            email_text = "Hey there. You tried to place an order with the lemon.markets API, but the market is " \
                         "currently closed. Please try again later."
            subject = "Trade failed: the market is currently closed."
            EmailSenderSendgrid(email_text, subject)
            return

        try:
            isin = "DE0008232125"  # ISIN of Lufthansa
            expires_at = 7  # specify your timestamp
            side = "buy"
            quantity = 1

            price = self.client.market_data.quotes.get_latest(isin).results[0].b
            if quantity * price < 50:
                print(f"Order price is, €{price}, which is below minimum allowed of €50.")
                return

            response = self.client.trading.orders.create(isin=isin,
                                                         quantity=quantity,
                                                         side=side,
                                                         expires_at=expires_at,
                                                         venue=LEMON_EXCHANGE_MIC)
            order_id = response.results.id
            self.client.trading.orders.activate(order_id=order_id)
            print('Order was activated')
            self.send_out_email()
        except Exception as e:
            print('Placing order not possible:', e)


if __name__ == "__main__":
    TradingVenue().place_order_with_email()