import pyRofex
import math
from orders_table import OrdersTable
import time
import threading




class Carrier():
    def __init__(self, broker, strategy=None):
        """
        Initialize the Carrier object.

        Args:
        - broker: The broker object.
        - strategy: Optional strategy object.

        """
        self.broker = broker
        self._strategy = strategy
        self.orders_table = OrdersTable(strategy)
        self._was_order_complete = False

    @property
    def strategy(self):
        return self._strategy

    @strategy.setter
    def strategy(self, value):
        self._strategy = value

    
    def run_strategy(self):
        self.strategy.run_strategy()

    def get_market_data(self, instruments, formated=False) -> list:
        """
        Get market data for the specified instruments.

        Args:
        - instruments: List of instruments for which to retrieve market data.

        Returns:
        - List of market data entries (bids, offers, last price) for each instrument.

        """
        if not formated:
            self.format_instruments(instruments)
        entries = [pyRofex.MarketDataEntry.BIDS, pyRofex.MarketDataEntry.OFFERS, pyRofex.MarketDataEntry.LAST]

        return [pyRofex.get_market_data(ticker=instrument, entries=entries, depth=2) for instrument in instruments]
    

    # TODO mejorar
    def send_orders(self, orders: dict):
        size = orders[0].get("Size")
        symbol = orders[0].get("Symbol")
        response = None


        for order in orders:
            clearing = order.get("Clearing")
            side = order.get("Side")
            price_with_costs = order.get("Price_with_costs")[0]

            if side == "buy":
                price = order.get("Offer")
                order_side = pyRofex.Side.BUY

                available_to_afford = math.floor(self.broker.budget / price_with_costs)
                size = min(size, available_to_afford)

                cost_of_operation = price_with_costs * size
                if size == 0:
                    print(f"You need money to complete this operation, you have ${self.broker.budget} and it costs at least ${price_with_costs} ")
                    break

                self.broker.budget -= cost_of_operation

            else:
                price = order.get("Bid")
                order_side = pyRofex.Side.SELL

            formatted_symbol = self.format_instruments(symbol, clearing)
            response = pyRofex.send_order(
                ticker=formatted_symbol,
                side=order_side,
                size=size,
                price=price,
                order_type=pyRofex.OrderType.LIMIT
            )["order"]["clientId"]


            order["Size"] = size
            if response is not None:
                try:
                    # ver si no deberias poner un time sleep para que se ejecute la orden
                    # time.sleep(1)
                    order_status = self.order_status(response)
                    # self.wb_disconnect()
                    was_operated = order_status["order"]["text"] == "Operada "

                    if was_operated == False:
                        self.cancel_order(response)
                        break
                    else:
                        threading.Thread(target=self.orders_table.save_row, args=(order,)).start()
                except Exception as e:
                    print(e)


            # order["Total_cost_of_operation"] = cost_of_operation
            



    def send_orders_wb(self, orders:dict):
        size = orders[0].get("Size")
        symbol = orders[0].get("Symbol")

        for order in orders:
            clearing = order.get("Clearing")
            side = order.get("Side")
            price_with_costs = order.get("Price_with_costs")[0]

            if side == "buy":
                price = order.get("Offer")
                order_side = pyRofex.Side.BUY

                available_to_afford = math.floor(self.broker.budget / price_with_costs)
                size = min(size, available_to_afford)

                cost_of_operation = price_with_costs * size
                if size == 0:
                    print(f"You need money to complete this operation, you have ${self.broker.budget} and it costs at least ${price_with_costs} ")
                    break

                self.broker.budget -= cost_of_operation

            else:
                price = order.get("Bid")
                order_side = pyRofex.Side.SELL

            formatted_symbol = self.format_instruments(symbol, clearing)
            pyRofex.send_order_via_websocket(
                ticker=formatted_symbol,
                side=order_side,
                size=size,
                price=price,
                order_type=pyRofex.OrderType.LIMIT,
            )

            order["Size"] = size
            # is_complete = self.await_for_order_complete()
            is_complete = True
            if is_complete:
                threading.Thread(target=self.orders_table.save_row, args=(order,)).start()
                self._was_order_complete= False
            else:
                break

    def await_for_order_complete(self):
        time.sleep(0.1)
        if self._was_order_complete:
            return True
        else:
            return False



    def get_account_report(self) -> dict:
        """Retrieves the account report.
        Returns
        =============================================================
        - report: The account report obtained from the pyRofex library.
        """
        return pyRofex.get_account_report(account=self.broker.account)

    
    def cancel_order(self, client_id : str) -> dict:
        """Cancels an order associated with the given client ID.

        Parameters
        =============================================================
        - client_id [str]: The client ID of the order to be canceled.

        Returns
        =============================================================
        - response: The response indicating the status of the cancellation request.
        """
        return pyRofex.cancel_order(client_id)


    def order_status(self, client_id: str) -> dict:
        """Retrieves the status of an order associated with the given client ID.

        Parameters
        =============================================================
        - client_id [str]: The client ID of the order for which the status is requested.

        Returns
        =============================================================
        - status [dict]: The status of the order obtained from the pyRofex library.
        """
        return pyRofex.get_order_status(client_id)


    @staticmethod
    def format_instruments(ticket_or_tickets: list|str, liq : str = 'CI') -> list | str:    
        """Formats a instrument to be valid in the requests.

        Parameters
        =============================================================
        - ticket_or_tickets [list or str]: A list of simple tickers or a single ticker to be formatted. If a list is provided, each ticker will be formatted individually. If a string is provided, it will be treated as a single ticker.
        - liq [str]: The settlement term of the operation. Can be either "48hs" or "CI" (Cash Immediate).

        Returns
        =============================================================
        - ticker_list [list] or [str]: If a list of tickers is provided, returns a list of formatted tickers. If a single ticker is  provided, returns the formatted ticker as a string.
        """

        def set_ticker(ticket, liq):
            
            ticket = f'MERV - XMEV - {ticket} - {liq}'
            return ticket

        if isinstance(ticket_or_tickets, list):
            ticker_list = []
            for ticket in ticket_or_tickets:
                ticker_list.append(set_ticker(ticket, liq))
            return ticker_list
        else:
            ticket = set_ticker(ticket_or_tickets, liq)
            return ticket




    def _order_report_handler(self, message:dict)-> None:
        # print(f"\nMensaje de OrderRouting: {message}")
        origin = message.get('orderReport').get('originatingUsername')
        # print(origin)
        if origin == "PBCP":
            text = message['orderReport']['text'].strip()
            if text != "Operada":
                # print("no esta operado")
                client_order_id = message['orderReport']['clOrdId']
                pyRofex.cancel_order_via_websocket(client_order_id=client_order_id)
                self._was_order_complete = False
            self._was_order_complete = True
                
    def get_detailed_position(self):
        return pyRofex.get_detailed_position(self.broker.account)

    def _error_handler(self, message) -> None:
        """
        Handle errors by printing an error message.

        Args:
        - message: The error message to be printed.

        """
        print(f"Mensaje de error: {message}")

    def _exception_error(self, message) -> None:
        """
        Handle exceptions by printing an exception message.

        Args:
        - message: The exception message to be printed.

        """

        print(f"Mensaje de excepción: {message}")

    def conect_wb(self) -> None:
        """
        Establish a WebSocket connection.
    
        """
        pyRofex.init_websocket_connection(error_handler=self._error_handler)
    

    def market_data_subscription(self, instruments_to_subscription, entries, handler, depth=1)-> None:
        """
        Subscribe to market data for specified instruments.

        Args:
        - instruments_to_subscription: List of instruments to subscribe to.

        """
        # Define the types of market data to subscribe to (bids, offers)


        # Subscribe to market data
        pyRofex.market_data_subscription(
            instruments_to_subscription,
            entries,
            depth=depth,
            handler=handler
        )


    def order_report_subscription(self)-> None:
        """
        Subscribe to order reports.

        """
        # Subscribe to order reports
        pyRofex.order_report_subscription(
            self.broker.credentials.get("account"),
            snapshot=True,
            handler=self._order_report_handler
        )


    def wb_disconnect(self)-> None:
        """
        Disconnect from the WebSocket connection.

        """
        # Close the WebSocket connection
        pyRofex.close_websocket_connection()











