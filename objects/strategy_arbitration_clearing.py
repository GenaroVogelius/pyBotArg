from abc import ABC, abstractmethod
import pandas as pd 
import pyRofex

class BaseStrategy(ABC):
    """
    Abstract base class for defining trading strategies.

    This class defines the interface for trading strategies, including methods
    for running the strategy and creating a DataFrame.

    """

    @abstractmethod
    def run_strategy(self):
        """
        Abstract method for running the trading strategy.

        This method should be implemented by subclasses to define the logic
        for running the trading strategy based on new data.

        """
        pass

    @abstractmethod
    def create_df(self):
        """
        Abstract method for creating a DataFrame.

        This method should be implemented by subclasses to create a DataFrame
        with the necessary structure for storing trading data.

        """
        pass



class StrategyArbitrationOfClearing(BaseStrategy):
    """
    Implementation of a strategy arbitrage of clearing.

    Args:
    - carrier: The carrier object associated with the strategy.
    - tna_expected: The expected TNA value. Defaults to 110.
    - ticket_to_subscription: Tickets to subscribe to watch in the web socket.

    """
    def __init__(self, carrier : object, ticket_to_subscription: list = None, tna_expected: int = 110):
        super().__init__()
        self.tna_expected = tna_expected
        self.carrier = carrier
        self.not_value = 0.0
        self.costs = carrier.broker.costs
        self.main_df = self.create_df()
        self.ticket_to_subscription = ticket_to_subscription



    def create_df(self)-> pd.DataFrame:
        """
        Create a DataFrame with predefined columns.

        This method creates a DataFrame with predefined columns for storing trading data.

        Returns:
        - A pandas DataFrame with predefined columns.

        """
        column_symbol= 'Symbol'
        column_clearing= 'Clearing'
        column_bid= 'Bid'
        column_offer = 'Offer'
        column_size= "Size"
        column_price_with_costs = "Price_with_costs"
        columns=[column_symbol, column_clearing, column_bid, column_offer, column_size, column_price_with_costs]
        return pd.DataFrame(columns=columns)


    def format_tickets(self):
        tickets_24 = self.carrier.format_instruments(self.ticket_to_subscription, liq="24hs")
        tickets_ci = self.carrier.format_instruments(self.ticket_to_subscription, liq="CI")

        instruments_to_subscription= []
        instruments_to_subscription = tickets_24
        instruments_to_subscription.extend(tickets_ci)
        return instruments_to_subscription




    def run_strategy(self):
        """
        Some preparative for running the strategy correctly
        In this case the main importance is in handle incoming messages which has all the logic for this strategy.
        """
        instruments_to_subscription = self.format_tickets()
        self.carrier.conect_wb()
        entries = [
            pyRofex.MarketDataEntry.BIDS,
            pyRofex.MarketDataEntry.OFFERS,
        ]
        self.carrier.market_data_subscription(instruments_to_subscription, entries=entries, handler=self.handle_incoming_messages, depth=1)


    def handle_incoming_messages(self, new_data:dict):
        print(new_data)
        data_manipulated = self.manipulate_data(new_data)
        symbol = data_manipulated["Symbol"]


        is_new_symbol, index = self._is_new_symbol(data_manipulated)
        if is_new_symbol:
            self.add_data_to_df(data_manipulated, index)

        else:
            is_new_clearing = self.is_new_clearing(data_manipulated)

            if is_new_clearing[0]:
                self.place_data_bellow_existing_ticket(data_manipulated, index+1)
                self.start_rows_calculations(symbol)


            else:
                # todo 
                # self.Carrier.cancel_order(symbol)
                # Means that the symbol and clearing already exist in the df

                # has_same_price_as_before? bid o offer
                # va a comparar la data con las rows_with_symbol, se va a fijar si la bid o la offer es distinta, si es distinta que agrega la data al df, sino que no haga nada ya que significa que solo se cambio el size
                clearing = data_manipulated["Clearing"]
                if clearing == "CI":
                    price = data_manipulated["Offer"]
                else:
                    price = data_manipulated["Bid"]
                index = is_new_clearing[1]
                has_same_price = self._has_same_price_as_before(price, index)
                if not has_same_price:
                    self.add_data_to_df(data_manipulated, index)
                    self.start_rows_calculations(symbol)




    def manipulate_data(self, new_data:dict) -> dict:
        """
        Manipulate market data received from the Carrier according to the strategy and dataframe.

        Parameters:
        - new_data (dict): A dictionary containing market data received from the Carrier.
        
        Example:
        {
            'type': 'Md',
            'timestamp': 1710873570879,
            'instrumentId': {
                'marketId': 'ROFX',
                'symbol': 'MERV - XMEV - ALUA - CI'
                },
            'marketData': {'BI': [], 'OF': []
            }
        }

        Returns:
        - dict: A dictionary containing manipulated data based on the received market data.

        Example:
        {
            "Symbol": "ALUA",
            "Clearing": "CI",
            "Bid": None,
            "Offer": None
        }

        Explanation:
        This function extracts symbol and clearing information from the received market data
        and manipulates it according to the strategy and dataframe requirements. It then
        returns a dictionary with the manipulated data, including the symbol, clearing,
        bid, and offer values. If bid and offer values are not available, they are set to None.
        """


        symbol_and_clearing = new_data["instrumentId"]["symbol"]
        symbol_and_clearing = symbol_and_clearing.split(' - ')[-2:]
        symbol = symbol_and_clearing[0]
        clearing = symbol_and_clearing[1]


        offer = self.not_value
        bid = self.not_value
        size = 0

        if clearing == "CI":
            try:
                offer = new_data.get("marketData", {}).get("OF", [{}])[0].get("price")
                size = new_data.get("marketData", {}).get("OF", [{}])[0].get("size")
            except (IndexError, TypeError):
                pass
        elif clearing == "48hs":
            try:
                bid = new_data.get("marketData", {}).get("BI", [{}])[0].get("price")
                size = new_data.get("marketData", {}).get("BI", [{}])[0].get("size")
            except (IndexError, TypeError):
                pass


            # offer = new_data.get("marketData").get("OF", self.not_value)[0].get("price", self.not_value)
            # size = new_data.get("marketData").get("OF", self.not_value)[0].get("size", self.not_value)



        return {'Symbol':symbol, "Clearing":clearing, "Bid": bid, "Offer" : offer, "Size": size, "Price_with_costs": (0, False)}

    def _is_new_symbol(self, data_manipulated: dict) -> tuple[bool, int]:
        """
        Checks if there is another equal ticket in some row of the DataFrame.

        Args:
        - data_manipulated (dict): The data to be checked, containing the symbol of the ticket.

        Returns:
        - Tuple[bool, int]: A tuple containing a boolean indicating whether the ticket is new or not, and the index of the existing ticket if it exists. If no existing ticket is found, it returns True and the last row index of the DataFrame.
        """

        ticket_symbol = data_manipulated.get("Symbol")
        index = self.main_df.index[self.main_df["Symbol"] == ticket_symbol]

        
        if not index.empty:
            # If the index is not empty, means the ticket exists
            return False, index[0]
        else:
            # If the index is empty, means the ticket doesn't exist
            last_row_index = len(self.main_df) 
            return True, last_row_index


    def place_data_bellow_existing_ticket(self, data_to_add: dict, index: int):
        # If it's a new clearing, insert the data at the specified index and move other rows below
        new_row = pd.DataFrame([data_to_add])
        new_row = new_row.fillna(self.not_value)
        self.main_df = pd.concat([self.main_df.iloc[:index], new_row, self.main_df.iloc[index:]]).reset_index(drop=True)

    def add_data_to_df(self, data_to_add: dict, index: int):
        """
        Adds data to the DataFrame at the specified index.

        Args:
        - data_to_add (dict): The data to be added to the DataFrame.
        - index (int): The index where the data should be added.
        - is_new_clearing (bool, optional): Indicates whether it's a new clearing or not.
        If True, the data is inserted at the specified index and moves the other rows below.
        If False, the data is replaced at the specified index.

        Returns:
        - None
        """

        self.main_df.loc[index] = data_to_add


    
    def is_new_clearing(self, data_manipulated: dict) -> bool | tuple:
        """
        Checks if the clearing of the given row is different from the new data.

        Args:
        - data_manipulated (dict): The new data to be checked.
        - index (int): The index of the row in the DataFrame to be compared.

        Returns:
        - bool: True if the clearing of the specified row is different from the new data, False otherwise.
        - tuple: A tuple containing the result (True or False) and the index of the row if found.
        """
        clearing = data_manipulated.get("Clearing")
        symbol = data_manipulated.get("Symbol")

        # Check if there exists at least one row with symbol name and same clearing
        rows_with_clearing = self.main_df[(self.main_df['Symbol'] == symbol) & (self.main_df['Clearing'] == clearing)]
        
        if not rows_with_clearing.empty:
            # If at least one row with the same symbol and clearing is found, return False and the index
            return False, rows_with_clearing.index[0]
        else:
            # If no matching rows are found, return True
            return True,
    
    def _has_same_price_as_before(self, price, index):

        return price in self.main_df.iloc[index].tolist()


    def start_rows_calculations(self, symbol):
        rows_with_symbol_df = self.get_symbol_rows(symbol)
        # if not (rows_with_symbol_df["Bid"] == self.main_df["Bid"])
        if not (rows_with_symbol_df['Size'] == 0).any():
            tna = self.calculate_tna(rows_with_symbol_df)
            print("TNA: ", tna, symbol)
            rows_with_symbol_df = self.get_symbol_rows(symbol)
            rows_with_symbol_df = self.persist_tna_in_df(rows_with_symbol_df, tna)
            if tna >= self.tna_expected:
                self.prepare_orders(rows_with_symbol_df)
            else:
                return ("No se mando orden por no tener TNA requerida")   


    def calculate_tna(self, rows_with_symbol_df) -> float:
        """
        Calculate TNA (Total Net Assets) for the given symbol.

        Args:
        - symbol (str): The symbol for which TNA is to be calculated.

        Returns:
        - Tuple[float, List[dict]]: A tuple containing the calculated TNA and a list of dictionaries representing the rows with the specified symbol.
        """

        row_48 = rows_with_symbol_df.loc[rows_with_symbol_df['Clearing'] == '48hs']
        row_ci = rows_with_symbol_df.loc[rows_with_symbol_df['Clearing'] == 'CI']

        bid_48 = row_48["Bid"].values[0], row_48["Price_with_costs"].values[0]
        offer_ci = row_ci["Offer"].values[0], row_ci["Price_with_costs"].values[0]


        bid_48_value = bid_48[0]
        offer_ci_value = offer_ci[0]


        if False in bid_48[1]:
            bid_48_value  = self.add_costs({"Sell": bid_48_value})
            index_row_48 = row_48.index[0]

            self.main_df.at[index_row_48, "Price_with_costs"] = (bid_48_value, True)
        if False in offer_ci[1]:
            offer_ci_value = self.add_costs({"Buy": offer_ci_value})
            index_row_ci = row_ci.index[0]

            
            self.main_df.at[index_row_ci, "Price_with_costs"] = (offer_ci_value, True)


        tna = round((((bid_48_value / offer_ci_value - 1) / 2) * 365) * 100, 2)

        return tna    
    
    def persist_tna_in_df(self, rows_with_symbol_df, tna):
        
        # rows_with_symbol_df["TNA"] = tna
        rows_with_symbol_df.loc[:, "TNA"] = tna
        
        return rows_with_symbol_df

    def _clean_rows_in_df(self, rows_with_symbol_df):
        indexes = rows_with_symbol_df.index.to_list()
        # this is not necessary
        # self.main_df.loc[indexes, self.column_bid] = self.not_value
        # self.main_df.loc[indexes, self.column_offer] = self.not_value
        self.main_df.loc[indexes, "Size"] = self.not_value

    def _format_order(self, rows_with_symbol_dict):

        rows_with_symbol_dict = sorted(rows_with_symbol_dict, key=lambda dictionary: dictionary['Clearing'] != 'CI')

        order_buy = rows_with_symbol_dict[0]
        order_sell = rows_with_symbol_dict[1]

        order_buy["Side"] = "buy"
        order_sell["Side"] = "sell"

        return order_buy, order_sell
    
    def get_symbol_rows(self, symbol):
        return self.main_df[self.main_df["Symbol"] == symbol].copy()

    def transform_rows_into_dict(self, rows_with_symbol_df):
        rows_with_symbol_dict = rows_with_symbol_df.to_dict(orient='records')
        return rows_with_symbol_dict

    def determine_size_order(self, rows_with_symbol_df):
        min_size = rows_with_symbol_df['Size'].min()
        rows_with_symbol_df['Size'] = min_size
        return rows_with_symbol_df

    def prepare_orders(self, rows_with_symbol_df):
        rows_with_symbol_df = self.determine_size_order(rows_with_symbol_df)
        rows_with_symbol_dict =  self.transform_rows_into_dict(rows_with_symbol_df)
        # self._clean_rows_in_df(rows_with_symbol_df)
        order_buy, order_sell = self._format_order(rows_with_symbol_dict)
        self.carrier.send_orders_wb([order_buy, order_sell])
        return ("La TNA cumple con los requerimientos")

    def add_costs(self, dict_price: dict):
        """
        Returns the price of the bid and offer with market right and comissions costs added.
        """

        
        if "Sell" in dict_price:
            value = dict_price["Sell"]

            costs = value * self.costs / 100
            iva = costs * 21 / 100
            value = value-costs-iva
            
            return round(value, 2)
        else:
            value = dict_price["Buy"]
            costs = value * self.costs / 100
            iva = costs * 21 / 100
            value = value+costs+iva
            return round(value, 2)


            

        # vas a agarrar las 2 rows
        # al offer le sumas la comision
        # al bid le restas la comision
        # lo guardas en el df original y pones una columna llamada has_cost_calculated, y se lo pones en True
        # recien ahi calculas la tna
        # ver como relacionas al broker con la estrategia

    def format_df_of_orders(self, order):

        # orders_df.drop(columns=['Bid', 'Offer'], inplace=True)
        del order['Bid'], order["Offer"]
        order["Price_with_costs"] = order["Price_with_costs"][0]
        return order

