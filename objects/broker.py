from typing import Tuple, List, Dict
import pyRofex
import sys

class Broker:
    """
    Represents your broker responsible for managing trades in matriz.
    
    Attributes:
        budget (float): The budget that you want to be operable for trading.
        comission (float): The commission rate percentage applied to each trade.
        market_right (float): The market right percentage.
        credentials (dict): The credentials required for initializing the trading platform.
    """

    def __init__(
        self, credentials: Dict[str, str], budget: float = 100, comission: float = 0.15, market_right: float = 0.08, prod_env=False
    ) -> None:
        """
        Initializes a new instance of the Broker class.
        
        Args:
            credentials (dict): The credentials required for initializing the trading platform.
            budget (float, optional): The initial budget available for trading. Defaults to 100.
            comission (float, optional): The commission rate applied to each trade. Defaults to 0.5.
            market_right (float, optional): The market right percentage. Defaults to 0.08.
        """
        self._budget = budget
        self.comission = comission
        self.market_right = market_right
        self._costs = None
        self.credentials = credentials
        environment = pyRofex.Environment.REMARKET

        if prod_env:
            self.security_measure()
            pyRofex._set_environment_parameter("url", "https://api.veta.xoms.com.ar/", pyRofex.Environment.LIVE)
            pyRofex._set_environment_parameter("ws", "wss://api.veta.xoms.com.ar/", pyRofex.Environment.LIVE)
            environment = pyRofex.Environment.LIVE


        # Log in in Matriz
        if credentials != None:
            pyRofex.initialize(
                user=self.credentials.get("user"),
                password=self.credentials.get("password"),
                account=self.credentials.get("account"),
                environment=environment,
                active_token=None
            )



    @property
    def costs(self):
        if self._costs is None:
            self._costs = self.comission + self.market_right
            return self._costs
        else:
            return self._costs

    @costs.setter
    def costs(self, value):
        self._costs = value

    @property
    def budget(self):
        if self._budget <= 0:
            raise ValueError("Budget has to be greater than zero")
        else:
            return self._budget

    @budget.setter
    def budget(self, value):
        self._budget = value

    def security_measure(self):
        result = int(input("Are you sure to connect to the production environment? Press 1 if you are sure "))
        if result != 1:
            sys.exit()
