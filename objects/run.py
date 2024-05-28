
import os
from broker import Broker
from carrier import Carrier
from strategy_arbitration_clearing import StrategyArbitrationOfClearing
from dotenv import load_dotenv
load_dotenv()

prod_env=False

if prod_env:
    credentials = {"account" : os.environ.get('ACCOUNT'), "user" : os.environ.get('USER'), "password": os.environ.get('PASSWORD')}
    tickers_list = [
    "BBAR",
    "BMA",
    "BYMA",
    "COME",
    "CRES",
    "EDN",
    "GGAL",
    "IRSA",
    "INTR",
    "PAMP",
    "SUPV",
    "TECO2",
    "TGNO4",
    "TGSU2",
    "YPFD",
    "INVJ",
    "CARC",
    "BOLT",
    "BHIP",
    "BPAT",
    "CRE3W",
    ""
    ]
    budget=20000
else:
    credentials = {"account" : os.environ.get('ACCOUNTTEST'), "user" : os.environ.get('USERTEST'), "password": os.environ.get('PASSWORDTEST')}
    tickers_list = [
    "BMA",
    "BYMA",
    "CEPU",
    "COME",
    "CRES",
    "GGAL",
    ]
    budget=200000000

broker = Broker(credentials=credentials, budget=budget, prod_env=prod_env)
carrier = Carrier(broker)



strategy = StrategyArbitrationOfClearing(carrier, ticket_to_subscription=tickers_list, tna_expected=90)

carrier.strategy = strategy




# # * Get the instruments 48 and CI
# tickets_48 = carrier.format_instruments(tickers_list, liq="24hs")
# tickets_ci = carrier.format_instruments(tickers_list, liq="CI")

# instruments_to_subscription = tickets_48
# instruments_to_subscription.extend(tickets_ci)

if __name__ == '__main__':
    carrier.run_strategy()

    while True:
        try:
            choice = int(input("Choose a command:\n1)See dataframe with market data \n2)See dataframe with orders sended \n3)See current budget \n4)Disconnect web socket "))
            if choice == 1:
                print(carrier.strategy.main_df)
            elif choice == 2:
                print(carrier.orders_table.orders_df)
            elif choice == 3:
                print("Te sobran: $",broker.budget)
            elif choice == 4:
                carrier.wb_disconnect()
                print("Te sobran: $",broker.budget)
                carrier.orders_table.create_excel()
                break
            else:
                print("Invalid input, try again")
        except Exception as e:
            print(e)
