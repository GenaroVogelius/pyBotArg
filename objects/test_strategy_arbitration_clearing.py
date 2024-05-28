import pandas as pd
from carrier import Carrier
from strategy_arbitration_clearing import StrategyArbitrationOfClearing
from broker import Broker
import pytest
import numpy as np




class TestStrategyArbitrationOfClearing:


    not_value = 0.0 

    @pytest.fixture
    def strategy_instance(self):
        broker = Broker(credentials=None, budget=999999)
        carrier = Carrier(broker)
        return StrategyArbitrationOfClearing(carrier)

    @pytest.mark.parametrize(
        "new_data, expected_result",
        [
            (   
                {'type': 'Md', 'timestamp': 1710873570879, 'instrumentId': {'marketId': 'ROFX', 'symbol': 'MERV - XMEV - ALUA - CI'}, 'marketData': {'BI': [], 'LA': {'price': 23049.0, 'size': 69, 'date': 1710872944016}, 'OF': [{'price': 84990.0, 'size': 46}]}},
                {'Symbol': 'ALUA', 'Clearing': 'CI', 'Bid': 0.0, 'Offer': 84990.0, 'Size': 46, 'Price_with_costs': (0, False)}
            ),
            (   
                {'type': 'Md', 'timestamp': 1710873570879, 'instrumentId': {'marketId': 'ROFX', 'symbol': 'MERV - XMEV - ALUA - CI'}, 'marketData': {'BI': [], 'LA': {'price': 23049.0, 'size': 69, 'date': 1710872944016}, 'OF': []}},
                {'Symbol': 'ALUA', 'Clearing': 'CI', 'Bid': 0.0, 'Offer': 0.0, 'Size': 0, 'Price_with_costs': (0, False)}
            ),
            (   
                {'type': 'Md', 'timestamp': 1710873570879, 'instrumentId': {'marketId': 'ROFX', 'symbol': 'MERV - XMEV - ALUA - 48hs'}, 'marketData': {'BI': [{'price': 84990.0, 'size': 46}], 'LA': {'price': 23049.0, 'size': 69, 'date': 1710872944016}, 'OF': []}},
                {'Symbol': 'ALUA', 'Clearing': '48hs', 'Bid': 84990.0, 'Offer': 0.0, 'Size': 46, 'Price_with_costs': (0, False)}
            ),
            (   
                {'type': 'Md', 'timestamp': 1710873570879, 'instrumentId': {'marketId': 'ROFX', 'symbol': 'MERV - XMEV - ALUA - 48hs'}, 'marketData': {'BI': [], 'LA': {'price': 23049.0, 'size': 69, 'date': 1710872944016}, 'OF': []}},
                {'Symbol': 'ALUA', 'Clearing': '48hs', 'Bid': 0.0, 'Offer': 0.0, 'Size': 0, 'Price_with_costs': (0, False)} 
            ),
        ],
        ids=["Test Case 1: CI with Offer", "Test Case 2: CI without Offer", "Test Case 3: 48hs with Bid", "Test Case 4: 48hs without Bid"],
    )
    def test_manipulate_data(self, strategy_instance, new_data, expected_result):
        """
        Test the manipulate_data method of StrategyArbitrationOfClearing.
        """

        result = strategy_instance.manipulate_data(new_data)
        assert result == expected_result

    @pytest.mark.parametrize(
    "data_manipulated, expected_result, data_for_df",
    [
        (   
            {"Symbol": "ALUA", "Clearing": "48hs", "Bid": None, "Offer": None},
            (True, 0),
            None
        ),
        (   
            {"Symbol": "ALUA", "Clearing": "48hs", "Bid": not_value, "Offer": not_value},
            (False, 0),
            {"Symbol": ["ALUA"], "Clearing": ["48hs"], "Bid": [not_value], "Offer": [not_value]},
        ),
        (   
            {"Symbol": "ALUA", "Clearing": "48hs", "Bid": not_value, "Offer": not_value},
            (False, 2),
            {"Symbol": ["COME", "EDENOR", "ALUA"], "Clearing": ["48hs", "CI", "48hs"], "Bid": [not_value, not_value, not_value], "Offer": [not_value, not_value, not_value]}
        )
    ], ids=["Test Case 1: Symbol does not exist in df without data", "Test Case 2: Symbol exists in row 0 of a df with data", "Test Case 3: Symbol exists in row 3 of a df with data",]
)
    def test__is_new_symbol(self, strategy_instance, data_manipulated, expected_result, data_for_df):
            
            if data_for_df:  # If true populate the dataframe
                strategy_instance.main_df = pd.DataFrame(data_for_df)

            result =  strategy_instance._is_new_symbol(data_manipulated)


            assert result == expected_result


    @pytest.mark.parametrize(
    "data_to_add, index, expected_result, data_for_df",
    [ 
        (   
            {"Symbol": "ALUA", "Clearing": "48hs", "Bid": 10, "Offer": not_value, "Size": 10, "Price_with_costs" :(0, False)},
            0,
            pd.DataFrame({"Symbol": ["ALUA"], "Clearing": ["48hs"], "Bid": [10], "Offer": [not_value], "Size":[10], "Price_with_costs": [(0, False)]}),
            None 
        ),
        (   
            {"Symbol": "COME", "Clearing": "CI", "Bid": 20, "Offer": not_value, "Size": 14},
            1,
            pd.DataFrame({"Symbol": ["ALUA", "COME"], "Clearing": ["48hs", "CI"], "Bid": [10, 20], "Offer": [not_value, not_value], "Size": [14, 14]}),
            {"Symbol": ["ALUA"], "Clearing": ["48hs"], "Bid": [10], "Offer": [not_value], "Size": 14},
        ),
        (   
            {"Symbol": "ALUA", "Clearing": "48hs", "Bid": 15, "Offer": not_value},
            0,
            pd.DataFrame({"Symbol": ["ALUA"], "Clearing": ["48hs"], "Bid": [15], "Offer": [not_value]}),
            {"Symbol": ["ALUA"], "Clearing": ["48hs"], "Bid": [10], "Offer": [not_value]},
            ),
        (   
        {"Symbol": "ALUA", "Clearing": "CI", "Bid": not_value, "Offer": 20},
        1,
        pd.DataFrame({"Symbol": ["ALUA", "ALUA"], "Clearing": ["48hs","CI"], "Bid": [not_value, not_value], "Offer": [not_value, 20]}),
        {"Symbol": ["ALUA", "ALUA"], "Clearing": ["48hs", "CI"], "Bid": [not_value, not_value], "Offer": [not_value, 30]},
        ),       
    ],
    ids=["Test Case 1: add data in a df without data", "Test Case 2: add data in a df with data","Test Case 3: Replacing data in row 0","Test Case 4: Replacing data in row 1"]
)
    def test_add_data_to_dataframe(self, strategy_instance, data_to_add, index, expected_result, data_for_df):
            """
            add new data into the dataframe
            """

            if data_for_df:
                strategy_instance.main_df = pd.DataFrame(data_for_df)
            strategy_instance.add_data_to_df(data_to_add, index)
            
            result =  strategy_instance.main_df



            pd.testing.assert_frame_equal(result, expected_result)


    @pytest.mark.parametrize(
    "data_manipulated, data_for_df, expected_result",
    [

        (   
            {"Symbol": "ALUA", "Clearing": "CI", "Bid": not_value, "Offer": not_value},
            pd.DataFrame({"Symbol": ["ALUA"], "Clearing": ["48hs"], "Bid": [10], "Offer": [not_value]}),
            (True, )

        ),

        (   
            {"Symbol": "ALUA", "Clearing": "48hs", "Bid": 5, "Offer": not_value},
            pd.DataFrame({"Symbol": ["ALUA"], "Clearing": ["48hs"], "Bid": [10], "Offer": [not_value]}),
            (False, 0)
        ),

    ],
    ids=["Test Case 1: Clearing does not exist in ticket row", "Test Case 2: Clearing exists in df"]
    )
    def test_is_new_clearing(self, strategy_instance, data_manipulated, data_for_df, expected_result):

        strategy_instance.main_df = data_for_df
        result =  strategy_instance.is_new_clearing(data_manipulated)
        assert result == expected_result

    @pytest.mark.parametrize(
    "data_to_add, index, expected_result, initial_df",
    [      
        (   
            {"Symbol": "ALUA", "Clearing": "CI", "Bid": not_value, "Offer": 15, "Size": 14},
            1,
            pd.DataFrame({"Symbol": ["ALUA", "ALUA","COME", "COME"], "Clearing": ["48hs", "CI", "CI", "48hs"], "Bid": [10, not_value, np.nan, 15], "Offer": [not_value, 15, 10, not_value], "TNA":[np.nan, np.nan, np.nan, np.nan], "Size": [14, 14, 10, 5], "ClientId": [np.nan,np.nan, np.nan, np.nan]}),
            pd.DataFrame({"Symbol": ["ALUA","COME", "COME"], "Clearing": ["48hs", "CI", "48hs"], "Bid": [10, np.nan, 15], "Offer": [not_value, 10, not_value], "TNA":[np.nan, np.nan, np.nan], "Size": [14, 10, 5], "ClientId": [np.nan, np.nan, np.nan]}),
        ),

    ],
    ids=["Test Case 1: add data in a df with data in index 1"]
)
    def test_place_data_bellow_existing_ticket(self, strategy_instance, data_to_add, index, expected_result, initial_df):
        strategy_instance.main_df = initial_df
        strategy_instance.place_data_bellow_existing_ticket(data_to_add, index)

        pd.testing.assert_frame_equal(strategy_instance.main_df, expected_result)

    @pytest.mark.parametrize(
    "dict_price, expected_result",
        [
            (
                {"Buy": 73220.00},
                73423.77
            ),
            (   
                {"Sell": 1260.5},
                1256.99,
            ),
            (   
                {"Buy": 1260.5},
                1264.01,
            ),

        ]
    ,ids=["Test Case 1: adding comission, iva and market right to a buy","Test Case 2: adding comissions, iva, and market right to a sell", "Test Case 3: adding comissions, iva and market right to a buy"])
    def test_add_costs(self, strategy_instance, dict_price, expected_result):
        result = strategy_instance.add_costs(dict_price)

        assert result == expected_result

    @pytest.mark.parametrize(
    "rows_with_symbol_df, expected_result_tna, expected_df",
        [
            (   
                pd.DataFrame(
                    {"Symbol": ["ALUA", "ALUA"], 
                    "Clearing": ["48hs", "CI"], 
                    "Bid": [15, not_value], 
                    "Offer": [not_value, 10],
                    "Price_with_costs": [(0,False), (0,False)]
                    }),
                8970.34,
                pd.DataFrame(
                    {"Symbol": [np.nan, np.nan], 
                    "Clearing": ["48hs", "CI"], 
                    "Bid": [14.940105, np.nan], 
                    "Offer": [np.nan, 10.03993],
                    "Size": [np.nan,np.nan],
                    "Price_with_costs": [(8970.34, True), (8970.34, True)]
                    }),

            ),
            (   
                pd.DataFrame({
                            "Symbol": ["ALUA", "ALUA"], 
                            "Clearing": ["48hs", "CI"], 
                            "Bid": [2027.50, not_value], 
                            "Offer": [not_value, 2010],
                            "Price_with_costs": [(0,False), (0,False)]
                            }),
                56.77,
                pd.DataFrame({
                            "Symbol": ["ALUA", "ALUA"], 
                            "Clearing": ["48hs", "CI"], 
                            "Bid": [2027.50, not_value], 
                            "Offer": [not_value, 2010],
                            "Price_with_costs": [(56.77, True), (56.77, True)]
                            }),
            ),
            (   
                pd.DataFrame({
                            "Symbol": ["ALUA", "ALUA"], 
                            "Clearing": ["48hs", "CI"], 
                            "Bid": [2027.50, not_value], 
                            "Offer": [not_value, 2010],
                            "Price_with_costs": [(0,False), (0,False)]
                            }),
                56.77,
                pd.DataFrame({
                            "Symbol": ["ALUA", "ALUA"], 
                            "Clearing": ["48hs", "CI"], 
                            "Bid": [2027.50, not_value], 
                            "Offer": [not_value, 2010],
                            "Price_with_costs": [(56.77, True), (56.77, True)]
                            }),
            ),

        ]
    ,ids=["Test Case 1: Calculate TNA of rows 0 and 1", "Test Case 2: Calculate TNA of rows 0 and 1 with different values", "Test Case 3: Calculate TNA of rows 0 and 1 where row 0 has already calculated costs"])
    def test_calculate_tna(self, strategy_instance, rows_with_symbol_df, expected_result_tna, expected_df):
        
        
        result_tna = strategy_instance.calculate_tna(rows_with_symbol_df)
        assert result_tna == expected_result_tna
        # pd.testing.assert_frame_equal(strategy_instance.df, expected_df)
    
    @pytest.mark.parametrize(
    "rows_with_symbol_df, expected_result",
        [
            (   
                pd.DataFrame(
                    {"Symbol": ["ALUA", "ALUA"], 
                    "Clearing": ["48hs", "CI"], 
                    "Bid": [15, not_value], 
                    "Offer": [not_value, 10]
                    }),
                [{'Symbol': 'ALUA', 'Clearing': '48hs', 'Bid': 15.0, 'Offer': not_value}, {'Symbol': 'ALUA', 'Clearing': 'CI', 'Bid': not_value, 'Offer': 10.0}],
            ),
            (   
                pd.DataFrame({
                            "Symbol": ["ALUA", "ALUA"], 
                            "Clearing": ["48hs", "CI"], 
                            "Bid": [2027.50, not_value], 
                            "Offer": [not_value, 2010]
                            }),
                [{'Symbol': 'ALUA', 'Clearing': '48hs', 'Bid': 2027.5, 'Offer': not_value}, {'Symbol': 'ALUA', 'Clearing': 'CI', 'Bid': not_value, 'Offer': 2010.0}],
            ),

        ]
    ,ids=["Test Case 1: Transform df with two tickets into dictionary", "Test Case 2: Transform df with two tickets into dictionary with different values",])
    def test_transform_rows_into_dict(self, strategy_instance, rows_with_symbol_df, expected_result):

        result = strategy_instance.transform_rows_into_dict(rows_with_symbol_df)
        assert result == expected_result


    # @pytest.mark.parametrize(
    # "data, df_data, expected_df, expected_result, should_call_prepare_orders",
    #     [
    #         (   
    #             {'type': 'Md', 'timestamp': 1713216949167, 'instrumentId': {'marketId': 'ROFX', 'symbol': 'MERV - XMEV - ALUA - 48hs'}, 'marketData': {'OF': [], 'BI': [{'price': 19654.0, 'size': 1}]}},
    #             pd.DataFrame(),
    #             pd.DataFrame({"Symbol": ["ALUA"], "Clearing": ["48hs"], "Bid": [19654.0], "Offer": [not_value], "Size": [1]}),
    #             None,
    #             False
    #         ),
    #         (   
    #             {'type': 'Md', 'timestamp': 1713216949168, 'instrumentId': {'marketId': 'ROFX', 'symbol': 'MERV - XMEV - BYMA - CI'}, 'marketData': {'Bid': [], 'OF': [{'price': 108222.0, 'size': 21}]}},
    #             pd.DataFrame({"Symbol": ["ALUA"], "Clearing": ["48hs"], "Bid": [19654.0], "Offer": [not_value], "Size": [1]}),
    #             pd.DataFrame({"Symbol": ["ALUA", "BYMA"], "Clearing": ["48hs", "CI"], "Bid": [19654.0, not_value], "Offer": [not_value, 108222.0], "Size": [1, 21]}),
    #             None,
    #             False
    #         ),
    #         (   
    #             {'type': 'Md', 'timestamp': 1713216949167, 'instrumentId': {'marketId': 'ROFX', 'symbol': 'MERV - XMEV - BYMA - 48hs'}, 'marketData': {'OF': [], 'BI': [{'price': 19654.0, 'size': 1}]}},
    #             pd.DataFrame({
    #                 "Symbol": ["ALUA", "BYMA"], 
    #                 "Clearing": ["48hs", "CI"], 
    #                 "Bid": [19654.0, not_value], 
    #                 "Offer": [not_value, 19654.0], 
    #                 "Size": [1, 21],
    #                 "has_costs_calculated": [False, False]}),
    #             pd.DataFrame({
    #                 "Symbol": ["ALUA", "BYMA", "BYMA"], 
    #                 "Clearing": ["48hs", "CI", "48hs"], 
    #                 "Bid": [19654.0, not_value, 19575.521578], 
    #                 "Offer": [not_value,  19732.478422, not_value], 
    #                 "Size": [1, 21, 1],
    #                 "has_costs_calculated": [False, True, True]}),
    #             ("No se mando orden por no tener TNA requerida"),
    #             False
    #         ),
    #         # (   
    #         #     {'type': 'Md', 'timestamp': 1713216949167, 'instrumentId': {'marketId': 'ROFX', 'symbol': 'MERV - XMEV - BYMA - 48hs'}, 'marketData': {'OF': [], 'BI': [{'price': 15, 'size': 1}]}},
    #         #     pd.DataFrame({"Symbol": ["ALUA", "BYMA"], "Clearing": ["48hs", "CI"], "Bid": [19654.0, not_value], "Offer": [not_value, 10], "Size": [1, 21]}),
    #         #     pd.DataFrame({"Symbol": ["ALUA", "BYMA", "BYMA"], "Clearing": ["48hs", "CI", "48hs"], "Bid": [19654.0, not_value, 15], "Offer": [not_value, 10, not_value], "Size": [1, 21, 1]}),
    #         #     ("La TNA cumple con los requerimientos"),
    #         #     True
    #         # ),
    #     ],
    #     ids=["Test Case 1: df without data adding new data", 
    #         "Test Case 2: df with data adding new data with different symbol",
    #         "Test Case 3: df with data adding new data with same symbol different clearing and not sending order",
    #         # "Test Case 4: df with data adding new data with same symbol different clearing and sending order"
    #         ]
    # )
    # def test_smoke(self, strategy_instance, data, df_data, expected_df, expected_result, should_call_prepare_orders):        
    #     if not df_data.empty:
    #         strategy_instance.df = df_data

    #     result = strategy_instance.run_strategy(data)

    #     if expected_result is not None:
    #         assert result == expected_result
    #         #     with patch.object(strategy_instance, 'prepare_orders') as mock_prepare_orders:
    #         #         strategy_instance.run_strategy(data)
    #         #         assert mock_prepare_orders.called == should_call_prepare_orders


    #         pd.testing.assert_frame_equal(strategy_instance.df, expected_df)
    #     # except:
    #     #     with patch.object(strategy_instance, 'calculate_tna') as mock_prepare_orders:
    #     #         print(mock_prepare_orders)
    #     #         strategy_instance.run_strategy(data)
    #     #         assert mock_prepare_orders.called == should_call_prepare_orders




if __name__ == '__main__':
    # [-s] es para ver los prints
    # [-v] es para obtener información mas detallada de los errores
    # [-x ]stop after first failure
    # [-tb=short]  shorter traceback format
    # [-p no:doctest] es para que no te salgan los parametros que tiene cada test fallido
    pytest.main(["-s", "-vv", "-x", "--tb=short", "-p no:doctest"])