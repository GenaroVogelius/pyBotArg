from unittest.mock import Mock, patch, call, MagicMock
from carrier import Carrier
from orders_table import OrdersTable
import pytest

class TestCarrier:

    

    @pytest.fixture
    def mock_broker(self):
        mock = Mock()
        return mock
    
    @pytest.fixture
    def carrier(self, mock_broker):
        
        return Carrier(mock_broker)

    



    @pytest.mark.parametrize(
    "orders_list, budget, format_instruments_params, budget_substracting_costs, expected_order_size, order_status ",
        [
            (   
                [{'Symbol': 'ALUA', 'Clearing': 'CI', 'Bid': 0.0, 'Offer': 52294.0, 'Size': 8, 'Price_with_costs': (52392.08, True), 'TNA': 14995.33, 'Side': 'buy'}, {'Symbol': 'ALUA', 'Clearing': '48hs', 'Bid': 95620.0, 'Offer': 0.0, 'Size': 8, 'Price_with_costs': (95440.66, True), 'TNA': 14995.33, 'Side': 'sell'}],
                90000,
                [call("ALUA", "CI"), call("ALUA", "48hs")],
                37607.92,
                1,
                MagicMock(return_value={"order": {"text": "Operada "}})
            ),
            (   
                [{'Symbol': 'ALUA', 'Clearing': 'CI', 'Bid': 0.0, 'Offer': 52294.0, 'Size': 8, 'Price_with_costs': (52392.08, True), 'TNA': 14995.33, 'Side': 'buy'}, {'Symbol': 'ALUA', 'Clearing': '48hs', 'Bid': 95620.0, 'Offer': 0.0, 'Size': 8, 'Price_with_costs': (95440.66, True), 'TNA': 14995.33, 'Side': 'sell'}],
                100,
                None,
                None,
                None,
                None
            ),
            (   
                    [{'Symbol': 'ALUA', 'Clearing': 'CI', 'Bid': 0.0, 'Offer': 52294.0, 'Size': 8, 'Price_with_costs': (1000, True), 'TNA': 14995.33, 'Side': 'buy'}, {'Symbol': 'ALUA', 'Clearing': '48hs', 'Bid': 95620.0, 'Offer': 0.0, 'Size': 8, 'Price_with_costs': (95440.66, True), 'TNA': 14995.33, 'Side': 'sell'}],
                    9000,
                    [call("ALUA", "CI"), call("ALUA", "48hs")],
                    1000,
                    8,
                    MagicMock(return_value={"order": {"text": "Operada "}})
            ),
            (   
                    [{'Symbol': 'ALUA', 'Clearing': 'CI', 'Bid': 0.0, 'Offer': 52294.0, 'Size': 8, 'Price_with_costs': (1000, True), 'TNA': 14995.33, 'Side': 'buy'}, {'Symbol': 'ALUA', 'Clearing': '48hs', 'Bid': 95620.0, 'Offer': 0.0, 'Size': 8, 'Price_with_costs': (95440.66, True), 'TNA': 14995.33, 'Side': 'sell'}],
                    9000,
                    [call("ALUA", "CI")],
                    1000,
                    8,
                    MagicMock(return_value={"order": {"text": "Not operada"}})
            ),

        ]
    ,ids=["Test Case 1: Sending order with size 1", "Test Case 2: Not sending order for low budget", "Test Case 3: Sending order with full size", "Test Case 4: Sending order but canceling it for not being operated"])
    # todo hacer test con budget que alcanza para algunas acciones pero no todas las disponibles
    def test_send_orders(self, carrier, mock_broker, orders_list, budget, format_instruments_params, budget_substracting_costs, expected_order_size, order_status):

        mock_broker.budget = budget

        # Set up mock_order_status
        mock_order_status = order_status

        with patch.object(carrier, 'format_instruments') as format_instruments, \
                patch.object(carrier.orders_table, 'save_row') as mock_save_row, \
                patch('pyRofex.send_order') as mock_sended_order, \
                patch('pyRofex.cancel_order') as mock_cancel_order:
            # Assign mock_order_status to the carrier object
            carrier.order_status = mock_order_status
            mock_cancel_order.return_value = {'status': 'OK', 'order': {'clientId': '454029179388448', 'proprietary': 'PBCP'}}

            carrier.send_orders(orders_list)

            if budget >= orders_list[0]["Price_with_costs"][0]:
                if order_status.return_value["order"]["text"] == "Operada ":
                    assert format_instruments.call_args_list == format_instruments_params
                    assert mock_sended_order.call_count == 2
                    assert mock_sended_order.call_args[1]['size'] == expected_order_size
                    assert mock_broker.budget == budget_substracting_costs
                else:
                    assert format_instruments.call_args_list == format_instruments_params
                    assert mock_sended_order.call_count == 1
                    assert mock_sended_order.call_args[1]['size'] == expected_order_size
                    assert mock_broker.budget == budget_substracting_costs


            else:

                assert mock_sended_order.call_count == 0
                assert format_instruments.call_count == 0
                assert mock_save_row.call_count == 0


if __name__ == '__main__':
    # [-s] es para ver los prints
    # [-v] es para obtener información mas detallada de los errores
    # [-x ]stop after first failure
    # [-tb=short]  shorter traceback format
    # [-p no:doctest] es para que no te salgan los parametros que tiene cada test fallido
    pytest.main(["-s", "-vv", "-x", "--tb=short", "-p no:doctest"])

