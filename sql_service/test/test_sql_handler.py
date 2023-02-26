import unittest
from unittest.mock import Mock, patch

import sql_handler
import sql_controller

class TestSqlHandler(unittest.TestCase):
    def setUp(self):

        self.fake_table_name = 'tbl'
        self.fake_where = "id = '1'"
        self.fake_transaction_id = '123456'
        self.fake_columns = "attr1,attr2"
        self.fake_values = "value1,value2"
        self.fake_params = "params[attr1]=value1&params[attr2]=value2"
        self.fake_params_params = [{'attr1': 'value1'}, {'attr2': 'value2'}]
        self.generic_error = "Generic error occured"

        self.fake_logger = Mock()     

        self.handler = sql_handler.SqlService(
            'delete', 
            {
                'table': self.fake_table_name,
                'where': self.fake_where,
                'columns': None,
                'values': None,
                'params': None
            },
            self.fake_transaction_id,
            self.fake_logger
        )

    @patch('sql_handler.SqlService')
    def test__init__1(self, mock_handler):
        mock_handler.upper.return_value = 'DELETE'

        actual_result = sql_handler.SqlService(
            'delete', 
            {
                'table': self.fake_table_name,
                'columns': self.fake_columns,
                'values': self.fake_values,
                'params': self.fake_params_params,
                'where': self.fake_where
            },
            self.fake_transaction_id,
            self.fake_logger
        )

        with self.subTest("""
        GIVEN no exceptions are caught
        WHEN the upper() function is invoked
        THEN a functions used to enrich the object are invoked and parameters are correctly set
        """):
            self.assertEqual(mock_handler.call_count, 1)

    @patch.object(sql_handler.SqlService, 'is_valid')
    @patch('sql_handler.utils')
    def test__init__2(self, mock_utils, mock_valid_request):
        mock_utils.comma_split.side_effect = [self.fake_columns, self.fake_values]
        mock_utils.get_params.return_value = self.fake_params_params
        mock_utils.is_key.return_value = self.fake_where

        mock_valid_request.return_value = True

        validation_msg = "Trying to execute DELETE statement. One or more parameters is missing. Provide values for'statement_type', 'table' and 'where' parameters in request body."

        mock_valid_request.return_value = validation_msg

        with self.subTest("""
            GIVEN is_valid() returns a validation message
            WHEN the SqlController() class is instantiated
            THEN a ValueError exception will be raised
            """):

            with self.assertRaises(ValueError) as context:
                sql_handler.SqlService(
                    'delete', 
                    {
                        'table': self.fake_table_name,
                        'where': self.fake_where,
                        'columns': None,
                        'values': None,
                        'params': None
                    },
                    self.fake_transaction_id,
                    self.fake_logger
                )

    @patch('sql_controller.SqlController')
    @patch.object(sql_handler.SqlService, 'is_valid')
    @patch('sql_handler.utils')
    def test__init__3(self, mock_utils, mock_valid_request, mock_controller):
        mock_utils.comma_split.side_effect = [self.fake_columns, self.fake_values]
        mock_utils.get_params.return_value = self.fake_params_params
        mock_utils.is_key.return_value = self.fake_where

        mock_valid_request.return_value = True

        mock_controller.return_value = Mock()

        actual_result = sql_handler.SqlService(
            'delete', 
            {
                'table': self.fake_table_name,
                'where': self.fake_where,
                'columns': self.fake_columns,
                'values': self.fake_values,
                'params': self.fake_params
            },
            self.fake_transaction_id,
            self.fake_logger
        )

        with self.subTest("""
            GIVEN is_valid() returns a boolean True
            WHEN the SqlController() class is instantiated
            THEN a SqlController() object will be instantiated
            """):
            self.assertEqual(self.fake_transaction_id, actual_result.transaction_id)
            self.assertEqual(self.fake_columns, actual_result.params['columns'])
            self.assertEqual(self.fake_values, actual_result.params['values'])
            self.assertEqual(self.fake_params_params, actual_result.params['params'])
            self.assertEqual(self.fake_where, actual_result.params['where'])
            self.assertTrue(actual_result.valid_request)
            self.assertEqual(mock_controller.call_count, 1)

    @patch('sql_controller.SqlController')
    @patch.object(sql_handler.SqlService, 'is_valid')
    @patch('sql_handler.utils')
    def test__init__4(self, mock_utils, mock_valid_request, mock_controller):
        mock_utils.comma_split.side_effect = [self.fake_columns, self.fake_values]
        mock_utils.get_params.return_value = self.fake_params_params
        mock_utils.is_key.return_value = self.fake_where

        mock_controller.side_effect = ConnectionError(self.generic_error)

        mock_valid_request.return_value = True

        with self.subTest("""
            GIVEN a ConnectionError exception is caught
            WHEN the SqlController() class is instantiated
            THEN a ConnectionError exception will be raised
            """):

            with self.assertRaises(ConnectionError) as context:
                sql_handler.SqlService(
                    'delete', 
                    {
                        'table': self.fake_table_name,
                        'where': self.fake_where,
                        'columns': None,
                        'values': None,
                        'params': None
                    },
                    self.fake_transaction_id,
                self.fake_logger
                )
            self.assertTrue(self.generic_error in str(context.exception))

    def test_is_valid(self):
        expected_result = "Trying to handle request. An invalid endpoint 'invalid' was called. Use a valid option: select/, insert/, update/ or delete/"

        self.handler.statement_type = 'invalid'

        actual_result = self.handler.is_valid()

        with self.subTest("""
        GIVEN the value of self.statement_type is invalid
        WHEN the is_valid() function is called
        THEN a validation error message will be returned
        """):
            self.assertEqual(expected_result, actual_result)

        expected_result = "Trying to execute DELETE statement. One or more parameters is missing. Provide values for'statement_type', 'table' and 'where' parameters in request body."

        self.handler.statement_type = 'DELETE'
        self.handler.params['table'] = None

        actual_result = self.handler.is_valid()

        with self.subTest("""
        GIVEN the value of self.statement_type is 'DELETE' and parameters are missing
        WHEN the is_valid() function is called
        THEN a validation error message will be returned
        """):
            self.assertEqual(expected_result, actual_result)
        
        expected_result = "Trying to execute INSERT statement. One or more parameters is missing. Provide values for'statement_type', 'table', 'columns' and 'values' parameters in request body."

        self.handler.statement_type = 'INSERT'
        self.handler.params['table'] = None

        actual_result = self.handler.is_valid()

        with self.subTest("""
        GIVEN the value of self.statement_type is 'INSERT' and parameters are missing
        WHEN the is_valid() function is called
        THEN a validation error message will be returned
        """):
            self.assertEqual(expected_result, actual_result)

        expected_result = "Trying to execute SELECT statement. One or more parameters is missing. Provide values for'statement_type', 'table' and 'columns' parameters in request body."

        self.handler.statement_type = 'SELECT'
        self.handler.params['table'] = None

        actual_result = self.handler.is_valid()

        with self.subTest("""
        GIVEN the value of self.statement_type is 'SELECT' and parameters are missing
        WHEN the is_valid() function is called
        THEN a validation error message will be returned
        """):
            self.assertEqual(expected_result, actual_result)
        
        expected_result = "Trying to execute UPDATE statement. One or more parameters is missing. Provide values for'statement_type', 'table', 'columns' and 'params' parameters in request body."

        self.handler.statement_type = 'UPDATE'
        self.handler.params['table'] = None

        actual_result = self.handler.is_valid()

        with self.subTest("""
        GIVEN the value of self.statement_type is 'UPDATE' and parameters are missing
        WHEN the is_valid() function is called
        THEN a validation error message will be returned
        """):
            self.assertEqual(expected_result, actual_result)

        self.handler.params['table'] = self.fake_table_name

        actual_result = self.handler.is_valid()

        with self.subTest("""
        GIVEN the value of self.statement_type is 'UPDATE' and no parameters are missing
        WHEN the is_valid() function is called
        THEN a boolean True will be returned
        """):
            self.assertTrue(actual_result)

    @patch.object(sql_controller.SqlController, 'close')
    @patch.object(sql_controller.SqlController, 'update')
    @patch.object(sql_controller.SqlController, 'select')
    @patch.object(sql_controller.SqlController, 'insert')
    @patch.object(sql_controller.SqlController, 'delete')
    def test_sql_handler(self, mock_delete, mock_insert, mock_select, mock_update, mock_close):

        handler = sql_handler.SqlService(
            'delete', 
            {
                'table': self.fake_table_name,
                'where': self.fake_where,
                'columns': None,
                'values': None,
                'params': None
            },
            self.fake_transaction_id,
            self.fake_logger
        )

        handler.sql_handler()

        with self.subTest("""
        GIVEN the value of self.statement_type is 'DELETE'
        WHEN the sql_handler() function is called
        THEN the delete() function will be invoked
        """):
            self.assertEqual('DELETE', handler.statement_type)
            self.assertEqual(mock_delete.call_count, 1)
            self.assertEqual(mock_close.call_count, 1)

        mock_close.reset_mock()

        handler = sql_handler.SqlService(
            'insert', 
            {
                'table': self.fake_table_name,
                'where': None,
                'columns': self.fake_columns,
                'values': self.fake_values,
                'params': None
            },
            self.fake_transaction_id,
            self.fake_logger
        )

        handler.sql_handler()

        with self.subTest("""
        GIVEN the value of self.statement_type is 'INSERT'
        WHEN the sql_handler() function is called
        THEN the insert() function will be invoked
        """):
            self.assertEqual('INSERT', handler.statement_type)
            self.assertEqual(mock_insert.call_count, 1)
            self.assertEqual(mock_close.call_count, 1)

        mock_close.reset_mock()

        handler = sql_handler.SqlService(
            'select', 
            {
                'table': self.fake_table_name,
                'where': self.fake_where,
                'columns': self.fake_columns,
                'values': None,
                'params': None
            },
            self.fake_transaction_id,
            self.fake_logger
        )

        handler.sql_handler()

        with self.subTest("""
        GIVEN the value of self.statement_type is 'SELECT'
        WHEN the sql_handler() function is called
        THEN the select() function will be invoked
        """):
            self.assertEqual('SELECT', handler.statement_type)
            self.assertEqual(mock_select.call_count, 1)
            self.assertEqual(mock_close.call_count, 1)

        mock_close.reset_mock()

        handler = sql_handler.SqlService(
            'update', 
            {
                'table': self.fake_table_name,
                'where': None,
                'columns': None,
                'values': None,
                'params': self.fake_params
            },
            self.fake_transaction_id,
            self.fake_logger
        )

        handler.sql_handler()

        with self.subTest("""
        GIVEN the value of self.statement_type is 'UPDATE'
        WHEN the sql_handler() function is called
        THEN the update() function will be invoked
        """):
            self.assertEqual('UPDATE', handler.statement_type)
            self.assertEqual(mock_update.call_count, 1)
            self.assertEqual(mock_close.call_count, 1)

        mock_close.reset_mock()

        mock_update.side_effect = Exception(self.generic_error)

        with self.subTest("""
        GIVEN an Exception is raised
        WHEN the update() function is called
        THEN an Exception will be raised and controller cursor will be closed
        """):
            with self.assertRaises(Exception) as context:
                handler.sql_handler()
            self.assertEqual(mock_close.call_count, 1)


if __name__ == "__main__":
    unittest.main()