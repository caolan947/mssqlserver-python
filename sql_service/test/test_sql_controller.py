import unittest
from unittest.mock import Mock, patch

import sql_controller
import sql_service

class TestSqlController(unittest.TestCase):
    def setUp(self):
        self.fake_valid_conn_string = 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;DATABASE=testdb;UID=user;PWD=123'
        
        self.fake_cursor = Mock(rowcount = 3)
        self.fake_logger = Mock()     

        self.sql_controller_w_where = sql_controller.SqlController(params = {'table': 'tbl_client', 'columns': 'first_name,last_name', 'values': None, 'params': [{'attr1': 'value1'}, {'attr2': 'value2'}], 'where': "WHERE id = '8B6E8C04-3137-4EB0-8E68-F6236D47C2E6'"}, transaction_id = '123456', logger = self.fake_logger)
        self.sql_controller_wo_where = sql_controller.SqlController(params = {'table': 'tbl_client', 'columns': 'first_name,last_name', 'values': None, 'params': [{'attr1': 'value1'}, {'attr2': 'value2'}], 'where': None}, transaction_id = '123456', logger = self.fake_logger)

        self.generic_error = "Generic error occured"

        self.fake_config = {
            "credentials": {
                "driver": "DRIVERNAME",
                "server": "SERVERNAME", 
                "database": "db_name", 
                "username": "username",
                "password": "password"
            }
        }

        self.fake_table_name = 'tbl'
        self.fake_select_query = "SELECT attr1, attr2 FROM tbl WHERE id = '1'"
        self.fake_cursor_description = (('attr1', str, None, 50, 50, 0, True), ('attr2', str, None, 50, 50, 0, True))
        self.fake_columns = ['attr1', 'attr2']
        self.fake_transaction_id = '123456'
        self.fake_rows_affected = 3
        self.fake_values = ['value1', 'value2']
        self.fake_params = "attr1 = 'value1', attr2 = 'value2'"
        self.fake_where = "id = '1'"
        self.fake_results = [('value1', 'value2')]
        self.fake_select_query = f"SELECT {self.fake_columns} FROM {self.fake_table_name} WHERE {self.fake_where}"
        self.fake_insert_statement = f"INSERT INTO {self.fake_table_name} ({self.fake_columns}) VALUES ({self.fake_values})"
        self.fake_update_statement = f"UPDATE {self.fake_table_name} SET {self.fake_params} {self.fake_where}"
        self.fake_delete_statement = f"DELETE FROM {self.fake_table_name} WHERE {self.fake_where}"

    @patch.object(sql_controller.config, 'db')
    @patch.object(sql_controller.SqlController, 'connect')
    def test__init__(self, mock_cursor, mock_config):

        mock_config.return_value = self.fake_config
        mock_cursor.return_value = self.fake_cursor

        params = {
                'table': self.fake_table_name,
                'columns': self.fake_columns,
                'values': None,
                'params': self.fake_params,
                'where': self.fake_where
            }

        actual_result = sql_controller.SqlController(
            params = params,
            transaction_id = self.fake_transaction_id,
            logger = self.fake_logger
        )
        
        with self.subTest("""
        GIVEN no exceptions are caught
        WHEN the SqlController() class is instantiated
        THEN a functions used to enrich the object are invoked and parameters are correctly set
        """):
            self.assertEqual(mock_cursor.call_count, 1)
            self.assertEqual(self.fake_logger, actual_result.logger)
            self.assertEqual(self.fake_cursor, actual_result.cursor)
            self.assertEqual(self.fake_transaction_id, actual_result.transaction_id)
            self.assertEqual(params, actual_result.params)

    @patch.object(sql_service, 'create_cursor')
    @patch.object(sql_service, 'connect')
    @patch.object(sql_service, 'form_conn_string')
    def test_connect(self, mock_conn_string, mock_conn, mock_cursor):        
        mock_conn_string.return_value = {
            'error': True,
        }

        with self.subTest("""
        GIVEN an exception is caught
        WHEN the form_conn_string() method is called
        THEN a RuntimeError exception is raised
        """):
            with self.assertRaises(RuntimeError) as context:
                self.sql_controller_w_where.connect()
            self.assertTrue('Error when forming connection string' in str(context.exception))
        
        mock_conn_string.return_value = {
            'error': False,
            'data': self.fake_valid_conn_string,
        }

        mock_conn.return_value = {
            'error': True,
            'exception': self.generic_error
        }

        with self.subTest("""
        GIVEN an exception is caught
        WHEN the connect() method is called
        THEN a ConnectionError exception is raised
        """):
            with self.assertRaises(ConnectionError) as context:
                self.sql_controller_w_where.connect()
            self.assertTrue(self.generic_error in str(context.exception))

        mock_conn.return_value = {
            'error': False,
            'data': self.fake_cursor,
        }

        mock_cursor.return_value = {
            'error': True,
            'exception': self.generic_error
        }

        with self.subTest("""
        GIVEN an exception is caught
        WHEN the create_cursor() method is called
        THEN a ConnectionError exception is raised
        """):
            with self.assertRaises(ConnectionError) as context:
                self.sql_controller_w_where.connect()
            self.assertTrue(self.generic_error in str(context.exception))

        mock_cursor.return_value = {
            'error': False,
            'data': self.fake_cursor,
        }

        actual_result = self.sql_controller_w_where.connect()

        with self.subTest("""
        GIVEN a connection is made successfully
        WHEN the form_conn_string(), connect() and create_cursor() methods are called
        THEN a connect is made, a cursor created and returned successfully
        """):
            self.assertEqual(self.fake_cursor, actual_result)
    
    @patch.object(sql_service, 'close_cursor')
    def test_close(self, mock_close_cursor):
        mock_close_cursor.side_effect = [
            {'error': True, 'exception': Exception(self.generic_error)},
            {'error': True, 'exception': Exception(self.generic_error)}
            ]

        with self.subTest("""
        GIVEN two exceptions are caught
        WHEN the close_cursor() method is called
        THEN a Exception exception is raised
        """):
            with self.assertRaises(Exception) as context:
                self.sql_controller_w_where.close()
            self.assertTrue(self.generic_error in str(context.exception))

        mock_close_cursor.side_effect = [
            {'error': True, 'exception': Exception(self.generic_error)},
            {'error': False, 'data': self.fake_cursor}
        ]

        actual_result = self.sql_controller_w_where.close()

        with self.subTest("""
        GIVEN one exception is caught
        WHEN the close_cursor() method is called
        THEN the closed cursor is returned
        """):
            self.assertEqual(self.fake_cursor, actual_result)

        mock_close_cursor.side_effect = None
        mock_close_cursor.return_value = {'error': False, 'data': self.fake_cursor}

        actual_result = self.sql_controller_w_where.close()

        with self.subTest("""
        GIVEN no exceptions are caught
        WHEN the close_cursor() method is called
        THEN the close cursor is returned
        """):
            self.assertEqual(self.fake_cursor, actual_result)

    @patch.object(sql_service, 'commit')    
    @patch.object(sql_service, 'rollback')    
    @patch.object(sql_service, 'execute_formed_statement')    
    @patch.object(sql_service, 'form_delete_statement')    
    def test_delete(self, mock_statement, mock_result, mock_rollback, mock_commit):
        mock_statement.return_value = {
            'error': True,
            'exception': Exception(self.generic_error)
        }

        with self.subTest("""
        GIVEN an exception is caught
        WHEN the form_delete_statement() method is called
        THEN a OSError exception is raised
        """):
            with self.assertRaises(OSError) as context:
                self.sql_controller_w_where.delete()
            self.assertTrue(self.generic_error in str(context.exception))   

        mock_statement.return_value = {
            'error': False,
            'data': self.fake_delete_statement
        }

        mock_result.return_value = {
            'error': True,
            'exception': Exception(self.generic_error)
        }

        with self.subTest("""
        GIVEN an exception is caught
        WHEN the execute_formed_statement() method is called
        THEN a Exception exception is raised
        """):
            with self.assertRaises(Exception) as context:
                self.sql_controller_w_where.delete()
            self.assertTrue(self.generic_error in str(context.exception))    
            self.assertTrue(mock_rollback.called)

        mock_result.return_value = {
            'error': False,
            'data': self.fake_cursor
        }

        mock_commit.return_value = {
            'error': True,
            'exception': Exception(self.generic_error)
        }

        with self.subTest("""
        GIVEN an exception is caught
        WHEN the commit() method is called
        THEN a Exception exception is raised
        """):
            with self.assertRaises(Exception) as context:
                self.sql_controller_w_where.delete()
            self.assertTrue(self.generic_error in str(context.exception))  
            self.assertTrue(mock_rollback.called)

        mock_commit.return_value = {
            'error': False,
            'data': self.fake_cursor
        }

        actual_result = self.sql_controller_w_where.delete()

        with self.subTest("""
        GIVEN no exceptions are caught
        WHEN the commit() method is called
        THEN a message summarising the execution is returned
        """):
            self.assertEqual(self.fake_cursor, actual_result['data'])

    @patch.object(sql_service, 'commit')    
    @patch.object(sql_service, 'rollback')    
    @patch.object(sql_service, 'execute_formed_statement')    
    @patch.object(sql_service, 'form_insert_statement')    
    @patch('sql_controller.utils')
    def test_insert(self, mock_util, mock_statement, mock_result, mock_rollback, mock_commit):
        mock_util.listify_string.side_effect = RuntimeError(self.generic_error)

        with self.subTest("""
        GIVEN an exception is caught
        WHEN the listify_string() method is called
        THEN a RuntimeError exception is raised
        """):
            with self.assertRaises(RuntimeError) as context:
                self.sql_controller_w_where.insert()
            self.assertTrue(self.generic_error in str(context.exception))      

        mock_util.listify_string.side_effect = None
        mock_util.listify_string.return_value = "'value1','value2'"

        mock_statement.side_effect = OSError(self.generic_error)

        with self.subTest("""
        GIVEN an exception is caught
        WHEN the form_insert_statement() method is called
        THEN a RuntimeError exception is raised
        """):
            with self.assertRaises(OSError) as context:
                self.sql_controller_w_where.insert()
            self.assertTrue(self.generic_error in str(context.exception))

        mock_statement.side_effect = None
        mock_statement.return_value = {
            'error': False,
            'data': self.fake_cursor
        }

        mock_result.return_value = {
            'error': True,
            'exception': Exception(self.generic_error)
        }

        mock_rollback.return_value = {
            'error': False,
            'msg': 'Successfully rolled back cursor changes'
        }

        with self.subTest("""
        GIVEN an exception is caught
        WHEN the execute_formed_statement() method is called
        THEN a Exception exception is raised
        """):
            with self.assertRaises(Exception) as context:
                self.sql_controller_w_where.insert()
            self.assertTrue(self.generic_error in str(context.exception))  
            self.assertTrue(mock_rollback.call_count, 1)

        mock_result.return_value = {
            'error': False,
            'data': self.fake_cursor
        }

        mock_commit.return_value = {
            'error': True,
            'exception': Exception(self.generic_error)
        }

        mock_rollback.reset_mock()

        with self.subTest("""
        GIVEN an exception is caught
        WHEN the commit() method is called
        THEN a Exception exception is raised
        """):
            with self.assertRaises(Exception) as context:
                self.sql_controller_w_where.insert()
            self.assertTrue(self.generic_error in str(context.exception))  
            self.assertTrue(mock_rollback.call_count, 1)

        mock_commit.return_value = {
            'error': False,
            'data': self.fake_cursor
        }

        actual_result = self.sql_controller_w_where.insert()

        with self.subTest("""
        GIVEN no exception is caught
        WHEN the commit() method is called
        THEN a message summarising the execution is returned
        """):
            self.assertEqual(self.fake_cursor, actual_result['data'])

    @patch.object(sql_service, 'zip_columns_results')
    @patch.object(sql_service, 'get_results')
    @patch.object(sql_service, 'get_columns')
    @patch.object(sql_service, 'rollback')
    @patch.object(sql_service, 'execute_formed_query')
    @patch.object(sql_service, 'form_select_query')
    def test_select(self, mock_query, mock_query_results, mock_rollback, mock_columns, mock_results, mock_results_cols):
        mock_query.return_value = {
            'error': True,
            'exception': Exception(self.generic_error)
        }

        with self.subTest("""
        GIVEN an exception is caught
        WHEN the form_select_query() method is called
        THEN a OSError exception is raised
        """):
            with self.assertRaises(OSError) as context:
                self.sql_controller_w_where.select()
            self.assertTrue(self.generic_error in str(context.exception))

        mock_query.return_value = {
            'error': False,
            'data': self.fake_select_query
        }

        mock_query_results.return_value = {
            'error': True,
            'exception': Exception(self.generic_error)
        }

        mock_rollback.return_value = {
            'error': False,
            'msg': 'Successfully rolled back cursor changes'
        }

        with self.subTest("""
        GIVEN an exception is caught
        WHEN the execute_formed_query() method is called
        THEN a Exception exception is raised
        """):
            with self.assertRaises(Exception) as context:
                self.sql_controller_w_where.select()
            self.assertTrue(self.generic_error in str(context.exception))
            self.assertEqual(mock_rollback.call_count, 1) 

        mock_query_results.return_value = {
            'error': False,
            'data': self.fake_results
        }
        
        mock_columns.return_value = {
            'error': True,
            'exception': Exception(self.generic_error)
        }

        with self.subTest("""
        GIVEN an exception is caught
        WHEN the get_columns() method is called
        THEN a Exception exception is raised
        """):
            with self.assertRaises(Exception) as context:
                self.sql_controller_w_where.select()
            self.assertTrue(self.generic_error in str(context.exception))

        mock_columns.return_value = {
            'error': False,
            'data': self.fake_columns
        }
        
        mock_results.return_value = {
            'error': True,
            'exception': Exception(self.generic_error)
        }

        with self.subTest("""
        GIVEN an exception is caught
        WHEN the get_results() method is called
        THEN a Exception exception is raised
        """):
            with self.assertRaises(Exception) as context:
                self.sql_controller_w_where.select()
            self.assertTrue(self.generic_error in str(context.exception))
        
        mock_results.return_value = {
            'error': False,
            'data': self.fake_values
        }
        
        mock_results_cols.return_value = {
            'error': True,
            'exception': Exception(self.generic_error)
        }

        with self.subTest("""
        GIVEN an exception is caught
        WHEN the zip_columns_results() method is called
        THEN a Exception exception is raised
        """):
            with self.assertRaises(Exception) as context:
                self.sql_controller_w_where.select()
            self.assertTrue(self.generic_error in str(context.exception))

    
        expected_result = {
            'msg': 'Successfully zipped columns with results',
            'data': [{'attr1': 'value1', 'attr2': 'value2'}]
        }

        mock_results_cols.return_value = {
            'error': False,
            'msg': 'Successfully zipped columns with results',
            'data': [{'attr1': 'value1', 'attr2': 'value2'}]
        }

        actual_result = self.sql_controller_w_where.select()

        with self.subTest("""
        GIVEN no exception is caught
        WHEN the zip_columns_results() method is called
        THEN a message summarising the execution is returned
        """):
            self.assertTrue(expected_result, actual_result)
     
    @patch.object(sql_service, 'commit')
    @patch.object(sql_service, 'rollback')
    @patch.object(sql_service, 'execute_formed_statement')
    @patch.object(sql_service, 'form_update_statement')
    @patch('sql_service.utils')
    def test_update(self, mock_params, mock_statement, mock_result, mock_rollback, mock_commit):
        
        mock_params.unpack_dict_list.side_effect = RuntimeError(self.generic_error)

        with self.subTest("""
        GIVEN an exception is caught
        WHEN the unpack_dict_list() method is called
        THEN a RuntimeError exception is raised
        """):
            with self.assertRaises(Exception) as context:
                self.sql_controller_w_where.update()

        mock_params.unpack_dict_list.side_effect = None
        mock_params.unpack_dict_list.return_value = self.fake_params

        mock_statement.return_value = {
            'error': True,
            'exception': OSError(self.generic_error)
        }

        with self.subTest("""
        GIVEN an exception is caught
        WHEN the form_update_statement() method is called
        THEN a OSError exception is raised
        """):
            with self.assertRaises(OSError) as context:
                self.sql_controller_w_where.update()
            self.assertTrue(self.generic_error in str(context.exception))
        
        mock_statement.return_value = {
            'error': False,
            'data': self.fake_update_statement
        }

        mock_result.return_value = {
            'error': True,
            'exception': Exception(self.generic_error)
        }

        mock_rollback.return_value = {
            'error': False,
            'msg': 'Successfully rolled back cursor changes'
        }

        with self.subTest("""
        GIVEN an exception is caught
        WHEN the execute_formed_statement() method is called
        THEN a Exception exception is raised
        """):
            with self.assertRaises(Exception) as context:
                self.sql_controller_w_where.update()
            self.assertEqual(mock_rollback.call_count, 1)
            self.assertTrue(self.generic_error in str(context.exception))
        

        mock_result.return_value = {
            'error': False,
            'data': self.fake_cursor
        }

        mock_commit.return_value = {
            'error': True,
            'exception': Exception(self.generic_error)
        }

        mock_rollback.reset_mock()

        with self.subTest("""
        GIVEN an exception is caught
        WHEN the commit() method is called
        THEN a Exception exception is raised
        """):
            with self.assertRaises(Exception) as context:
                self.sql_controller_w_where.update()
            self.assertEqual(mock_rollback.call_count, 1)
            self.assertTrue(self.generic_error in str(context.exception))

        expected_result = {
            'error': False,
            'msg': 'Successfully committed changes',
            'data': f"3 row(s) affected"
        }

        mock_commit.return_value = {
            'error': False,
            'msg': 'Successfully committed changes',
            'data': f"3 row(s) affected"
        }

        actual_result = self.sql_controller_w_where.update()

        with self.subTest("""
        GIVEN self.params['where'] is not None
        WHEN the update() method is called
        THEN self.params['where'] will be prepended with 'WHERE '
        """):
            self.assertEqual(expected_result, actual_result)
        

        expected_result = {
            'error': False,
            'msg': 'Successfully committed changes',
            'data': f"3 row(s) affected"
        }

        actual_result = self.sql_controller_wo_where.update()

        with self.subTest("""
        GIVEN self.params['where'] is not None
        WHEN the update() method is called
        THEN self.params['where'] will be prepended with 'WHERE '
        """):
            self.assertEqual(expected_result, actual_result)
      
    @patch.object(sql_service, 'rollback')
    def test_rollback(self, mock_response):
        error_msg = 'An error occured when trying to rollback cursor changes'
        
        mock_response.side_effect = [
            {'error': True, 'msg': error_msg}, 
            {'error': True, 'msg': error_msg}
        ]

        actual_result = self.sql_controller_w_where.rollback()
        with self.subTest("""
        GIVEN two exceptions are caught
        WHEN the rollback() method is called
        THEN an error message is returned
        """):
            self.assertEqual(mock_response.call_count, 2)    
            self.assertEqual(error_msg, actual_result)

        success_msg = 'Successfully rolled back cursor changes'

        mock_response.side_effect = [
            {'error': True, 'msg': error_msg}, 
            {'error': False, 'msg': success_msg}
        ]

        mock_response.reset_mock()

        actual_result = self.sql_controller_w_where.rollback()

        with self.subTest("""
        GIVEN one exception is caught
        WHEN the rollback() method is called
        THEN a success message is returned
        """):
            self.assertEqual(mock_response.call_count, 2)    
            self.assertEqual(success_msg, actual_result)

        mock_response.side_effect = None
        mock_response.return_value = {
            'error': False,
            'msg': success_msg
        }

        mock_response.reset_mock()

        actual_result = self.sql_controller_w_where.rollback()

        with self.subTest("""
        GIVEN one exception is caught
        WHEN the rollback() method is called
        THEN a success message is returned
        """):
            self.assertEqual(mock_response.call_count, 1)    
            self.assertEqual(success_msg, actual_result)
               
if __name__ == "__main__":
    unittest.main()