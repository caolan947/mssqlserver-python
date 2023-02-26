import unittest
from unittest.mock import Mock, patch

import sql_service

class TestSqlService(unittest.TestCase):

    def setUp(self):
        self.fake_valid_conn_string = 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;DATABASE=testdb;UID=user;PWD=123'
        self.fake_invalid_conn_string = 'DRIVER=invalid;SERVER=localhost;DATABASE=testdb;UID=user;PWD=123'

        self.fake_rows_affected = 3
        self.fake_cursor = Mock(rowcount = self.fake_rows_affected)
        self.fake_logger = Mock()
        self.generic_error = "Generic error occurred"

        self.fake_table_name = 'tbl'
        self.fake_select_query = "SELECT attr1, attr2 FROM tbl WHERE id = '1'"
        self.fake_cursor_description = (('attr1', str, None, 50, 50, 0, True), ('attr2', str, None, 50, 50, 0, True))
        self.fake_columns = ['attr1', 'attr2']
        self.fake_values = ['value1', 'value2']
        self.fake_params = "attr1 = 'value1', attr2 = 'value2'"
        self.fake_where = "id = '1'"
        self.fake_results = [('value1', 'value2')]
        self.fake_select_query = f"SELECT {self.fake_columns} FROM {self.fake_table_name} WHERE {self.fake_where}"
        self.fake_insert_statement = f"INSERT INTO {self.fake_table_name} ({self.fake_columns}) VALUES ({self.fake_values})"
        self.fake_update_statement = f"UPDATE {self.fake_table_name} SET {self.fake_params} {self.fake_where}"
        self.fake_delete_statement = f"DELETE FROM {self.fake_table_name} WHERE {self.fake_where}"

    def test_form_conn_string(self):
        expected_result = {
            'msg': 'Successfully formed connection string',
            'data': self.fake_valid_conn_string
        }

        actual_result = sql_service.form_conn_string('ODBC Driver 18 for SQL Server', 'localhost', 'testdb', 'user', '123', self.fake_logger)
        
        with self.subTest("""
        GIVEN all parameters are provided
        WHEN the form_conn_string() method is called
        THEN a valid connection string is formed and return
        """):
            self.assertFalse(actual_result['error'])        
            self.assertEqual(expected_result['msg'], actual_result['msg'])
            self.assertEqual(expected_result['data'], actual_result['data'])  

    @patch('sql_service.pyodbc')
    def test_connect(self, mock_conn):
        expected_result = {
            'msg': 'Successfully connected to database using pyodbc',
            'data': self.fake_cursor
        }

        mock_conn.connect.return_value = expected_result['data']
        
        actual_result = sql_service.connect(self.fake_valid_conn_string, self.fake_logger)

        with self.subTest("""
        GIVEN a valid connection string is passed
        WHEN the connect() method is called
        THEN the passed connection string is used to connect to pyodbc and connection object returned
        """):
            self.assertFalse(actual_result['error'])        
            self.assertEqual(expected_result['msg'], actual_result['msg'])
            self.assertEqual(expected_result['data'], actual_result['data'])        
    
        expected_result = {
            'msg': 'An error occurred when trying to connect to database',
            'exception': Exception(self.generic_error)
        }

        mock_conn.connect.side_effect = expected_result['exception']
        
        actual_result = sql_service.connect(self.fake_invalid_conn_string, self.fake_logger)

        with self.subTest("""
        GIVEN an exception is raised 
        WHEN the connect() method is called
        THEN an error dictionary will be returned
        """):
            self.assertTrue(actual_result['error'])        
            self.assertTrue(expected_result['msg'] in actual_result['msg'])
            self.assertEqual(expected_result['exception'], actual_result['exception'])        
            self.assertIsNone(actual_result['data'])        

    @patch('sql_service.pyodbc')
    def test_create_cursor(self, mock_cursor):
        expected_result = {
            'msg': 'Successfully created pyodbc cursor object',
            'data': self.fake_cursor
        }

        mock_cursor.cursor.return_value = expected_result['data']

        actual_result = sql_service.create_cursor(mock_cursor, self.fake_logger)

        with self.subTest("""
        GIVEN a valid connection object is passed
        WHEN the create_cursor() method is called
        THEN the passed connection will be used to create and return a cursor object
        """):
            self.assertFalse(actual_result['error'])        
            self.assertEqual(actual_result['msg'], expected_result['msg'])
            self.assertEqual(actual_result['data'], expected_result['data']) 
    
        expected_result = {
            'msg': 'An error occurred when trying to create cursor object',
            'exception': Exception(self.generic_error)
        }

        mock_cursor.cursor.side_effect = expected_result['exception']

        actual_result = sql_service.create_cursor(mock_cursor, self.fake_logger)

        with self.subTest("""
        GIVEN an exception is raised
        WHEN the create_cursor() method is called
        THEN an error dictionary will be returned
        """):
            self.assertTrue(actual_result['error'])        
            self.assertTrue(expected_result['msg'] in actual_result['msg'])
            self.assertEqual(expected_result['exception'], actual_result['exception'])        
            self.assertIsNone(actual_result['data']) 

    @patch('sql_service.pyodbc')
    def test_close_cursor(self, mock_cursor):
        expected_result = {
            'msg': 'Successfully closed cursor object',
            'data': self.fake_cursor
        }

        mock_cursor.close.return_value = expected_result['data']

        actual_result = sql_service.close_cursor(mock_cursor, self.fake_logger)

        with self.subTest("""
        GIVEN a valid cursor object is passed
        WHEN the close_cursor() method is called
        THEN the passed cursor will be closed and returned
        """):
            self.assertFalse(actual_result['error'])        
            self.assertEqual(actual_result['msg'], expected_result['msg'])
            self.assertEqual(actual_result['data'], expected_result['data']) 
    
        expected_result = {
            'msg': 'An error occurred when trying to close cursor object',
            'exception': Exception(self.generic_error)
        }

        mock_cursor.close.side_effect = expected_result['exception']

        actual_result = sql_service.close_cursor(mock_cursor, self.fake_logger)

        with self.subTest("""
        GIVEN an exception is raised
        WHEN the close() method is called
        THEN an error dictionary will be returned
        """):
            self.assertTrue(actual_result['error'])        
            self.assertTrue(expected_result['msg'] in actual_result['msg'])
            self.assertEqual(expected_result['exception'], actual_result['exception'])        
            self.assertIsNone(actual_result['data'])    

    @patch('sql_service.open')
    def test_form_select_query(self, mock_query):
        expected_result = {
            'msg': f'Successfully formed SELECT query {self.fake_select_query}',
            'data': self.fake_select_query
        }

        mock_query.return_value.read.return_value = expected_result['data']

        actual_result = sql_service.form_select_query(self.fake_table_name, self.fake_logger)

        with self.subTest("""
        GIVEN a value for table parameter is passed
        WHEN the open().read().format() method is called
        THEN the values will be used to form a select statement and returned
        """):
            self.assertFalse(actual_result['error'])        
            self.assertEqual(actual_result['msg'], expected_result['msg'])
            self.assertEqual(actual_result['data'], expected_result['data']) 

        expected_result = {
            'msg': f'Successfully formed SELECT query {self.fake_select_query}',
            'data': self.fake_select_query
        }

        mock_query.return_value.read.return_value = expected_result['data']

        actual_result = sql_service.form_select_query(self.fake_table_name, self.fake_logger, attributes =  'attr1, attr2')

        with self.subTest("""
        GIVEN values for table and attributes parameters are passed
        WHEN the open().read().format() method is called
        THEN the values will be used to form a select statement and returned
        """):
            self.assertFalse(actual_result['error'])        
            self.assertEqual(actual_result['msg'], expected_result['msg'])
            self.assertEqual(actual_result['data'], expected_result['data'])

        expected_result = {
            'msg': f'Successfully formed SELECT query {self.fake_select_query}',
            'data': self.fake_select_query
        }

        mock_query.return_value.read.return_value = expected_result['data']

        actual_result = sql_service.form_select_query(self.fake_table_name, self.fake_logger, attributes =  'attr1, attr2', where = "WHERE id = '1'")

        with self.subTest("""
        GIVEN values for table, attributes and where parameters are passed
        WHEN the open().read().format() method is called
        THEN the values will be used to form a select statement and returned
        """):
            self.assertFalse(actual_result['error'])        
            self.assertEqual(actual_result['msg'], expected_result['msg'])
            self.assertEqual(actual_result['data'], expected_result['data']) 

        expected_result = {
            'msg': f'An error occurred when trying to form SELECT query',
            'exception': Exception(self.generic_error)
        }

        mock_query.side_effect = expected_result['exception']

        actual_result = sql_service.form_select_query(self.fake_table_name, self.fake_logger)

        with self.subTest("""
        GIVEN an exception is raised
        WHEN the open().read().format() method is called
        THEN an error dictionary will be returned
        """):
            self.assertTrue(actual_result['error'])        
            self.assertTrue(expected_result['msg'] in actual_result['msg'])
            self.assertEqual(expected_result['exception'], actual_result['exception'])        
            self.assertIsNone(actual_result['data'])  

    @patch('sql_service.pyodbc')
    def test_execute_formed_query(self, mock_cursor):
        expected_result = {
            'msg': f'Successfully executed formed query {self.fake_select_query}',
            'data': self.fake_cursor
        }

        mock_cursor.execute.return_value = expected_result['data']

        actual_result = sql_service.execute_formed_query(mock_cursor, self.fake_select_query, self.fake_logger)
        with self.subTest("""
        GIVEN values for cursor and query parameters are passed
        WHEN the execute() method is called
        THEN the passed query will be executed using the passed cursor
        """):
            self.assertFalse(actual_result['error'])        
            self.assertEqual(actual_result['msg'], expected_result['msg'])
            self.assertEqual(actual_result['data'], expected_result['data'])

        expected_result = {
            'msg': f'An error occurred when trying to execute formed query',
            'exception': Exception(self.generic_error)
        }

        mock_cursor.execute.side_effect = expected_result['exception']

        actual_result = sql_service.execute_formed_query(mock_cursor, self.fake_select_query, self.fake_logger)

        with self.subTest("""
        GIVEN an exception is raised
        WHEN the execute() method is called
        THEN an error dictionary will be returned
        """):
            self.assertTrue(actual_result['error'])        
            self.assertTrue(expected_result['msg'] in actual_result['msg'])
            self.assertEqual(expected_result['exception'], actual_result['exception'])        
            self.assertIsNone(actual_result['data']) 

    def test_get_columns(self):
        expected_result = {
            'msg': f'Successfully got columns from cursor description',
            'data': self.fake_columns
        }

        actual_result = sql_service.get_columns(self.fake_cursor_description, self.fake_logger)
        
        with self.subTest("""
        GIVEN a value for cursor description parameter is passed
        WHEN the first element is fetched
        THEN the passed query will be executed using the passed cursor description
        """):
            self.assertFalse(actual_result['error'])        
            self.assertEqual(actual_result['msg'], expected_result['msg'])
            self.assertEqual(actual_result['data'], expected_result['data'])

        expected_result = {
            'msg': f'An error occurred when trying to get columns from cursor description',
            'exception': IndexError('tuple index out of range')
        }
        
        actual_result = sql_service.get_columns(((), ()), self.fake_logger)

        with self.subTest("""
        GIVEN an exception is raised
        WHEN the first element is fetched
        THEN an error dictionary will be returned
        """):
            self.assertTrue(actual_result['error'])        
            self.assertTrue(expected_result['msg'] in actual_result['msg'])
            self.assertEqual(str(expected_result['exception']), str(actual_result['exception']))        
            self.assertEqual(type(expected_result['exception']), type(actual_result['exception']))        
            self.assertIsNone(actual_result['data']) 

    @patch('sql_service.pyodbc')
    def test_get_results(self, mock_cursor):
        expected_result = {
            'msg': 'Successfully got results from cursor',
            'data': self.fake_columns
        }

        mock_cursor.fetchall.return_value = expected_result['data']

        actual_result = sql_service.get_results(mock_cursor, self.fake_logger)

        with self.subTest("""
        GIVEN a value for cursor parameter is passed
        WHEN the fetchall() method is called
        THEN the results will be fetched from the passed cursor
        """):
            self.assertFalse(actual_result['error'])        
            self.assertEqual(actual_result['msg'], expected_result['msg'])
            self.assertEqual(actual_result['data'], expected_result['data']) 

        expected_result = {
            'msg': f'An error occurred when trying to get results from cursor',
            'exception': Exception(self.generic_error)
        }

        mock_cursor.fetchall.side_effect = expected_result['exception']
        
        actual_result = sql_service.get_results(mock_cursor, self.fake_logger)

        with self.subTest("""
        GIVEN an exception is raised
        WHEN the fetchall() method is called
        THEN an error dictionary will be returned
        """):
            self.assertTrue(actual_result['error'])        
            self.assertTrue(expected_result['msg'] in actual_result['msg'])
            self.assertEqual(expected_result['exception'], actual_result['exception'])        
            self.assertIsNone(actual_result['data']) 

    @patch('sql_service.zip')
    def test_zip_columns_results(self, mock_zip):
        expected_result = {
            'msg': 'Successfully zipped columns with results',
            'data': [{'attr1': 'value1', 'attr2': 'value2'}]
        }

        mock_zip.side_effect = expected_result['data']

        actual_result = sql_service.zip_columns_results(self.fake_results, self.fake_columns, self.fake_logger)

        with self.subTest("""
        GIVEN a value for cursor parameter is passed
        WHEN the append(dict(zip)) methods are called
        THEN the results and columns will be zipped using the passed cursor
        """):
            self.assertFalse(actual_result['error'])        
            self.assertEqual(actual_result['msg'], expected_result['msg'])
            self.assertEqual(actual_result['data'], expected_result['data']) 

        expected_result = {
            'msg': 'An error occurred when trying to zip columns with results',
            'exception': Exception(self.generic_error)
        }

        mock_zip.side_effect = expected_result['exception']
        
        actual_result = sql_service.zip_columns_results(self.fake_results, self.fake_columns, self.fake_logger)

        with self.subTest("""
        GIVEN an exception is raised
        WHEN the append(dict(zip)) methods are called
        THEN an error dictionary will be returned
        """):
            self.assertTrue(actual_result['error'])        
            self.assertTrue(expected_result['msg'] in actual_result['msg'])
            self.assertEqual(expected_result['exception'], actual_result['exception'])        
            self.assertIsNone(actual_result['data']) 

    @patch('sql_service.open')
    def test_form_insert_statement(self, mock_statement):
        expected_result = {
            'msg': f'Successfully formed INSERT statement {self.fake_insert_statement}',
            'data': self.fake_insert_statement
        }

        mock_statement.return_value.read.return_value = expected_result['data']

        actual_result = sql_service.form_insert_statement(self.fake_table_name, self.fake_columns, self.fake_values, self.fake_logger)

        with self.subTest("""
        GIVEN values for table, columns and values parameters are passed
        WHEN the open().read().format() method is called
        THEN the values will be used to form an insert statement and returned
        """):
            self.assertFalse(actual_result['error'])        
            self.assertEqual(actual_result['msg'], expected_result['msg'])
            self.assertEqual(actual_result['data'], expected_result['data']) 

        expected_result = {
            'msg': f'An error occurred when trying to form INSERT statement',
            'exception': Exception(self.generic_error)
        }

        mock_statement.side_effect = expected_result['exception']

        actual_result = sql_service.form_insert_statement(self.fake_table_name, self.fake_columns, self.fake_values, self.fake_logger)

        with self.subTest("""
        GIVEN an exception is raised
        WHEN the open().read().format() method is called
        THEN an error dictionary will be returned
        """):
            self.assertTrue(actual_result['error'])        
            self.assertTrue(expected_result['msg'] in actual_result['msg'])
            self.assertEqual(expected_result['exception'], actual_result['exception'])        
            self.assertIsNone(actual_result['data'])  

    @patch('sql_service.pyodbc')
    def test_execute_formed_statement(self, mock_cursor):
        expected_result = {
            'msg': f'Successfully executed formed statement {self.fake_insert_statement}',
            'data': self.fake_cursor
        }

        mock_cursor.execute.return_value = expected_result['data']

        actual_result = sql_service.execute_formed_statement(mock_cursor, self.fake_insert_statement, self.fake_logger)
        
        with self.subTest("""
        GIVEN an exception is raised
        WHEN the execute() method is called
        THEN an error dictionary will be returned
        """):
            self.assertFalse(actual_result['error'])        
            self.assertEqual(actual_result['msg'], expected_result['msg'])
            self.assertEqual(actual_result['data'], expected_result['data'])

        expected_result = {
            'msg': f'An error occurred when trying to execute formed statement',
            'exception': Exception(self.generic_error)
        }

        mock_cursor.execute.side_effect = expected_result['exception']

        actual_result = sql_service.execute_formed_statement(mock_cursor, self.fake_insert_statement, self.fake_logger)

        with self.subTest("""
        GIVEN values for cursor and query parameters are passed
        WHEN the execute() method is called
        THEN the passed query will be executed using the passed cursor
        """):
            self.assertTrue(actual_result['error'])        
            self.assertTrue(expected_result['msg'] in actual_result['msg'])
            self.assertEqual(expected_result['exception'], actual_result['exception'])        
            self.assertIsNone(actual_result['data']) 

    @patch('sql_service.pyodbc')
    def test_commit(self, mock_cursor):
        expected_result = {
            'msg': 'Successfully committed changes',
            'data': f"{self.fake_rows_affected} row(s) affected"
        }

        mock_cursor.rowcount.return_value = self.fake_rows_affected
        mock_cursor.commit.return_value = self.fake_cursor

        actual_result = sql_service.commit(mock_cursor, self.fake_rows_affected, self.fake_logger)

        with self.subTest("""
        GIVEN a values for cursor parameter is passed
        WHEN the commit() method is called
        THEN pending changes will be committed on the passed cursor
        """):
            self.assertFalse(actual_result['error'])        
            self.assertEqual(actual_result['msg'], expected_result['msg'])
            self.assertEqual(actual_result['data'], expected_result['data'])      

        expected_result = {
            'msg': f'An error occurred when trying to commit changes',
            'exception': Exception(self.generic_error)
        }

        mock_cursor.commit.side_effect = expected_result['exception']
        
        actual_result = sql_service.commit(mock_cursor, self.fake_rows_affected, self.fake_logger)

        with self.subTest("""
        GIVEN an exception is raised
        WHEN the commit() method is called
        THEN an error dictionary will be returned
        """):
            self.assertTrue(actual_result['error'])        
            self.assertTrue(expected_result['msg'] in actual_result['msg'])
            self.assertEqual(expected_result['exception'], actual_result['exception'])        
            self.assertIsNone(actual_result['data'])

    @patch('sql_service.open')
    def test_form_update_statement(self, mock_statement):
        expected_result = {
            'msg': f'Successfully formed UPDATE statement {self.fake_update_statement}',
            'data': self.fake_update_statement
        }

        mock_statement.return_value.read.return_value = expected_result['data']

        actual_result = sql_service.form_update_statement(self.fake_table_name, self.fake_params, self.fake_where, self.fake_logger)

        with self.subTest("""
        GIVEN values for table, params and where parameters are passed
        WHEN the open().read().format() method is called
        THEN the values will be used to form an update statement and returned
        """):
            pass
            self.assertFalse(actual_result['error'])        
            self.assertEqual(actual_result['msg'], expected_result['msg'])
            self.assertEqual(actual_result['data'], expected_result['data']) 

        expected_result = {
            'msg': f'An error occurred when trying to form UPDATE statement',
            'exception': Exception(self.generic_error)
        }

        mock_statement.side_effect = expected_result['exception']

        actual_result = sql_service.form_update_statement(self.fake_table_name, self.fake_params, self.fake_where, self.fake_logger)

        with self.subTest("""
        GIVEN an exception is raised
        WHEN the open().read().format() method is called
        THEN an error dictionary will be returned
        """):
            self.assertTrue(actual_result['error'])        
            self.assertTrue(expected_result['msg'] in actual_result['msg'])
            self.assertEqual(expected_result['exception'], actual_result['exception'])        
            self.assertIsNone(actual_result['data']) 

    @patch('sql_service.open')
    def test_form_delete_statement(self, mock_statement):
        expected_result = {
            'msg': f'Successfully formed DELETE statement {self.fake_delete_statement}',
            'data': self.fake_delete_statement
        }

        mock_statement.return_value.read.return_value = expected_result['data']

        actual_result = sql_service.form_delete_statement(self.fake_table_name, self.fake_where, self.fake_logger)

        with self.subTest("""
        GIVEN values for table, params and where parameters are passed
        WHEN the open().read().format() method is called
        THEN the values will be used to form an delete statement and returned
        """):
            pass
            self.assertFalse(actual_result['error'])        
            self.assertEqual(actual_result['msg'], expected_result['msg'])
            self.assertEqual(actual_result['data'], expected_result['data']) 

        expected_result = {
            'msg': f'An error occurred when trying to form DELETE statement',
            'exception': Exception(self.generic_error)
        }

        mock_statement.side_effect = expected_result['exception']

        actual_result = sql_service.form_delete_statement(self.fake_table_name, self.fake_where, self.fake_logger)

        with self.subTest("""
        GIVEN an exception is raised
        WHEN the open().read().format() method is called
        THEN an error dictionary will be returned
        """):
            self.assertTrue(actual_result['error'])        
            self.assertTrue(expected_result['msg'] in actual_result['msg'])
            self.assertEqual(expected_result['exception'], actual_result['exception'])        
            self.assertIsNone(actual_result['data']) 

    @patch('sql_service.pyodbc')
    def test_rollback(self, mock_cursor):
        expected_result = {
            'msg': 'Successfully rolled back cursor changes',
            'data': self.fake_cursor
        }

        mock_cursor.rollback.return_value = expected_result['data']
        
        actual_result = sql_service.rollback(mock_cursor, self.fake_logger)

        with self.subTest("""
        GIVEN values for cursor parameters are passed
        WHEN the rollback() method is called
        THEN pending changes will be rolled back on the passed cursor
        """):
            self.assertFalse(actual_result['error'])        
            self.assertEqual(actual_result['msg'], expected_result['msg'])
            self.assertEqual(actual_result['data'], expected_result['data'])      

        expected_result = {
            'msg': f'An error occurred when trying to rollback cursor change',
            'exception': Exception(self.generic_error)
        }

        mock_cursor.rollback.side_effect = expected_result['exception']
        
        actual_result = sql_service.rollback(mock_cursor, self.fake_logger)

        with self.subTest("""
        GIVEN an exception is raised
        WHEN the rollback() method is called
        THEN an error dictionary will be returned
        """):
            self.assertTrue(actual_result['error'])        
            self.assertTrue(expected_result['msg'] in actual_result['msg'])
            self.assertEqual(expected_result['exception'], actual_result['exception'])        
            self.assertIsNone(actual_result['data'])

if __name__ == "__main__":
    unittest.main()