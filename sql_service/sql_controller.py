from sql_service import utils
from sql_service import sql_service

class SqlController():

    def __init__(self, params, transaction_id, logger, driver, server, database, username, password):
        self.driver = driver
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.logger = logger
        
        self.cursor = self.connect()
        
        self.transaction_id = transaction_id
        self.params = params

    def connect(self):
        conn_string = sql_service.form_conn_string(self.driver, self.server, self.database, self.username, self.password, self.logger)
        if conn_string['error']:
            raise RuntimeError("Error when forming connection string")

        conn =  sql_service.connect(conn_string['data'], self.logger)       
        if conn['error']:
            raise ConnectionError(conn['exception'])
        
        cursor = sql_service.create_cursor(conn['data'], self.logger)
        if cursor['error']:
            raise ConnectionError(cursor['exception'])

        self.cursor = cursor['data']

        return cursor['data']

    def close(self):
        close_cursor = sql_service.close_cursor(self.cursor, self.logger)
        
        if close_cursor['error']:
            self.logger.error(f"SQL_CLR_CLS: Closing cursor failed. Retrying")
            
            close_cursor = sql_service.close_cursor(self.cursor, self.logger)

            if close_cursor['error']:
                raise Exception(close_cursor['exception'])
            
        return close_cursor['data']

    def delete(self):
        statement = sql_service.form_delete_statement(table = self.params['table'], where = self.params['where'], logger = self.logger)
        if statement['error']:
            raise OSError(statement['exception'])

        result = sql_service.execute_formed_statement(self.cursor, statement['data'], self.logger)
        if result['error']:
            self.rollback()
            raise Exception(result['exception'])
        
        commit = sql_service.commit(result['data'], result['data'].rowcount, self.logger)
        if commit['error']:
            self.rollback()
            raise Exception(commit['exception'])

        return commit

    def insert(self):
        params = utils.listify_string(self.params['values'])

        statement = sql_service.form_insert_statement(self.params['table'], self.params['columns'], params, self.logger)
        if statement['error']:
            raise OSError(statement['exception'])

        result = sql_service.execute_formed_statement(self.cursor, statement['data'], self.logger)
        if result['error']:
            self.rollback()

            raise Exception(result['exception'])

        commit = sql_service.commit(result['data'], result['data'].rowcount, self.logger)
        if commit['error']:
            self.rollback()
            raise Exception(commit['exception'])

        return commit

    def select(self):
        query = sql_service.form_select_query(self.params['table'], attributes = self.params['columns'], where = self.params['where'], logger = self.logger)
        if query['error']:
            raise OSError(query['exception'])

        query_results = sql_service.execute_formed_query(self.cursor, query['data'], self.logger)
        if query_results['error']:
            self.rollback()
            raise Exception(query_results['exception'])

        columns = sql_service.get_columns(self.cursor.description, self.logger)
        if columns['error']:
            raise Exception(columns['exception'])

        results = sql_service.get_results(self.cursor, self.logger)
        if results['error']:
            raise Exception(results['exception'])

        results_cols = sql_service.zip_columns_results(results['data'], columns['data'], self.logger)
        if results_cols['error']:
            raise Exception(results_cols['exception'])

        return results_cols['data']

    def update(self):
        if not self.params['where']:
            self.params['where'] = ""
        else:
            self.params['where'] = "WHERE " + self.params['where']

        params = utils.unpack_dict_list(self.params['params'])

        statement = sql_service.form_update_statement(self.params['table'], params, self.params['where'], self.logger)
        if statement['error']:
            raise OSError(statement['exception'])

        result = sql_service.execute_formed_statement(self.cursor, statement['data'], self.logger)
        if result['error']:
            self.rollback()
            raise Exception(result['exception'])

        commit = sql_service.commit(result['data'], result['data'].rowcount, self.logger)
        if commit['error']:
            self.rollback()
            raise Exception(commit['exception'])

        return commit

    def rollback(self):
        response = sql_service.rollback(self.cursor, self.logger)
        
        if response['error']:
            self.logger.error(f"SQL_CLR_RBK: Rollback failed. Retrying")

            response = sql_service.rollback(self.cursor, self.logger)

        return response['msg']