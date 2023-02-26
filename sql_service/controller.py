from sql_service import utils
from sql_service import service

class SqlService():

    def __init__(self, statement_type, args, driver, server, database, username, password):
        self.logger = utils.create_logger()

        self.statement_type = statement_type.upper()
        self.transaction_id = utils.generate_uuid()

        self.driver = driver
        self.server = server
        self.database = database
        self.username = username
        self.password = password

        self.params = {
            'table': args['table']
        }

        self.params['columns'] = utils.comma_split(args['columns'])
        self.params['values'] = utils.comma_split(args['values'])
        self.params['params'] = utils.get_params(args['params'])
        self.params['where'] = utils.is_key(args['where'])

        self.valid_request = self.is_valid()

        if not self.valid_request is True:
            raise ValueError(self.valid_request)

        self.cursor = self.connect()

    def is_valid(self):
        if not (self.statement_type == "DELETE" or self.statement_type == "INSERT" or self.statement_type == "SELECT" or self.statement_type == "UPDATE"):
            return f"Validating values for passed parameters. An invalid value '{self.statement_type.lower()}' was passed for statement_type. You must use a valid option: select, insert, update or delete."

        if self.statement_type == "DELETE" and (self.params['table'] is None or self.params['where'] is None):
            return f"Validating values for passed parameters to execute DELETE statement. One or more parameters is missing. Provide values for'statement_type', 'table' and 'where' parameters."
        
        elif self.statement_type == "INSERT" and (self.params['table'] is None or self.params['columns'] is None or self.params['values'] is None):
            return f"Validating values for passed parameters to execute INSERT statement. One or more parameters is missing. Provide values for'statement_type', 'table', 'columns' and 'values' parameters."

        elif self.statement_type == "SELECT" and (self.params['table'] is None or self.params['columns'] is None):
            return f"Validating values for passed parameters to execute SELECT statement. One or more parameters is missing. Provide values for'statement_type', 'table' and 'columns' parameters."

        elif self.statement_type == "UPDATE" and (self.params['table'] is None or self.params['params'] is None):
            return f"Validating values for passed parameters to execute UPDATE statement. One or more parameters is missing. Provide values for'statement_type', 'table', 'columns' and 'params' parameters."

        return True
    
    def sql_handler(self):
        try:
            if self.statement_type == "DELETE":
                result = self.delete()

            elif self.statement_type == "INSERT":
                result = self.insert()

            elif self.statement_type == "SELECT":
                result = self.select()

            elif self.statement_type == "UPDATE":
                result = self.update()
           
        except (OSError, Exception) as e:
            self.close()
            raise Exception(e)
        
        self.close()

        return result

    def connect(self):
        conn_string = service.form_conn_string(self.driver, self.server, self.database, self.username, self.password, self.logger)
        if conn_string['error']:
            raise RuntimeError("Error when forming connection string")

        conn =  service.connect(conn_string['data'], self.logger)       
        if conn['error']:
            raise ConnectionError(conn['exception'])
        
        cursor = service.create_cursor(conn['data'], self.logger)
        if cursor['error']:
            raise ConnectionError(cursor['exception'])

        self.cursor = cursor['data']

        return cursor['data']

    def close(self):
        close_cursor = service.close_cursor(self.cursor, self.logger)
        
        if close_cursor['error']:
            self.logger.error(f"SQL_CLR_CLS: Closing cursor failed. Retrying")
            
            close_cursor = service.close_cursor(self.cursor, self.logger)

            if close_cursor['error']:
                raise Exception(close_cursor['exception'])
            
        return close_cursor['data']

    def delete(self):
        statement = service.form_delete_statement(table = self.params['table'], where = self.params['where'], logger = self.logger)
        if statement['error']:
            raise OSError(statement['exception'])

        result = service.execute_formed_statement(self.cursor, statement['data'], self.logger)
        if result['error']:
            self.rollback()
            raise Exception(result['exception'])
        
        commit = service.commit(result['data'], result['data'].rowcount, self.logger)
        if commit['error']:
            self.rollback()
            raise Exception(commit['exception'])

        return commit

    def insert(self):
        params = utils.listify_string(self.params['values'])

        statement = service.form_insert_statement(self.params['table'], self.params['columns'], params, self.logger)
        if statement['error']:
            raise OSError(statement['exception'])

        result = service.execute_formed_statement(self.cursor, statement['data'], self.logger)
        if result['error']:
            self.rollback()

            raise Exception(result['exception'])

        commit = service.commit(result['data'], result['data'].rowcount, self.logger)
        if commit['error']:
            self.rollback()
            raise Exception(commit['exception'])

        return commit

    def select(self):
        query = service.form_select_query(self.params['table'], attributes = self.params['columns'], where = self.params['where'], logger = self.logger)
        if query['error']:
            raise OSError(query['exception'])

        query_results = service.execute_formed_query(self.cursor, query['data'], self.logger)
        if query_results['error']:
            self.rollback()
            raise Exception(query_results['exception'])

        columns = service.get_columns(self.cursor.description, self.logger)
        if columns['error']:
            raise Exception(columns['exception'])

        results = service.get_results(self.cursor, self.logger)
        if results['error']:
            raise Exception(results['exception'])

        results_cols = service.zip_columns_results(results['data'], columns['data'], self.logger)
        if results_cols['error']:
            raise Exception(results_cols['exception'])

        return results_cols['data']

    def update(self):
        if not self.params['where']:
            self.params['where'] = ""
        else:
            self.params['where'] = "WHERE " + self.params['where']

        params = utils.unpack_dict_list(self.params['params'])

        statement = service.form_update_statement(self.params['table'], params, self.params['where'], self.logger)
        if statement['error']:
            raise OSError(statement['exception'])

        result = service.execute_formed_statement(self.cursor, statement['data'], self.logger)
        if result['error']:
            self.rollback()
            raise Exception(result['exception'])

        commit = service.commit(result['data'], result['data'].rowcount, self.logger)
        if commit['error']:
            self.rollback()
            raise Exception(commit['exception'])

        return commit

    def rollback(self):
        response = service.rollback(self.cursor, self.logger)
        
        if response['error']:
            self.logger.error(f"SQL_CLR_RBK: Rollback failed. Retrying")

            response = service.rollback(self.cursor, self.logger)

        return response['msg']