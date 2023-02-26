from sql_service import sql_controller
from sql_service import utils

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

        try:
            self.controller = sql_controller.SqlController(self.params, self.transaction_id, self.logger, self.driver, self.server, self.database, self.username, self.password)
        
        except ConnectionError as ce:
            raise ConnectionError(ce)

    def is_valid(self):
        if not (self.statement_type == "DELETE" or self.statement_type == "INSERT" or self.statement_type == "SELECT" or self.statement_type == "UPDATE"):
            return f"Trying to handle request. An invalid endpoint '{self.statement_type.lower()}' was called. Use a valid option: select/, insert/, update/ or delete/"

        if self.statement_type == "DELETE" and (self.params['table'] is None or self.params['where'] is None):
            return f"Trying to execute DELETE statement. One or more parameters is missing. Provide values for'statement_type', 'table' and 'where' parameters in request body."
        
        elif self.statement_type == "INSERT" and (self.params['table'] is None or self.params['columns'] is None or self.params['values'] is None):
            return f"Trying to execute INSERT statement. One or more parameters is missing. Provide values for'statement_type', 'table', 'columns' and 'values' parameters in request body."

        elif self.statement_type == "SELECT" and (self.params['table'] is None or self.params['columns'] is None):
            return f"Trying to execute SELECT statement. One or more parameters is missing. Provide values for'statement_type', 'table' and 'columns' parameters in request body."

        elif self.statement_type == "UPDATE" and (self.params['table'] is None or self.params['params'] is None):
            return f"Trying to execute UPDATE statement. One or more parameters is missing. Provide values for'statement_type', 'table', 'columns' and 'params' parameters in request body."

        return True
    
    def sql_handler(self):
        try:
            if self.statement_type == "DELETE":
                result = self.controller.delete()

            elif self.statement_type == "INSERT":
                result = self.controller.insert()

            elif self.statement_type == "SELECT":
                result = self.controller.select()

            elif self.statement_type == "UPDATE":
                result = self.controller.update()
           
        except (OSError, Exception) as e:
            self.controller.close()
            raise Exception(e)
        
        self.controller.close()

        return result