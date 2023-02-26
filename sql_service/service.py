import pyodbc

select_query_file = "sql_service\\queries\\select_from_table.sql"
insert_statement_file = "sql_service\\queries\\select_from_table.sql"
update_statement_file = "sql_service\\queries\\update_table.sql"
delete_statement_file = "sql_service\\queries\\delete_statement.sql"

def form_conn_string(driver, server, database, username, password, logger):
    try:
        logger.info(f"SQL_SVC_FRM_CONN: Attempting to form connection string")

        conn_string = 'DRIVER={'+driver+'};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password
       
        msg = f'Successfully formed connection string'
        logger.info(f"SQL_SVC_FRM_CONN: {msg}")

        return {
            'error': False,
            'msg': msg,
            'data': conn_string
        }

    except Exception as e:
        msg =f'An error occurred when trying to form connection string, {e}'
        logger.error(f"SQL_SVC_FRM_CONN_ERR: {msg}")

        return {
            'error': True,
            'msg': msg,
            'exception': e,
            'data': None
        }

def connect(conn_string, logger):
    logger.info(f"SQL_SVC_CONN: Attempting to connect to database using pyodbc")
    try:
        conn = pyodbc.connect(conn_string)

        msg = f'Successfully connected to database using pyodbc'
        logger.info(f"SQL_SVC_CONN: {msg}")

        return {
            'error': False,
            'msg': msg,
            'data': conn
        }

    except Exception as e:
        msg =f'An error occurred when trying to connect to database, {e}'
        logger.error(f"SQL_SVC_CONN_ERR: {msg}")

        return {
            'error': True,
            'msg': msg,
            'exception': e,
            'data': None
        }

def create_cursor(conn, logger):
    logger.info("SQL_SVC_CRT_CSR: Attempting to create pyodbc cursor")
    try:
        cursor = conn.cursor()

        msg = 'Successfully created pyodbc cursor object'
        logger.info(f"SQL_SVC_CRT_CSR: {msg}")

        return {
            'error': False,
            'msg': msg,
            'data': cursor
        }
    
    except Exception as e:
        msg =f'An error occurred when trying to create cursor object, {e}'
        logger.error(f"SQL_SVC_CRT_CSR_ERR: {msg}")
        
        return {
            'error': True,
            'msg': msg,
            'exception': e,
            'data': None
        }

def close_cursor(cursor, logger):
    logger.info("SQL_SVC_CLS_CSR: Attempting to close pyodbc cursor")
    try:
        cursor2 = cursor.close()

        msg = 'Successfully closed cursor object'
        logger.info(f"SQL_SVC_CLS_CSR: {msg}")

        return {
            'error': False,
            'msg': msg,
            'data': cursor2
        }
       
    except Exception as e:
        msg =f'An error occurred when trying to close cursor object, {e}'
        logger.error(f"SQL_SVC_CLS_CSR_ERR: {msg}")

        return {
            'error': True,
            'msg': msg,
            'exception': e,
            'data': None
        }

def form_select_query(table, logger, file = select_query_file, attributes="*", where = ""):
    logger.info("SQL_SVC_FRM_SLT: Attempting to form SELECT query")
    try:
        query = open(file).read().format(attributes, table, where)
        
        msg = f'Successfully formed SELECT query {query}'
        logger.info(f"SQL_SVC_FRM_QRY: {msg}")
        
        return {
            'error': False,
            'msg': msg,
            'data': query
        }
    
    except Exception as e:
        msg =f'An error occurred when trying to form SELECT query, {e}'
        logger.error(f"SQL_SVC_FRM_QRY_ERR: {msg}")

        return {
            'error': True,
            'msg': msg,
            'exception': e,
            'data': None
        }

def execute_formed_query(cursor, query, logger):
    logger.info("SQL_SVC_ECT_QRY: Attempting to execute formed query")
    try:
        cursor = cursor.execute(query)
        
        msg = f'Successfully executed formed query {query}'
        logger.info(f"SQL_SVC_ECT_QRY: {msg}")

        return {
            'error': False,
            'msg': msg,
            'data': cursor
        }

    except Exception as e:
        msg =f'An error occurred when trying to execute formed query, {e}'
        logger.error(f"SQL_SVC_ECT_QRY_ERR: {msg}")

        return {
            'error': True,
            'msg': msg,
            'exception': e,
            'data': None
        }

def get_columns(cursor_description, logger):
    logger.info("SQL_SVC_GT_CLS: Attempting to get columns from cursor description")
    try:
        columns = [column[0] for column in cursor_description]

        msg = f'Successfully got columns from cursor description'
        logger.info(f"SQL_SVC_GT_CLS: {msg}")

        return {
            'error': False,
            'msg': msg,
            'data': columns
        }

    except IndexError as e:
        msg =f'An error occurred when trying to get columns from cursor description, {e}'
        logger.error(f"SQL_SVC_GT_CLS_ERR: {msg}")

        return {
            'error': True,
            'msg': msg,
            'exception': e,
            'data': None
        }

def get_results(cursor, logger):
    logger.info("SQL_SVC_GT_RLS: Attempting to get results from cursor")
    try:
        results = cursor.fetchall()

        msg = f'Successfully got results from cursor'
        logger.info(f"SQL_SVC_GT_RLS: {msg}")

        return {
            'error': False,
            'msg': msg,
            'data': results
        }

    except Exception as e:
        msg =f'An error occurred when trying to get results from cursor, {e}'
        logger.error(f"SQL_SVC_GT_RLS_ERR: {msg}")
        return {
            'error': True,
            'msg': msg,
            'exception': e,
            'data': None
        }

def zip_columns_results(results, columns, logger):
    logger.info("SQL_SVC_ZP_CLM: Attempting to zip columns with results")
    
    try: 
        zipped_results = []
        for row in results:
            zipped_results.append(dict(zip(columns, row)))
        
        msg = f'Successfully zipped columns with results'
        logger.info(f"SQL_SVC_ZP_CLM: {msg}")

        return {
            'error': False,
            'msg': msg,
            'data': zipped_results
        }
    
    except Exception as e:
        msg =f'An error occurred when trying to zip columns with results, {e}'
        logger.error(f"SQL_SVC_ZP_CLM_ERR: {msg}")

        return {
            'error': True,
            'msg': msg,
            'exception': e,
            'data': None
        }

def form_insert_statement(table, columns, values, logger, file = insert_statement_file):
    logger.info("SQL_SVC_FRM_IST: Attempting to form INSERT statement")
    try:
        statement = open(file).read().format(table, columns, values)
        
        msg = f'Successfully formed INSERT statement {statement}'
        logger.info(f"SQL_SVC_FRM_SMT: {msg}")

        return {
            'error': False,
            'msg': msg,
            'data': statement
        }
    
    except Exception as e:        
        msg =f'An error occurred when trying to form INSERT statement, {e}'
        logger.error(f"SQL_SVC_FRM_SMT_ERR: {msg}")

        return {
            'error': True,
            'msg': msg,
            'exception': e,
            'data': None
        }

def execute_formed_statement(cursor, statement, logger):
    logger.info("SQL_SVC_ECT_SMT: Attempting to execute formed statement")
    try:
        cursor = cursor.execute(statement)

        msg = f'Successfully executed formed statement {statement}'
        logger.info(f"SQL_SVC_ECT_SMT: {msg}")
        
        return {
            'error': False,
            'msg': msg,
            'data': cursor
        }

    except Exception as e:
        msg =f'An error occurred when trying to execute formed statement, {e}'
        logger.error(f"SQL_SVC_ECT_SMT_ERR: {msg}")

        return {
            'error': True,
            'msg': msg,
            'exception': e,
            'data': None
        }

def commit(cursor, rows_affected, logger):
    logger.info("SQL_SVC_CMT: Attempting to commit changes")
    try:
        cursor = cursor.commit()

        msg = f'Successfully committed changes'
        logger.info(f"SQL_SVC_CMT: {msg}")

        return {
            'error': False,
            'msg': msg,
            'data':  f"{rows_affected} row(s) affected"
        }
    
    except Exception as e:
        msg =f'An error occurred when trying to commit changes, {e}'
        logger.error(f"SQL_SVC_CMT_ERR: {msg}")

        return {
            'error': True,
            'msg': msg,
            'exception': e,
            'data': None
        }

def form_update_statement(table, params, where, logger, file = update_statement_file):
    logger.info("SQL_SVC_FRM_UDT: Attempting to form UPDATE statement")
    try:
        statement = open(file).read().format(table, params, where)
       
        msg = f'Successfully formed UPDATE statement {statement}'
        logger.info(f"SQL_SVC_FRM_UDT: {msg}")

        return {
            'error': False,
            'msg': msg,
            'data': statement
        }
    
    except Exception as e:
        msg =f'An error occurred when trying to form UPDATE statement, {e}'
        logger.error(f"SQL_SVC_FRM_UDT_ERR: {msg}")
        
        return {
            'error': True,
            'msg': msg,
            'exception': e,
            'data': None
        }

def form_delete_statement(table, where, logger, file = delete_statement_file):
    logger.info("SQL_SVC_FRM_DLT: Attempting to form DELETE statement")    
    try:
        statement = open(file).read().format(table, where)

        msg = f'Successfully formed DELETE statement {statement}'
        logger.info(f"SQL_SVC_FRM_DLT: {msg}")

        return {
            'error': False,
            'msg': msg,
            'data': statement
        }
    
    except Exception as e:
        msg =f'An error occurred when trying to form DELETE statement, {e}'
        logger.error(f"SQL_SVC_FRM_DLT_ERR: {msg}")
        
        return {
            'error': True,
            'msg': msg,
            'exception': e,
            'data': None
        }

def rollback(cursor, logger):
    logger.info("SQL_SVC_RBK: Attempting to rollback cursor changes")    
    try:
        cursor = cursor.rollback()

        msg = f'Successfully rolled back cursor changes'
        logger.info(f"SQL_SVC_RBK: {msg}")

        return {
            'error': False,
            'msg': msg,
            'data': cursor
        }
    
    except Exception as e:
        msg =f'An error occurred when trying to rollback cursor changes, {e}'
        logger.error(f"SQL_SVC_RBK_ERR: {msg}")

        return {
            'error': True,
            'msg': msg,
            'exception': e,
            'data': None
        }
