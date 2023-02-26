import ast
import uuid
from datetime import datetime
import logging
import logging.config

import time


def create_logger():
    logging.config.dictConfig({'version': 1, 'disable_existing_loggers': True,})
    logger = logging

    log = logging.getLogger('werkzeug')
    log.disabled = True

    timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")

    logger.basicConfig(filename = f'C:/Users/caola/Documents/Github/mssqlserver-python/sql_service/logs{timestamp}.log',
        filemode='w',
        level = logging.INFO,
        format = '%(asctime)s | %(levelname)s | %(message)s',
        datefmt = '%Y/%m/%d %H:%M:%S'
    )

    return logger

def generate_uuid():
    generated_uuid = uuid.uuid4()
    
    return generated_uuid

def is_valid_uuid(value):
    try:
        uuid.UUID(value)
 
        return True
    
    except ValueError:
        return False

def current_time():
    current_time = datetime.now().strftime("%Y/%m/%d %H:%M:%S.%f")

    return current_time 

def element_in_object(element, object):
    try:
        object[element]

        return True

    except KeyError as ke:
        #print("No WHERE clause parameters provided. Executing without SQL WHERE")
        return False

def join_list(list):
    try:
        joined_list = ",".join(list)
    except Exception as e:
        raise RuntimeError(e)

    return joined_list

def listify_string(list):
    try:
        joined_list = list.replace(",", "','")
    except Exception as e:
        raise RuntimeError(e)

    try:
        joined_list = "".join(("'", joined_list, "'"))
    except Exception as e:
        raise RuntimeError(e)

    return joined_list

def unpack_dict_list(dict_list):
    params_list = []

    try:
        for dict in dict_list:
            params_list.append(unpack_dict(dict))
    except Exception as e:
        raise RuntimeError(e)

    try:
        params = join_list(params_list)
    except Exception as e:
        raise RuntimeError(e)

    return params

def unpack_dict(dict):
    try:
        key = list(dict.keys())[0]
    except Exception as e:
        raise RuntimeError(e)
    return f"{key} = '{dict[key]}'"



def comma_split(string):
    if string:
        string.split(',')
        return string

def get_params(params):
    if params:
        params = params.replace('params', '')
        params = params.replace('[', '')
        params = params.replace(']', '')
        params = "{'" + params + "'}"
        params = params.replace('=', "':'")
        params = params.replace('&', "'},{'")
        params = params.split(",")

        params_list = []

        for param in params:
            params_list.append(ast.literal_eval(param))

        return params_list

def is_key(args):
    if args:
        return args