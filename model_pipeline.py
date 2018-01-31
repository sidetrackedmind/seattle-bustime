import os
import pandas as pd
import numpy as np
import psycopg2
from sklearn.ensemble import GradientBoostingRegressor
import model_predict
from datetime import datetime




def dashboard_pipe(route_short_name, stop_name, direction, date_string, user_hour):
    '''This is a function to process user input into the model format
    INPUT
    -------
    string inputs from web dashboard.
    route_short_name - the user will pick a route
    stop_name - the user will pick a desired stop along the route
    month - month of request
    day - day of request
    hour - hour of request - v2 should have bus schedule dropdown
    dow - day of week based on the month/day input

    OUTPUT
    -------
    prediction -- bus arrival prediction in minutes
    -- positive is delay
    -- negative is early
    -- zero is "on schedule"
    '''

    db_name = os.environ["RDS_NAME"]
    user = os.environ["RDS_USER"]
    key = os.environ["RDS_KEY"]
    host = os.environ["RDS_HOST"]
    port = os.environ["RDS_PORT"]

    conn = psycopg2.connect(dbname=db_name,
                            user=user,
                            password=key,
                            host=host,
                            port=port)
    cur = conn.cursor()
    query_col_list = ['route_dir_stop','stop_sequence', 'route_id']
    select_string = column_list_to_string(query_col_list)
    query = '''
            select {}
            from stop_info
            where route_short_name = '{}'
            and stop_name = '{}' '''.format(select_string, route_short_name, stop_name)

    cur.execute(query)
    query_list = cur.fetchone()
    print(query_list)
    route_dir_stop = query_list[0]
    stop_sequence = query_list[1]
    route_id = query_list[2]
    route_dir = str(route_id) + '_' + str(direction)

    date = datetime.strptime(date_string, '%Y-%m-%d')
    hour = user_hour
    day = date.day
    month = date.month
    dow = date.weekday()

    query = '''
            select model_columns, pickle_path
            from models
            where route_dir = '{}' '''.format(route_dir)

    cur.execute(query)
    query_list = cur.fetchall()

    all_columns = query_list[0][0].strip('{}').split(',')
    pickle_path = query_list[0][1]

    dummy_cols = ['route_dir_stop','month', 'day', 'hour','dow']
    user_input_values = [route_dir_stop, float(stop_sequence), month, day, hour, dow]
    model_col_list = ['route_dir_stop','stop_sequence','month', 'day', 'hour','dow']
    user_input = pd.DataFrame(columns=model_col_list)
    user_input.loc[0] = user_input_values
    user_input_dummies = pd.get_dummies(user_input, columns=dummy_cols)
    user_columns = list(user_input_dummies.columns)
    for col in all_columns:
        if col not in user_columns:
            user_input_dummies[col] = 0
    user_input_fixed = user_input_dummies[all_columns]
    X_array = user_input_fixed.values

    prediction = model_predict.model_predict(X_array, pickle_path)

    cur.close()
    conn.close()

    return prediction

def column_list_to_string(list):
    column_str = ''
    for i, col in enumerate(list):
        if i == 0:
            column_str += str(col)
        else:
            column_str += ", "+str(col)
    return column_str
