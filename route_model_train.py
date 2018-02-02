import os
import pandas as pd
import numpy as np
import psycopg2
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import boto3
import pickle

def route_to_train(route_dir, n_estimators=1500,
            learning_rate=0.05, put_pickle=False, up_model_db=False):
    '''This is a function to process user input into the model format
    INPUT
    -------


    OUTPUT
    -------
    fit_model,
    predictions,
    y_test,
    error**(1/2),
    all_columns_str
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

    route_id = route_dir.split("_")[0]
    direction = route_dir.split("_")[1]
    pickle_path = build_filename(route_id, direction)

    model_col_list = ['route_dir_stop','stop_sequence','month', 'day', 'hour','dow','delay']

    select_string = column_list_to_string(model_col_list)

    query = '''
            select {}
            from updates
            where route_id = {}
            and direction_id = {}
            and time_pct < '2018-01-23'
            '''.format(select_string, route_id, direction)

    print('getting historical route information')
    cur.execute(query)
    query_list = cur.fetchall()
    result = pd.DataFrame(query_list, columns=model_col_list)
    y = (result.iloc[:,-1].values)/60
    result = result.drop('delay', axis=1)
    dummy_col = ['route_dir_stop','month', 'day', 'hour','dow']
    result_dummies = pd.get_dummies(result,columns=dummy_col)
    all_column_list = list(result_dummies.columns)
    all_columns_str = column_list_to_string(all_column_list)
    X = result_dummies.values
    X_train, X_test, y_train, y_test = train_test_split(X, y,
                                    test_size=0.15, random_state=128)
    gbr = GradientBoostingRegressor(n_estimators=n_estimators,
                                    learning_rate=learning_rate)
    print('starting model fit')
    fit_model = gbr.fit(X_train, y_train)
    print('model fit complete')

    if put_pickle:
        put_pickle_model(fit_model, pickle_path)
        update_model_database(conn, all_columns_str, pickle_path, route_dir)

    predictions = fit_model.predict(X_test)
    error = mean_squared_error(predictions, y_test)

    cur.close()
    conn.close()

    return fit_model, predictions, y_test, error**(1/2), all_columns_str




def get_route_metrics(route_short_name, stop_name, direction):
    '''This is a function to process user input
    grab a pickled model and output stop metrics
    INPUT
    -------
    route_short_name = route name e.g. "7" or "76"
    stop_name = stop name e.g. "1st Ave & Broad St"
    direction = which direction the bus is going on the route (0,1)

    OUTPUT
    -------
    route_df
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

    query = '''
            select route_id
            from route_all
            where route_short_name = '{}'
            and stop_name = '{}'
            and direction_id = {}
            LIMIT 1'''.format(route_short_name,
                                        stop_name,
                                        direction)

    print('finding route_id and direction_id')

    cur.execute(query)
    query_list = cur.fetchall()
    route_id = query_list[0][0]
    route_dir = str(route_id) + '_' + str(direction)
    pickle_path = build_filename(route_id, direction)

    model_col_list = ['route_dir_stop','stop_id','stop_name','stop_sequence','month', 'day', 'hour','dow','delay']

    select_string = column_list_to_string(model_col_list)

    query = '''
            select {}
            from updates
            where route_id = {}
            and direction_id = {}
            '''.format(select_string, route_id, direction)

    print('getting historical route information for {}-{}'.format(route_id, direction))
    cur.execute(query)
    query_list = cur.fetchall()
    result_df = pd.DataFrame(query_list, columns=model_col_list)
    '''y_array = (result.iloc[:,-1].values)/60
    result = result.drop('delay', axis=1)
    X_array = result.values'''

    cur.close()
    conn.close()

    return result_df

def column_list_to_string(list):
    column_str = ''
    for i, col in enumerate(list):
        if i == 0:
            column_str += str(col)
        else:
            column_str += ","+str(col)
    return column_str
