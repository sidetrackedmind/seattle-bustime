import os
import pandas as pd
import numpy as np
import psycopg2
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import boto3
import pickle

def route_to_train(route_short_name, stop_name, direction):
    '''This is a function to process user input into the model format
    INPUT
    -------


    OUTPUT
    -------
    trained model
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
            from route_select
            where route_short_name = '{}'
            and stop_name = '{}'
            and direction_id = {} '''.format(route_short_name,
                                        stop_name,
                                        direction)

    print('finding route_id and direction_id')

    cur.execute(query)
    query_list = cur.fetchall()
    route_id = query_list[0][0]
    route_dir = str(route_id) + '_' + str(direction)
    pickle_path = build_filename(route_id, direction)

    model_col_list = ['route_dir_stop','stop_sequence','month', 'day', 'hour','dow','delay']

    select_string = column_list_to_string(model_col_list)

    query = '''
            select {}
            from updates
            where route_id = {}
            and direction_id = {}
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
    gbr = GradientBoostingRegressor(n_estimators=1500,
                                    learning_rate=0.05)
    print('starting model fit')
    fit_model = gbr.fit(X_train, y_train)
    print('model fit complete')
    put_pickle_model(fit_model, pickle_path)

    update_model_database(conn, all_columns_str, pickle_path, route_dir)
    predictions = fit_model.predict(X_test)
    error = mean_squared_error(predictions, y_test)
    return fit_model, predictions, y_test, error**(1/2), all_columns_str

def column_list_to_string(list):
    column_str = ''
    for i, col in enumerate(list):
        if i == 0:
            column_str += str(col)
        else:
            column_str += ","+str(col)
    return column_str

def build_filename(route_id, direction):
    prefix = 'models/'
    route_dir = str(route_id) + '_' + str(direction) + '/'
    suffix = 'model.pkl'
    filename = prefix + route_dir + suffix
    return filename

def put_pickle_model(fit_model, filename):
        '''

        Output:
        -------
        Writes pickled model to route_direction specific
        s3 bucket location
        '''


        with open('model.pkl', 'wb') as f:
            pickle.dump(fit_model, f)

        bucket_name = os.environ["BUS_BUCKET_NAME"]

        s3 = boto3.client('s3')

        s3.put_object(Bucket=bucket_name,
                        Body=open('model.pkl', 'rb'), Key=filename)

def update_model_database(conn, all_columns, pickle_path, route_dir):
    cur = conn.cursor()

    cur.execute("UPDATE models "
                "SET pickle_path = (%s),"
                    "model_columns = (%s)"
                    "WHERE route_dir  = (%s)",
                    (pickle_path, all_columns, route_dir))
    conn.commit()
