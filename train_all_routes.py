import os
import pandas as pd
import numpy as np
import psycopg2
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import boto3
import pickle
import multiprocessing

def train_all_routes():
    '''This is a function to train all routes
    INPUT
    -------
    NONE

    OUTPUT
    -------
    None
    updated pickle model in S3
    updated train column in RDS "models" table
    '''

    route_dir_list = get_route_dir_list()

    n_pools = multiprocessing.cpu_count() - 2

    pool = multiprocessing.Pool(n_pools)
    pool.map(train_one_route, route_dir_list)

def train_one_route(route_dir):
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

    #check if route has been cross validated
    #if cross_val_check(conn, route_dir):
    if trained_check(conn, route_dir):

        #mark route as "in progress"
        route_in_progress(conn, route_dir)

        best_depth = get_params_from_db(conn, route_dir)

        cur = conn.cursor()

        query = '''
                select distinct(route_id), direction_id
                from route_info
                where route_dir = '{}'
                '''.format(route_dir)

        print('finding {}'.format(route_dir))

        cur.execute(query)
        query_list = cur.fetchall()
        route_id = query_list[0][0]
        direction = query_list[0][1]
        pickle_path = build_filename(route_id, direction)

        model_col_list = ['route_dir_stop','stop_sequence','month', 'day', 'hour','dow','delay']

        select_string = column_list_to_string(model_col_list)

        query = '''
                select {}
                from updates
                where route_id = {}
                and direction_id = {}
                '''.format(select_string, route_id, direction)

        print('getting historical route information for {}'.format(route_dir))
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

        gbr = GradientBoostingRegressor(loss='quantile',
                                        n_estimators=5000,
                                        learning_rate=0.005,
                                        max_depth=best_depth,
                                        subsample=0.5,
                                        alpha=0.75,
                                        random_state=128)

        print('starting model fit for {}_{}'.format(route_id, direction))
        fit_model = gbr.fit(X, y)
        print('model fit complete for {}_{}'.format(route_id, direction))
        put_pickle_model(fit_model, pickle_path)

        update_model_database(conn, all_columns_str, pickle_path, route_dir)
        print('database updated for {}_{}'.format(route_id, direction))
        mark_as_finished(conn, route_dir)

        cur.close()
        conn.close()
    else:
        pass

def get_route_dir_list():
    '''
    INPUT
    -------
    none

    OUTPUT
    --------
    route_dir_list
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
            select distinct (route_dir)
            from route_info
             '''

    cur.execute(query)
    route_dir_query = cur.fetchall()
    route_dir_list = []

    for item in route_dir_query:
        route_dir = item[0]
        route_dir_list.append(route_dir)

    return route_dir_list

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

def get_params_from_db(conn, route_dir):
    '''
    getting best_depth from model_params table

    OUTPUT
    -------
    best_depth - best depth value for this route based on CV
    '''
    cur = conn.cursor()

    cur.execute("SELECT best_depth "
                "FROM model_params "
                "WHERE route_dir  = (%s) ",
                [route_dir]
                    )
    query_list = cur.fetchall()
    best_depth = query_list[0][0]

    return best_depth

def trained_check(conn, route_dir):
    '''
    checking training database
    if the route_dir has been trained return True
    else return False

    OUTPUT
    -------
    True - if route_dir has been trained
    False - if route_dir has not been trained
    '''
    cur = conn.cursor()

    cur.execute("SELECT processed "
                "FROM models "
                "WHERE route_dir  = (%s) ",
                [route_dir]
                    )
    trained_status = cur.fetchall()

    return trained_status[0][0] != 'finished'

def cross_val_check(conn, route_dir):
    '''
    checking cross_val database
    if the route_dir has been CV'd return True
    else return False

    OUTPUT
    -------
    True - if route_dir has been CV'd
    False - if route_dir has not been CV'd
    '''
    cur = conn.cursor()

    cur.execute("SELECT c_validated "
                "FROM model_params "
                "WHERE route_dir  = (%s) ",
                [route_dir]
                    )
    cv_status = cur.fetchall()

    return cv_status[0][0] == True


def put_pickle_model(fit_model, filename):
        '''
        INPUT
        ------
        fit_model - fit gradient boosted regressor
        filename - location to put the pickle

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
    '''
    OUTPUT
    -------
    update the models table to mark the route_dir
    trained column as 'true'
    '''
    cur = conn.cursor()

    cur.execute("UPDATE models "
                "SET pickle_path = (%s),"
                    "model_columns = (%s),"
                    "trained = 'true' "
                    "WHERE route_dir  = (%s)",
                    (pickle_path, all_columns, route_dir))
    conn.commit()

def mark_as_finished(conn, route_dir):
    '''
    OUTPUT
    -------
    udpate model table to mark route_dir
    processed column as "finished"
    '''
    cur = conn.cursor()

    cur.execute("UPDATE models "
                "SET processed = 'finished' "
                    "WHERE route_dir  = (%s)",
                    [route_dir])
    conn.commit()


def find_next_route_dir(conn):
    '''
    Search for route_dir where processed = 'not_started'

    '''
    cur = conn.cursor()

    try:
        cur.execute("SELECT route_dir "
                    "FROM models "
                    "WHERE processed  = 'not_started' "
                    "LIMIT 1"
                        )
    except:
        call_status = "Done"
        return call_status, None

    query_list = cur.fetchall()

    route_dir = query_list[0][0]
    call_status = "Continue"

    return call_status, route_dir

def route_in_progress(conn, route_dir):
    '''
    mark specific route_dir as "in_progress"
    '''
    cur = conn.cursor()

    cur.execute("UPDATE models "
                "SET processed = 'in_progress' "
                    "WHERE route_dir  = (%s)",
                    [route_dir])
    conn.commit()



if __name__ == "__main__":
    train_all_routes()
