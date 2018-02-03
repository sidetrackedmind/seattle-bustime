import os
import pandas as pd
import numpy as np
import psycopg2
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split, KFold
from sklearn.metrics import mean_squared_error
import boto3
import pickle

def get_route_params(route_dir, param_type="tree_depth"):
    '''This is a function to process user input into the model format
    INPUT
    -------
    route_dir - route and direction unique id
    param_type = you can grid search for either "tree_depth" or "alphas"


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

    #change CV params as necessary
    n_folds = 6
    if param_type == "tree_depth":
        tree_depths = [3, 5, 7]
        params = make_tree_params(tree_depths, n_folds, X, y)
    if param_type == "alphas":
        alphas = [0.5, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85]
        params = make_alpha_params(alphas, n_folds, X, y)

    return params

def make_tree_params(tree_depths, n_folds, X, y):
    """
    Create a list of parameters to input into crossval_one for parallelization.

    Input
    ------
    tree_depths : List of tree_depths to gridsearch across
    n_folds : The number of folds to use in K-fold CV
    X : Full dataset, a numpy array
    y : Labels, a numpy array

    Output
    ------
    params : A list containing tuples (td, k, X_train, y_train, X_test, y_test)
            The length of params will be len(tree_depths) * n_folds
    """

    params = []
    kf = KFold(n_splits=n_folds, shuffle=True, random_state=1)
    for td in tree_depths:
        for k, (train_idxs, test_idxs) in enumerate(kf.split(X)):

            X_train, y_train = X[train_idxs, :], y[train_idxs]
            X_test, y_test = X[test_idxs, :], y[test_idxs]
            params.append((td, k, X_train, y_train, X_test, y_test))

    return params

def make_alpha_params(alphas, n_folds, X, y):
    """
    Create a list of parameters to input into crossval_one for parallelization.

    Input
    ------
    alphas : List of alphas to gridsearch across
    n_folds : The number of folds to use in K-fold CV
    X : Full dataset, a numpy array
    y : Labels, a numpy array

    Output
    ------
    params : A list containing tuples (alpha, k, X_train, y_train, X_test, y_test)
            The length of params will be len(alphas) * n_folds
    """

    params = []
    kf = KFold(n_splits=n_folds, shuffle=True, random_state=1)
    for alpha in alphas:
        for k, (train_idxs, test_idxs) in enumerate(kf.split(X)):

            X_train, y_train = X[train_idxs, :], y[train_idxs]
            X_test, y_test = X[test_idxs, :], y[test_idxs]
            params.append((alpha, k, X_train, y_train, X_test, y_test))
    return params

def crossval_one_depth(params):
    """
    Perform one fold of cross-validation with one tree depth

    Input
    ------
    params : The output of make_params

    Output
    ------
    td : The tree depth at the current stage
    k : The current cross-validation fold (can be used to map back to X and y from params)
    test_scores : A list, the model loss at each stage
    model : The model trained on the given parameters
    """
    (td, k, X_train, y_train, X_test, y_test) = params
    test_errors = []
    mse_losses = []
    model = GradientBoostingRegressor(loss='quantile', n_estimators=2000,
                                   max_depth=td, learning_rate=0.015,
                                   subsample=0.5,
                                   random_state=128)
    model.fit(X_train, y_train)

    for j, y_pred in enumerate(model.staged_predict(X_test)):
        test_errors.append(model.loss_(y_test, y_pred))
        mse_losses.append(mean_squared_error(y_test, y_pred))


    return td, k, test_errors, mse_losses

def crossval_one_alpha(params):
    """
    Perform one fold of cross-validation with one tree depth

    Input
    ------
    params : The output of make_params

    Output
    ------
    alpha : The alpha at the current stage
    k : The current cross-validation fold (can be used to map back to X and y from params)
    test_scores : A list, the model loss at each stage
    model : The model trained on the given parameters
    """
    (alpha, k, X_train, y_train, X_test, y_test) = params
    test_errors = []
    mse_losses = []
    model = GradientBoostingRegressor(loss='quantile', n_estimators=2000,
                                   max_depth=5, learning_rate=0.015,
                                   subsample=0.5, alpha=alpha,
                                   random_state=128)
    model.fit(X_train, y_train)

    for j, y_pred in enumerate(model.staged_predict(X_test)):
        test_errors.append(model.loss_(y_test, y_pred))
        mse_losses.append(mean_squared_error(y_test, y_pred))


    return alpha, k, test_errors, mse_losses

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
