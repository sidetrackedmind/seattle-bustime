import os
import pandas as pd
import numpy as np
import psycopg2
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split, KFold
from sklearn.metrics import mean_squared_error
from train_all_routes import get_route_dir_list
import boto3
import pickle
import multiprocessing

def cross_validate_routes():
    route_dir_list = get_route_dir_list()
    for i, route_dir in enumerate(route_dir_list):
        print("starting process for {} - #{} out of {}".format(
                                    route_dir, i, len(route_dir_list)))
        get_best_route_params(route_dir)

def get_best_route_params(route_dir):
    '''
    INPUT
    ------
    route_dir - unique route direction id

    OUTPUT
    -------
    updated cv database with optimal depth and alpha for the route_dir

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

    #you can always change these grid paramters
    n_folds = 6
    tree_depths = [3, 5, 7, 9]
    alphas = [0.7, 0.75, 0.8, 0.85, 0.9]
    n_estimators = 1500

    print("getting tree and alpha params")
    tree_params, alpha_params = get_route_params(route_dir, tree_depths,
                                        alphas, n_folds, n_estimators)

    print("cross validate for max depth")
    #BEWARE! this number is dependent on a big EC2 instance
    pool1 = multiprocessing.Pool(45)
    cv_depth_result = pool1.map(crossval_one_depth, tree_params)

    pool1.close()



    print("cross validate for alpha")
    #BEWARE! this number is dependent on a big EC2 instance
    pool2 = multiprocessing.Pool(45)
    cv_alpha_result = pool2.map(crossval_one_alpha, alpha_params)

    pool2.close()


    print("find best depth")
    best_depth = find_best_depth(cv_depth_result, n_estimators, n_folds,
                                                            tree_depths)

    print("find best alpha")
    best_alpha = find_best_alpha(cv_alpha_result, n_estimators, n_folds,
                                                                alphas)

    print("update cv database")
    update_cv_database(conn, best_alpha, best_depth, route_dir)

    #during testing phase return some params to quality check
    return cv_depth_result, cv_alpha_result, best_depth, best_alpha


def get_route_params(route_dir, tree_depths, alphas, n_folds,
                                                        n_estimators):
    '''This is a function to process user input into the model format
    INPUT
    -------
    route_dir - route and direction unique id


    OUTPUT
    -------
    tree_params
    alpha_params
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

    tree_params = make_tree_params(n_estimators, tree_depths, n_folds,
                                                                X, y)

    alpha_params = make_alpha_params(n_estimators, alphas, n_folds, X, y)

    cur.close()
    conn.close()

    return tree_params, alpha_params

def make_tree_params(n_estimators, tree_depths, n_folds, X, y):
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
    n_estimators = n_estimators
    params = []
    kf = KFold(n_splits=n_folds, shuffle=True, random_state=1)
    for td in tree_depths:
        for k, (train_idxs, test_idxs) in enumerate(kf.split(X)):

            X_train, y_train = X[train_idxs, :], y[train_idxs]
            X_test, y_test = X[test_idxs, :], y[test_idxs]
            params.append((n_estimators, td, k, X_train, y_train, X_test, y_test))

    return params

def make_alpha_params(n_estimators, alphas, n_folds, X, y):
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
    n_estimators = n_estimators
    params = []
    kf = KFold(n_splits=n_folds, shuffle=True, random_state=1)
    for alpha in alphas:
        for k, (train_idxs, test_idxs) in enumerate(kf.split(X)):

            X_train, y_train = X[train_idxs, :], y[train_idxs]
            X_test, y_test = X[test_idxs, :], y[test_idxs]
            params.append((n_estimators, alpha, k, X_train, y_train, X_test, y_test))
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
    (n_estimators, td, k, X_train, y_train, X_test, y_test) = params
    test_errors = []
    mse_losses = []
    model = GradientBoostingRegressor(loss='quantile',
                                    n_estimators=n_estimators,
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
    (n_estimators, alpha, k, X_train, y_train, X_test, y_test) = params
    test_errors = []
    mse_losses = []
    model = GradientBoostingRegressor(loss='quantile',
                                    n_estimators=n_estimators,
                                   max_depth=5, learning_rate=0.015,
                                   subsample=0.5, alpha=alpha,
                                   random_state=128)
    model.fit(X_train, y_train)

    for j, y_pred in enumerate(model.staged_predict(X_test)):
        test_errors.append(model.loss_(y_test, y_pred))
        mse_losses.append(mean_squared_error(y_test, y_pred))


    return alpha, k, test_errors, mse_losses

def find_best_depth(cv_depth_result, n_estimators, n_folds, tree_depths):
    '''
    INPUT
    -------
    n_estimators = number of estimators choosen
    k_folds = number of cv k-folds
    tree_depths = list of tree depths

    OUTPUT
    -------
    optimal tree depth value
    '''
    n_estimators = n_estimators
    k_folds = n_folds
    tree_depths = tree_depths
    n_trees = len(tree_depths)
    k_error_list = []
    for tree_idx in range(n_trees):
        error_arr = np.zeros(n_estimators)
        for k in range(k_folds):
            idx = k + (k_folds*tree_idx)
            #this is picking best test_error change to 3 if you want mse
            error_arr += np.array(cv_depth_result[idx][2])
        k_error_list.append(min(error_arr/k_folds))
    k_error_arr = np.array(k_error_list)
    best_depth = tree_depths[np.argmin(k_error_arr)]
    return best_depth

def find_best_alpha(cv_alpha_result, n_estimators, n_folds, alphas):
    '''
    INPUT
    -------
    n_estimators = number of estimators choosen
    k_folds = number of cv k-folds
    alphas = list of alphas

    OUTPUT
    -------
    optimal alpha value
    '''
    n_estimators = n_estimators
    k_folds = n_folds
    alpha_list = alphas
    n_alphas = len(alpha_list)
    k_error_list = []
    for alpha_idx in range(n_alphas):
        error_arr = np.zeros(n_estimators)
        for k in range(k_folds):
            idx = k + (k_folds*alpha_idx)
            #this is picking best test_error change to 3 if you want mse
            error_arr += np.array(cv_alpha_result[idx][2])
        k_error_list.append(min(error_arr/k_folds))
    k_error_arr = np.array(k_error_list)
    best_alpha = alpha_list[np.argmin(k_error_arr)]
    return best_alpha


def column_list_to_string(list):
    column_str = ''
    for i, col in enumerate(list):
        if i == 0:
            column_str += str(col)
        else:
            column_str += ","+str(col)
    return column_str

def update_cv_database(conn, best_alpha, best_depth, route_dir):
    cur = conn.cursor()

    cur.execute("UPDATE model_params "
                "SET best_alpha = (%s),"
                    "best_depth = (%s),"
                    "c_validated = 'true' "
                    "WHERE route_dir  = (%s)",
                    (best_alpha, best_depth, route_dir))
    conn.commit()

def plot_tree_depth_cv(ax, cv_depth_result, n_estimators,
                                            n_folds, tree_depths):
    n_estimators = n_estimators
    k_folds = n_folds
    tree_depths = tree_depths
    n_trees = len(tree_depths)
    k_error_list = []
    x = np.arange(1,n_estimators+1,1)
    for tree_idx in range(n_trees):
        error_arr = np.zeros(n_estimators)
        for k in range(k_folds):
            idx = k + (k_folds*tree_idx)
            error_arr += np.array(cv_depth_result[idx][2])
        k_error_list.append(min(error_arr/k_folds))
        ax.plot(x, (error_arr/k_folds), label="tree depth = {}".format(tree_depths[tree_idx]))
    k_error_arr = np.array(k_error_list)

    ax.legend(fontsize=15)
    ax.set_xlabel("n_estimators", fontsize=15)
    ax.set_ylabel("Test Error",  fontsize=15)

def plot_alpha_cv(ax, cv_alpha_result, n_estimators, n_folds, alphas):
    alpha_list = alphas
    n_alphas = len(alpha_list)
    k_error_list = []
    x = np.arange(1,n_estimators+1,1)
    for alpha_idx in range(n_alphas):
        error_arr = np.zeros(n_estimators)
        for k in range(k_folds):
            idx = k + (k_folds*alpha_idx)
            error_arr += np.array(cv_alpha_result[idx][2])
        k_error_list.append(min(error_arr/k_folds))
        ax.plot(x, (error_arr/k_folds),
                    label="alpha = {}".format(alpha_list[alpha_idx]))
    k_error_arr = np.array(k_error_list)

    ax.legend(fontsize=15)
    ax.set_xlabel("n_estimators", fontsize=15)
    ax.set_ylabel("Test Error",  fontsize=15)

if __name__ == "__main__":
    cross_validate_routes()
