from sklearn.model_selection import KFold, train_test_split
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_squared_error
import numpy as np


def cv_n_estimators(gbr,num_kfolds, X, y, print_status=True):
    #kfold cross validation to find optimal cv_n_estimators
    stages = True
    loss = []
    loss_optimal = []
    staged_loss=[]
    optimal_n_trees = []
    kf = KFold(n_splits=num_kfolds, random_state=128, shuffle=True)
    fold = 1
    for train_index, test_index in kf.split(X):
        X_train, X_test = X[train_index], X[test_index]
        y_train, y_test = y[train_index], y[test_index]
        fit_gbr = gbr.fit(X_train, y_train)
        predictions = fit_gbr.predict(X_test)
        final_loss = mean_squared_error(y_test, predictions)
        loss.append(final_loss)
        if stages:
            loss_test = np.zeros(len(fit_gbc.estimators_))
            staged_preds = fit_gbr.staged_predict(X_test)
            for i, preds in enumerate(staged_preds):
                loss_test[i] = mean_squared_error(y_test, preds)
            staged_loss.append(loss_test)
            optimal_n_trees.append(np.argmin(loss_test))
            loss_optimal.append(loss_test[optimal_n_trees][fold-1])
        if print_status:
            print("fold: {}, optimal number of trees: {}, RMSE at n_optimal (minutes): {:.2f}, final RMSE (minutes): {:.2f}".format(
                    fold, optimal_n_trees[fold-1], loss_test[optimal_n_trees][fold-1]**(1/2), final_loss**(1/2)))

        fold +=1
    return loss, loss_optimal, staged_loss, optimal_n_trees

def cv_grid_search(gbr,num_kfolds, X, y, n_estimators, depths=[1,3,5,7,9], print_status=True):
    #kfold cross validation to find optimal cv_n_estimators
    loss = []
    loss_optimal = []
    depth_loss=[]
    kf = KFold(n_splits=num_kfolds, random_state=128, shuffle=True)

    for fold, train_index, test_index in enumerate(kf.split(X)):
        for depth in depths:
            gbr = GradientBoostingRegressor(n_estimators=n_estimators, max_depth=depth)
            X_train, X_test = X[train_index], X[test_index]
            y_train, y_test = y[train_index], y[test_index]
            fit_gbc = gbr.fit(X_train, y_train)
            predictions = fit_gbc.predict(X_test)
            final_loss = mean_squared_error(y_test, predictions)
            depth_loss.append(final_loss)
        opt_depth_idx = np.argmin(np.array(depth_loss))
        opt_loss = np.array(depth_loss)[opt_depth_idx]
        opt_depth = np.array(depths)[opt_depth_idx]
        loss_optimal.append((opt_depth, opt_loss))

        if print_status:
            print("fold: {}, optimal depth: {}, RMSE at n_optimal (minutes): {:.2f}".format(
                    fold, opt_depth, opt_loss**(1/2)))

    return loss, loss_optimal, staged_loss, optimal_n_trees
