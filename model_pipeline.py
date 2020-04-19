import os
import pandas as pd
import numpy as np
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


    stop_info_df = pd.read_csv("local_data/stop_info.csv")

    user_info_mask = ((stop_info_df.loc[:,'route_short_name'] == route_short_name)&
            (stop_info_df.loc[:,'stop_name'] == stop_name))
    user_df = stop_info_df.loc[user_info_mask,:]

    route_dir_stop = user_df['route_dir_stop'].values[0]
    stop_sequence = user_df['stop_sequence'].values[0]
    route_id = user_df['route_id'].values[0]
    route_dir = str(route_id) + '_' + str(direction)

    date = datetime.strptime(date_string, '%Y-%m-%d')
    hour = user_hour
    day = date.day
    month = date.month
    dow = date.weekday()


    models_df = pd.read_csv("local_data/models.csv")

    model_mask = (models_df.loc[:,'route_dir']==route_dir)
    one_model = models_df[model_mask]

    pickle_path = one_model['pickle_path'].values[0]
    all_columns = one_model['model_columns'].values[0].split(',')

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


    return prediction

