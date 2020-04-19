from __future__ import division
from flask import Flask, render_template, request, jsonify
from math import sqrt
from model_pipeline import dashboard_pipe
import os
import pandas as pd
import numpy as np
import datetime


app = Flask(__name__)


route_short_df = pd.read_csv("local_data/route_all.csv")


route_short_list = list(route_short_df['route_short_name'].unique())
excluded_list = ['A Line', 'B Line', 'C Line', 'D Line', 'E Line',
                'F Line']
cut_route_list = [route for route in route_short_list if route not in excluded_list]
route_arr = np.array(cut_route_list).astype(int)
sorted_routes = sorted(route_arr)
sorted_route_list = [int(i) for i in sorted_routes]
initial_route_list = ['Route'] + sorted_route_list


route_shape_df = pd.read_csv("local_data/route_shape.csv")


stop_hour_df = pd.read_csv("local_data/route_metrics.csv")
print(len(stop_hour_df))


stop_hours = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
        12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]

def select_stop_conf(stop_hour_df, stop_name, route_dir, hour):
    route_stop_mask = ((stop_hour_df['stop_name'] == stop_name) &
                        (stop_hour_df['route_dir'] == route_dir))

    user_hour = hour
    per_10_col = 'hour{}_10'.format(user_hour)
    per_90_col = 'hour{}_90'.format(user_hour)
    conf_interval_arr = stop_hour_df[route_stop_mask][[per_10_col, per_90_col]].values[0]

    return conf_interval_arr

def get_stop_hours(stop_hour_df, route_short_df, route_short_name,
                    stop_name, direction):
    short_dir_mask = ((route_short_df['route_short_name'] == route_short_name)
                    & (route_short_df['direction_id'] == direction))
    select_route_dir = route_short_df[short_dir_mask]
    route_dir = select_route_dir['route_dir'].unique()[0]
    stop_route_id_mask = ((stop_hour_df['route_dir'] == route_dir) &
                            (stop_hour_df['stop_name'] == stop_name))
    stop_hours = stop_hour_df[stop_route_id_mask]['stop_hours'].values[0].strip("[]").split(",")
    strip_hours = []
    for stop in stop_hours:
        stop = stop.strip()
        strip_hours.append(stop)
    sorted_hours = sorted(np.array(strip_hours).astype(int))

    return sorted_hours

def get_stop_names(route_short_df, route_short_name, direction):
    route_dir_mask = ((route_short_df['route_short_name'] == route_short_name)
                    & (route_short_df['direction_id'] == direction))
    select_route_dir = route_short_df[route_dir_mask]
    sorted_route = select_route_dir.sort_values(by='stop_sequence')
    sorted_stop_names = sorted_route['stop_name'].unique().tolist()


    return sorted_stop_names

def get_route_shape(route_shape_df, route_short_name):
    route_mask = route_shape_df['route_short_name'] == route_short_name
    route_select_df = route_shape_df[route_mask]
    possible_shapes = [int(i) for i in route_select_df['shape_id'].unique()]
    return possible_shapes

def get_stop_id(route_short_df, current_stop_name):
    stop_mask = route_short_df['stop_name'] == current_stop_name
    stop_id = route_short_df[stop_mask]['stop_id'].values[0]
    return stop_id

def get_route_dir_shape(route_shape_df, route_short_name, direction_id):
    route_dir_mask = ((route_shape_df['route_short_name'] == route_short_name)
                    & (route_shape_df['direction_id'] == direction_id))
    route_select_df = route_shape_df[route_dir_mask]
    possible_shapes = [int(i) for i in route_select_df['shape_id'].unique()]

    return possible_shapes

def make_direction_list(route_short_df, short_name):
    direction_list = list(route_short_df['direction_id'].unique())
    #default_direction = "Select a direction"
    if len(route_short_df['direction_id'].unique()) < 2:
        route_mask_dir1 = ((route_short_df['route_short_name'] == short_name)
                            & (route_short_df['direction_id'] == direction_list[0]))
        select_route_dir1_df = route_short_df[route_mask_dir1]
        sorted_route = select_route_dir1_df.sort_values(by='stop_sequence')
        num_stops = len(sorted_route) - 1
        last_stop = "TO "+str(sorted_route['stop_name'].iloc[num_stops])
        directions = [last_stop]

    else:
        route_mask_dir1 = ((route_short_df['route_short_name'] == short_name)
                            & (route_short_df['direction_id'] == 1))
        select_route_dir1_df = route_short_df[route_mask_dir1]
        sorted_route_dir1 = select_route_dir1_df.sort_values(by='stop_sequence')
        num_stops = len(sorted_route_dir1) - 1
        last_stop_dir1 = "TO "+str(sorted_route_dir1['stop_name'].iloc[num_stops])

        route_mask_dir0 = ((route_short_df['route_short_name'] == short_name)
                            & (route_short_df['direction_id'] == 0))
        select_route_dir0_df = route_short_df[route_mask_dir0]
        sorted_route_dir0 = select_route_dir0_df.sort_values(by='stop_sequence')
        num_stops = len(sorted_route_dir0) - 1
        last_stop_dir0 = "TO "+str(sorted_route_dir0['stop_name'].iloc[num_stops])
        directions = [last_stop_dir0, last_stop_dir1]
    return directions


def short_dir_to_route_dir(route_short_df, short_dir):
    short_dir_mask = route_short_df['short_dir'] == short_dir
    route_dir = route_short_df[short_dir_mask]['route_dir'].unique()[0]
    return route_dir


@app.route('/')
def index():


    current_date = (datetime.date.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    return render_template('charts.html',
                                current_date=current_date,
                                route_names=initial_route_list
                                )


@app.route('/route', methods=['GET','POST'])
def route():


    current_route_name = request.args.get("routeSelect")

    current_date = (datetime.date.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    directions = make_direction_list(route_short_df, current_route_name)

    selected_route_shape = get_route_shape(route_shape_df, current_route_name)

    current_direction = "Select a direction"






    return render_template('charts.html',
                                route_names=sorted_routes,
                                current_route_name=int(current_route_name),
                                current_date=current_date,
                                current_direction=current_direction,
                                selected_route_shape=selected_route_shape,
                                directions=directions)



@app.route('/route_internal', methods=['POST'])
def route_internal():
    user_data = request.json

    print(user_data)

    sorted_route_list = [int(i) for i in sorted_routes]

    current_route_name = user_data['user_route']

    current_date = (datetime.date.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    directions = make_direction_list(route_short_df, current_route_name)

    selected_route_shape = get_route_shape(route_shape_df, current_route_name)

    print(selected_route_shape)

    return jsonify({'directions':directions,
                    'selected_route_shape':selected_route_shape,
                    'route_names':sorted_route_list,
                    'current_route_name':current_route_name
                    })

@app.route('/direction_internal', methods=['POST'])
def direction_internal():
    user_data = request.json

    print(user_data)

    sorted_route_list = [int(i) for i in sorted_routes]

    current_route_name = user_data['user_route']

    current_direction = user_data['user_direction']

    directions = make_direction_list(route_short_df, current_route_name)

    if current_direction == directions[1]:
        direction = 1
        stop_names = get_stop_names(route_short_df,
                                    current_route_name, direction)
    else:
        direction = 0
        stop_names = get_stop_names(route_short_df,
                                    current_route_name, direction)

    selected_route_shape = get_route_dir_shape(route_shape_df,
                                    current_route_name, direction)

    print(stop_names)

    return jsonify({'route_names': sorted_route_list,
                    'current_route_name': current_route_name,
                    'current_direction':current_direction,
                    'direction_number':direction,
                    'stop_names':stop_names,
                    'selected_route_shape':selected_route_shape
                    })


@app.route('/stop_internal', methods=['POST'])
def stop_internal():
    user_data = request.json

    print(user_data)

    sorted_route_list = [int(i) for i in sorted_routes]

    current_route_name = user_data['user_route']

    current_direction = user_data['user_direction']

    current_stop_name = user_data['user_stop']

    print(current_stop_name)

    directions = make_direction_list(route_short_df, current_route_name)

    if current_direction == directions[1]:
        direction = 1
        stop_names = get_stop_names(route_short_df,
                                    current_route_name, direction)
    else:
        direction = 0
        stop_names = get_stop_names(route_short_df,
                                    current_route_name, direction)

    hours = get_stop_hours(stop_hour_df, route_short_df,
                                current_route_name,
                                current_stop_name, direction)

    selected_route_shape = get_route_dir_shape(route_shape_df,
                                        current_route_name, direction)

    hours_list = [int(i) for i in hours]


    return jsonify({'directions': directions,
                    'stop_names':stop_names,
                    'selected_route_shape':selected_route_shape,
                    'hours':hours_list})

@app.route('/predict', methods=['POST'])
def predict():

    user_data = request.json

    print(user_data)

    route_short_name, current_direction, stop_name, hour, date = (user_data['user_route'],
                                user_data['user_direction'],
                                user_data['user_stop'],
                                int(user_data['user_hour']),
                                user_data['user_date'])

    directions = make_direction_list(route_short_df,
                                        route_short_name)

    if current_direction == directions[1]:
        direction = 1
    else:
        direction = 0




    print(route_short_name, stop_name, direction,
                            date, hour)

    short_dir = str(route_short_name) + "_" + str(direction)


    prediction = dashboard_pipe(route_short_name, 
                            stop_name, direction,
                            date, 
                            hour)

    route_dir = short_dir_to_route_dir(route_short_df, short_dir)

    conf_interval_arr = select_stop_conf(stop_hour_df, stop_name, route_dir, hour)


    if conf_interval_arr[0] == 'nan':
        conf_interval_10 = -0.01
    else:
        conf_interval_10 = conf_interval_arr[0]
    if conf_interval_arr[1] == 'nan':
        conf_interval_90 = 0.01
    else:
        conf_interval_90 = conf_interval_arr[1]

    print(conf_interval_10, conf_interval_90, prediction)

    return jsonify({'prediction': prediction,
                    'conf_interval_10': conf_interval_10,
                    'conf_interval_90': conf_interval_90
                    })




if __name__ == '__main__':
    app.run(host='0.0.0.0', threaded=True, debug=False)
