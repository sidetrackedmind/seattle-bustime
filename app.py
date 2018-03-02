from __future__ import division
from flask import Flask, render_template, request, jsonify
from math import sqrt
from model_pipeline import dashboard_pipe
import os
import pandas as pd
import psycopg2
import numpy as np
import datetime


app = Flask(__name__)

# Connect to the db
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

query_selections = ['route_dir','short_dir','route_short_name','direction_id',
                    'stop_id','route_id','stop_name','stop_sequence']

def column_list_to_string(list):
    column_str = ''
    for i, col in enumerate(list):
        if i == 0:
            column_str += str(col)
        else:
            column_str += ","+str(col)
    return column_str

query_string = column_list_to_string(query_selections)

cur.execute('''SELECT {}
            FROM route_all'''.format(query_string)
                )
route_short_list = cur.fetchall()


route_short_df = pd.DataFrame(route_short_list,
                        columns=query_selections)


route_short_list = list(route_short_df['route_short_name'].unique())
excluded_list = ['A Line', 'B Line', 'C Line', 'D Line', 'E Line',
                'F Line']
cut_route_list = [route for route in route_short_list if route not in excluded_list]
route_arr = np.array(cut_route_list).astype(int)
sorted_routes = sorted(route_arr)
sorted_route_list = [int(i) for i in sorted_routes]
initial_route_list = ['Select a route'] + sorted_route_list

conn.rollback()
cur = conn.cursor()
shape_query_selections = ['route_dir','route_id','direction_id',
                    'route_short_name','shape_id']
query_string = column_list_to_string(shape_query_selections)

cur.execute('''SELECT {}
            FROM route_shape'''.format(query_string)
                )
route_shape_list = cur.fetchall()
route_shape_df = pd.DataFrame(route_shape_list,
                        columns=shape_query_selections)

conn.rollback()
cur = conn.cursor()
cur.execute("SELECT * "
            "FROM route_metrics"
                )
stop_hour_list = cur.fetchall()

hour_column_list = ['route_id','is_week','stop_name','stop_id',
                    'direction_id','route_dir', 'stop_hours',
                    'hour0_10','hour0_90','hour1_10',
                    'hour1_90', 'hour2_10','hour2_90','hour3_10',
                    'hour3_90', 'hour4_10','hour4_90','hour5_10',
                    'hour5_90', 'hour6_10','hour6_90','hour7_10',
                    'hour7_90', 'hour8_10','hour8_90','hour9_10',
                    'hour9_90', 'hour10_10','hour10_90','hour11_10',
                    'hour11_90', 'hour12_10','hour12_90','hour13_10',
                    'hour13_90', 'hour14_10','hour14_90','hour15_10',
                    'hour15_90', 'hour16_10','hour16_90','hour17_10',
                    'hour17_90', 'hour18_10','hour18_90','hour19_10',
                    'hour19_90', 'hour20_10','hour20_90','hour21_10',
                    'hour21_90', 'hour22_10','hour22_90','hour23_10',
                    'hour23_90']

stop_hour_df = pd.DataFrame(stop_hour_list, columns=hour_column_list)
print(len(stop_hour_df))

#number_routes = []
#for route in route_list:
#    if route not in str_routes:
#        number_routes.append(route)

#number_routes = np.array(number_routes).astype(int)
#route_names = sorted(number_routes)

#direction_names = ["1","0"]

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

cur.close()
conn.close()

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
                    'selected_route_shape':selected_route_shape
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


    prediction = dashboard_pipe(route_short_name, stop_name, direction,
                            date, hour)

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
