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

route_list = list(route_short_df['short_dir'].unique())


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

hours = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
        12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]

def select_stop_conf(stop_hour_df, stop_name, route_dir, hour):
    route_stop_mask = ((stop_hour_df['stop_name'] == stop_name) &
                        (stop_hour_df['route_dir'] == route_dir))

    user_hour = hour
    per_10_col = 'hour{}_10'.format(user_hour)
    per_90_col = 'hour{}_90'.format(user_hour)
    conf_interval_arr = stop_hour_df[route_stop_mask][[per_10_col, per_90_col]].values[0]

    return conf_interval_arr

def select_route_name(route_short_df, route_name):
    route_mask = (route_short_df['short_dir'] == route_name)
    select_stop_names = route_short_df[route_mask]['stop_name'].unique().tolist()

    return select_stop_names

def short_dir_to_route_dir(route_short_df, short_dir):
    short_dir_mask = route_short_df['short_dir'] == short_dir
    route_dir = route_short_df[short_dir_mask]['route_dir'].unique()[0]
    return route_dir

cur.close()
conn.close()

@app.route('/')
def index():

    pred_width = 30
    conf_interval_10 = -5
    conf_interval_90 = 10

    tomorrows_date = (datetime.date.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    return render_template('charts.html',
                                pred_width=pred_width,
                                tomorrows_date=tomorrows_date,
                                route_names=route_list,
                                conf_interval_10=conf_interval_10,
                                conf_interval_90=conf_interval_90)

@app.route('/route')
def route():

#    current_date = request.args.get("datepicker")

    current_route_name = request.args.get("routeSelect")

    tomorrows_date = (datetime.date.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')


    stop_names = select_route_name(route_short_df,
                                    current_route_name)
    pred_width = 30
    conf_interval_10 = -5
    conf_interval_90 = 10

    return render_template('charts.html',
                                current_route_name=current_route_name,
                                route_names=route_list,
                                tomorrows_date=tomorrows_date,
                                stop_names=stop_names,
                                hours=hours,
                                conf_interval_10=conf_interval_10,
                                conf_interval_90=conf_interval_90)


@app.route('/predict', methods=['POST'])
def predict():

    user_data = request.json

    print(user_data)

    short_dir, stop_name, hour, date = (user_data['user_route'],
                                user_data['user_stop'],
                                int(user_data['user_hour']),
                                user_data['user_date'])

    route_short_name = short_dir.split('_')[0]
    direction = short_dir.split('_')[1]

    print(route_short_name, stop_name, direction,
                            date, hour)


    prediction = dashboard_pipe(route_short_name, stop_name, direction,
                            date, hour)

    route_dir = short_dir_to_route_dir(route_short_df, short_dir)

    conf_interval_arr = select_stop_conf(stop_hour_df, stop_name, route_dir, hour)



    conf_interval_10 = conf_interval_arr[0]
    conf_interval_90 = conf_interval_arr[1]

    return jsonify({'prediction': prediction,
                    'conf_interval_10': conf_interval_10,
                    'conf_interval_90': conf_interval_90
                    })




if __name__ == '__main__':
    app.run(host='0.0.0.0', threaded=True, debug=True)
