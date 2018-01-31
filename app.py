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

query_string = column_list_to_string(query_selections)

cur.execute('''SELECT {}
            FROM route_info'''.format(query_string)
                )
route_short_list = cur.fetchall()


route_short_df = pd.DataFrame(route_short_list,
                        columns=query_selections)

route_list = list(route_short_df['short_dir'].unique())

#number_routes = []
#for route in route_list:
#    if route not in str_routes:
#        number_routes.append(route)

#number_routes = np.array(number_routes).astype(int)
#route_names = sorted(number_routes)

#direction_names = ["1","0"]

hours = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
        12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]


def select_route_name(route_short_df, route_name):
    route_mask = (route_short_df['short_dir'] == route_name)
    select_stop_names = route_short_df[route_mask]['stop_name'].values.tolist()

    return select_stop_names

cur.close()
conn.close()

@app.route('/')
def index():
    tomorrows_date = (datetime.date.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    return render_template('charts.html',
                                tomorrows_date=tomorrows_date,
                                route_names=route_list)

@app.route('/route')
def route():

#    current_date = request.args.get("datepicker")

    current_route_name = request.args.get("routeSelect")

    tomorrows_date = (datetime.date.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    stop_names = select_route_name(route_short_df,
                                    current_route_name)

    return render_template('charts.html',
                                current_route_name=current_route_name,
                                route_names=route_list,
                                tomorrows_date=tomorrows_date,
                                stop_names=stop_names,
                                hours=hours)


@app.route('/predict', methods=['POST'])
def predict():

    user_data = request.json

    print(user_data)

    route, stop, hour, date = (user_data['user_route'],
                                user_data['user_stop'],
                                int(user_data['user_hour']),
                                user_data['user_date'])

    route_short_name = route.split('-')[0]
    direction = route.split('-')[1]
    #tomorrows_date = (datetime.date.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    prediction = dashboard_pipe(route_short_name, stop, direction,
                            date, hour)

def column_list_to_string(list):
    column_str = ''
    for i, col in enumerate(list):
        if i == 0:
            column_str += str(col)
        else:
            column_str += ","+str(col)
    return column_str

    return jsonify({'prediction': prediction})
if __name__ == '__main__':
    app.run(host='0.0.0.0', threaded=True)
