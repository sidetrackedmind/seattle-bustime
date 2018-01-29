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


cur.execute("SELECT * "
            "FROM route_dir"
                )
route_short_list = cur.fetchall()


route_short_df = pd.DataFrame(route_short_list,
                        columns=['short_dir','route_short_dir',
                        'route_short_name', 'direction_id',
                        'stop_name', 'route_id'])

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

    prediction = "select a route :)"

    return render_template('charts.html',
                                route_names=route_list,
                                prediction=prediction)

@app.route('/route')
def route():

#    current_date = request.args.get("datepicker")

    current_route_name = request.args.get("route_name")

    stop_names = select_route_name(route_short_df,
                                    current_route_name)

    return render_template('charts.html',
                                current_route_name=current_route_name,
                                route_names=route_list,
                                stop_names=stop_names,
                                hours=hours)

@app.route('/stop')
def stop():


    current_route_name = request.args.get("route_name")

    stop_names = select_route_name(route_short_df,
                                    current_route_name)

    current_stop = request.args.get("stop_name")

    return render_template('charts.html',
                                current_route_name=current_route_name,
                                route_names=route_list,
                                stop_name=current_stop,
                                stop_names=stop_names,
                                prediction=current_stop,
                                hours=hours)


@app.route('/hour')
def hour():

    current_hour = request.args.get("hour")

    current_route_name = request.args.get("route_name")

    current_stop = request.args.get("stop_name")

    stop_names = select_route_name(route_short_df,
                                    current_route_name)




    return render_template('charts.html',
                                current_route_name=current_route_name,
                                route_names=route_list,
                                current_stop=current_stop,
                                stop_names=stop_names,
                                hour=current_hour,
                                prediction=current_stop,
                                hours=hours)

@app.route('/predict', methods=['POST'])
def predict():

    user_data = request.json

    route, stop, hour = user_data['user_route'], user_data['user_stop'], int(user_data['user_hour'])

    route_short_name = route.split('-')[0]
    direction = route.split('-')[1]
    tomorrows_date = (datetime.date.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    prediction = dashboard_pipe(route_short_name, stop, direction,
                            tomorrows_date, hour)

    return jsonify({'prediction': prediction})
if __name__ == '__main__':
    app.run(host='0.0.0.0', threaded=True)
