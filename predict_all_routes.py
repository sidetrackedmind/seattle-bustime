import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2
import model_predict
import boto3
import pickle
import prediction_check
import io

def predict_all_routes():
    '''This is a function to process user input into the model format
    INPUT
    -------


    OUTPUT
    -------
    None
    create RDS table with prediction metrics
    called pred_metrics
    '''
    #engine params
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
    route_dir_list = cur.fetchall()

    for i, item in enumerate(route_dir_list):
        route_dir = item[0]

        route_output_df = predict_one_route_pipeline(route_dir)

        #set up engine
        engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
                                        user,
                                        key,
                                        host,
                                        port,
                                        db_name))
        print("writing {} to database".format(route_dir))
        write_to_table(route_output_df, engine, table_name='pred_metrics',
                                            if_exists='append')

def predict_one_route_pipeline(route_dir):
    '''
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
            select model_columns, pickle_path
            from models
            where route_dir = '{}' '''.format(route_dir)

    print("getting pickle")

    cur.execute(query)
    query_list = cur.fetchall()

    all_columns = query_list[0][0].strip('{}').split(',')
    pickle_path = query_list[0][1]

    fit_model = model_predict.get_pickle_model(pickle_path)

    route_id = route_dir.split('_')[0]
    direction_id = route_dir.split('_')[1]

    query_col_list = ['route_dir_stop','stop_sequence', 'month', 'day',
                    'hour', 'dow', 'delay', 'stop_name', 'stop_lat',
                    'stop_lon', 'time_pct', 'shape_dist_traveled',
                    'trip_id', 'vehicle_id', 'arrival_time']
    select_string = column_list_to_string(query_col_list)
    query = '''
            select {}
            from updates
            where route_id = {}
            and direction_id = {}
            and time_pct >= '2018-01-23'
                        '''.format(select_string, route_id, direction_id)

    print("getting update data for {}".format(route_dir))

    cur.execute(query)
    stop_updates_list = cur.fetchall()

    print("prediction stops for {} updates".format(len(stop_updates_list)))

    for i, stop_updates in enumerate(stop_updates_list):
        route_dir_stop = stop_updates[0]
        stop_sequence = stop_updates[1]
        month = stop_updates[2]
        day = stop_updates[3]
        hour = stop_updates[4]
        dow = stop_updates[5]
        delay = stop_updates[6]
        stop_name = stop_updates[7]
        stop_lat = stop_updates[8]
        stop_lon = stop_updates[9]
        time_pct = stop_updates[10]
        shape_dist_traveled = stop_updates[11]
        trip_id = stop_updates[12]
        vehicle_id = stop_updates[13]
        arrival_time = stop_updates[14]

        if i % 1000 == 0:
            print("completed {} updates".format(i))

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

        prediction = fit_model.predict(X_array)[0]

        update_route_df = build_output_df_row(route_dir_stop, route_dir,
                                time_pct, stop_sequence, shape_dist_traveled,
                                trip_id, vehicle_id, arrival_time,
                                stop_name, stop_lat, stop_lon,
                                month, day, hour, dow, delay, prediction)

        if i == 0:
            full_route_output_df = update_route_df.copy()
        else:
            full_route_output_df = full_route_output_df.append(update_route_df)
    cur.close()
    conn.close()

    prediction_check.prediction_check(full_route_output_df, route_dir)

    return full_route_output_df


def build_output_df_row(route_dir_stop, route_dir, time_pct, stop_sequence,
                        shape_dist_traveled, trip_id, vehicle_id, arrival_time,
                        stop_name, stop_lat, stop_lon, month, day, hour,
                        dow, delay, prediction):

    output_cols = ['route_dir_stop','route_dir','time_pct','stop_sequence',
                    'shape_dist_traveled', 'trip_id', 'vehicle_id', 'arrival_time',
                    'stop_name', 'stop_lat', 'stop_lon','month', 'day',
                    'hour','dow', 'act_delay', 'prediction']

    output_values = [route_dir_stop, route_dir, time_pct, float(stop_sequence),
                    shape_dist_traveled, trip_id, vehicle_id, arrival_time,
                    stop_name, stop_lat, stop_lon, month, day, hour, dow,
                    (delay)/60, prediction]

    route_output_df = pd.DataFrame(columns=output_cols)
    route_output_df.loc[0] = output_values
    route_output_df = route_output_df[output_cols]

    return route_output_df



def column_list_to_string(list):
    column_str = ''
    for i, col in enumerate(list):
        if i == 0:
            column_str += str(col)
        else:
            column_str += ","+str(col)
    return column_str

def write_to_table(df, db_engine, table_name, if_exists='fail'):
    string_data_io = io.StringIO()
    df.to_csv(string_data_io, sep='|', index=False)
    pd_sql_engine = pd.io.sql.pandasSQL_builder(db_engine)
    table = pd.io.sql.SQLTable(table_name, pd_sql_engine, frame=df,
                               index=False, if_exists=if_exists)
    table.create()
    string_data_io.seek(0)
    string_data_io.readline()  # remove header
    with db_engine.connect() as connection:
        with connection.connection.cursor() as cursor:
            copy_cmd = "COPY %s FROM STDIN HEADER DELIMITER '|' CSV" % table_name
            cursor.copy_expert(copy_cmd, string_data_io)
        connection.connection.commit()


if __name__ == "__main__":
    predict_all_routes()
