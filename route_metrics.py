import os
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
from route_model_train import get_route_metrics
from s3_to_rds import write_to_table

def route_metrics():

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


    cur.execute("SELECT DISTINCT route_dir"
                "FROM route_info"
                )
    route_dir_list = cur.fetchall()

    #set up the engine
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
                                    user,
                                    db_key,
                                    host,
                                    port,
                                    db_name))

    for i, item in enumerate(route_dir_list):
        route_dir = item[0]
        print("starting process for {} #{} of {}".format(route_dir,
                                                i, len(route_dir_list)))
        route_df = update_route_metrics(route_dir)
        if i == 0:
            write_to_table(route_df, engine, table_name='route_metrics',
                                                if_exists='replace')
        else:
            write_to_table(route_df, engine, table_name='route_metrics',
                                                if_exists='append')


    cur.close()
    conn.close()

def update_route_metrics(route_dir):
    '''
    INPUT
    --------
    route_dir = unique route_id and direction

    OUTPUT
    --------
    updated RDS table with route metrics
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


    cur.execute('''SELECT route_id, route_short_name, stop_name, direction_id, stop_id
                FROM route_all
                WHERE route_dir = '{}'
                LIMIT 1'''.format(route_dir))

    query_list = cur.fetchall()

    route_id = query_list[0][0]
    route_short_name = query_list[0][1]
    stop_name = query_list[0][2]
    direction_id = query_list[0][3]
    stop_id = query_list[0][4]
    route_dir = str(route_id)+'_'+str(direction_id)

    cur.close()
    conn.close()

    route_stats_df = get_route_metrics(route_short_name, stop_name, direction_id)

    weekend_route_stats = route_stats_df[route_stats_df['dow'] > 4]

    weekend_stop_list = list(weekend_route_stats['stop_name'].unique())

    weekday_route_stats = route_stats_df[route_stats_df['dow'] < 5]

    weekday_stop_list = list(weekday_route_stats['stop_name'].unique())

    hour_column_list = ['route_id','is_week','stop_name','stop_id',
                    'direction_id','route_dir',
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


    for weekday_stop in weekday_stop_list:
        is_week = True
        print(route_id, stop_id, weekday_stop, is_week)
        full_hour_df = build_hour_stop_stats_row(route_id,
                                                stop_id,
                                                weekday_stop,
                                                weekday_route_stats,
                                                direction_id,
                                                route_dir, is_week)
        full_hour_df = full_hour_df[hour_column_list]


    for weekend_stop in weekend_stop_list:
        is_week = False
        update_hour_df = build_hour_stop_stats_row(route_id,
                                                stop_id,
                                                weekend_stop,
                                                weekend_route_stats,
                                                direction_id,
                                                route_dir, is_week)

        full_hour_df = full_hour_df.append(update_hour_df)

        full_hour_df = full_hour_df[hour_column_list]



    return full_hour_df




def build_hour_stop_stats_row(route_id, stop_id, stop, week_df,
                                direction_id, route_dir, is_week=True):
    '''
    '''
    user_stop = week_df['stop_name'] == stop
    hours_range = np.arange(0,24,1)
    hours_week_df = pd.DataFrame({'route_id':route_id, 'stop_id':stop_id,
                                'stop':stop, 'is_week':is_week,
                                'direction_id':direction_id,
                                'route_dir':route_dir}, index=[0])
    for hour in hours_range:
        print('starting hour {} stop {}'.format(hour, stop))
        if hour in week_df[user_stop]['hour'].unique():
            hour_mask = week_df['hour'] == hour
            col_10_name = 'hour{}_10'.format(hour)
            col_90_name = 'hour{}_90'.format(hour)
            hours_week_df[col_10_name] = week_df[(hour_mask)&(user_stop)].groupby('hour').agg(percentile(10))['delay'].values[0]
            hours_week_df[col_90_name] = week_df[(hour_mask)&(user_stop)].groupby('hour').agg(percentile(90))['delay'].values[0]
        else:
            col_10_name = 'hour{}_10'.format(hour)
            col_90_name = 'hour{}_90'.format(hour)
            hours_week_df[col_10_name] = np.nan
            hours_week_df[col_90_name] = np.nan

    return hours_week_df

def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)/60
    percentile_.__name__ = 'percentile_%s' % n
    return percentile_

if __name__ == "__main__":
    route_metrics()
