import os
import pandas as pd
import numpy as np
import psycopg2
import io
from sqlalchemy import create_engine


def route_metrics():

    '''
    INPUT
    ------
    NONE

    OUTPUT
    -------
    updated RDS table with route metrics for each route_dir
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


    cur.execute("SELECT DISTINCT route_dir "
                "FROM route_info"
                )
    route_dir_list = cur.fetchall()

    cur.close()
    conn.close()



    for i, item in enumerate(route_dir_list):
        route_dir = item[0]

        #set up the engine
        engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
                                        user,
                                        key,
                                        host,
                                        port,
                                        db_name))

        print("starting process for {} #{} of {}".format(route_dir,
                                                i, len(route_dir_list)))
        route_df = update_route_metrics(route_dir)
        print("writing {} to the database".format(route_dir))
        if i == 0:
            write_to_table(route_df, engine, table_name='route_metrics',
                                                if_exists='replace')
        else:
            write_to_table(route_df, engine, table_name='route_metrics',
                                                if_exists='append')

        conn = psycopg2.connect(dbname=db_name,
                                user=user,
                                password=key,
                                host=host,
                                port=port)
        update_status_database(conn, route_dir)
        conn.close()
        cur.close()


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


    for i, weekday_stop in enumerate(weekday_stop_list):
        is_week = True
        stop_mask = weekday_route_stats['stop_name'] == weekday_stop
        stop_id = weekday_route_stats[stop_mask]['stop_id'].values[0]
        #print(route_id, stop_id, weekday_stop, is_week)
        update_hour_df = build_hour_stop_stats_row(route_id,
                                                stop_id,
                                                weekday_stop,
                                                weekday_route_stats,
                                                direction_id,
                                                route_dir, is_week)
        if i == 0:
            full_hour_df = update_hour_df.copy()
            full_hour_df = full_hour_df[hour_column_list]
        else:
            full_hour_df = full_hour_df.append(update_hour_df)
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

def get_route_metrics(route_short_name, stop_name, direction):
    '''This is a function to process user input
    grab a pickled model and output stop metrics
    INPUT
    -------
    route_short_name = route name e.g. "7" or "76"
    stop_name = stop name e.g. "1st Ave & Broad St"
    direction = which direction the bus is going on the route (0,1)

    OUTPUT
    -------
    route_df
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
            select route_id
            from route_all
            where route_short_name = '{}'
            and stop_name = '{}'
            and direction_id = {}
            LIMIT 1'''.format(route_short_name,
                                        stop_name,
                                        direction)

    print('finding route_id and direction_id')

    cur.execute(query)
    query_list = cur.fetchall()
    route_id = query_list[0][0]
    route_dir = str(route_id) + '_' + str(direction)
    #pickle_path = build_filename(route_id, direction)

    model_col_list = ['route_dir_stop','stop_id','stop_name','stop_sequence','month', 'day', 'hour','dow','delay']

    select_string = column_list_to_string(model_col_list)

    query = '''
            select {}
            from updates
            where route_id = {}
            and direction_id = {}
            '''.format(select_string, route_id, direction)

    print('getting historical route information for {}-{}'.format(route_id, direction))
    cur.execute(query)
    query_list = cur.fetchall()
    result_df = pd.DataFrame(query_list, columns=model_col_list)
    '''y_array = (result.iloc[:,-1].values)/60
    result = result.drop('delay', axis=1)
    X_array = result.values'''

    cur.close()
    conn.close()

    return result_df


def build_hour_stop_stats_row(route_id, stop_id, stop_name, week_df,
                                direction_id, route_dir, is_week=True):
    '''
    INPUT
    -------
    stop information

    OUTPUT
    -------
    hours_week_df - dataframe containing 90 percentile statistics for
    each unique hour for a particular stop
    '''
    user_stop = week_df['stop_name'] == stop_name
    stop_hours = list(week_df[user_stop]['hour'].unique())

    hours_range = np.arange(0,24,1)
    hours_week_df = pd.DataFrame({'route_id':route_id, 'stop_id':stop_id,
                                'stop_name':stop_name, 'is_week':is_week,
                                'direction_id':direction_id,
                                'route_dir':route_dir,
                                'stop_hours':'placeholder'}, index=[0])

    hours_week_df.at[0, 'stop_hours'] = stop_hours

    for hour in hours_range:
        #print('starting hour {} stop {}'.format(hour, stop_name))
        #print(list(week_df[user_stop]['hour'].unique()))
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

def write_to_table(df, db_engine, table_name, if_exists='fail'):
    '''
    function to write pandas df to RDS table
    INPUT
    ------
    df - dataframe
    db_engine
    table_name - output table name
    if_exists - what to do if the table already exists
    '''
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
    connection.close()

def update_status_database(conn, route_dir):

    '''
    OUTPUT
    -------
    update route_metric_status table to mark
    specfic route_dir updated column as "true"
    '''
    cur = conn.cursor()
    print(route_dir)
    cur.execute("UPDATE route_metric_status "
                "SET updated = 'true' "
                    "WHERE route_dir = (%s) ",
                    [route_dir])
    conn.commit()

def column_list_to_string(list):
    column_str = ''
    for i, col in enumerate(list):
        if i == 0:
            column_str += str(col)
        else:
            column_str += ","+str(col)
    return column_str

def percentile(n):
    '''
    quick numpy percentile function to apply to a pandas dataframe
    in order to create percentile columns
    '''
    def percentile_(x):
        return np.percentile(x, n)/60
    percentile_.__name__ = 'percentile_%s' % n
    return percentile_

if __name__ == "__main__":
    route_metrics()
