import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2
import io
import database_pipeline
import pb_todb
import subprocess
import download_check

def s3_updates_to_rds(year, month, day_list):
    '''
    INPUT
    ------
    year = '2017'
    month = '12'
    day_list = [13, 14, 15, 16]

    OUTPUT
    ------
    pushing update files for all days in the list to RDS database'''

    bucket_name = os.environ["BUS_BUCKET_NAME"]

    temp_storage_path = './temp_data_storage'

    aws_base_command = 'aws s3 sync s3://{}/{}/{}/'.format(bucket_name,
                                                                year,
                                                                month
                                                                )

    #engine params
    db_name = os.environ["RDS_NAME"]
    user = os.environ["RDS_USER"]
    db_key = os.environ["RDS_KEY"]
    host = os.environ["RDS_HOST"]
    port = os.environ["RDS_PORT"]

    #set up the engine
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
                                    user,
                                    db_key,
                                    host,
                                    port,
                                    db_name))
    #improvement for later -- put this in s3 so you don't have to load
    #grab the gtfs table for joining later
    full_gtfs = pd.read_csv('March2017_gtfs/full_gtfs.csv', index_col=0)

    for day in day_list:

        print("starting process for {}/{}/{} data".format(
                                                    year,
                                                    month,
                                                    day))
        #remove the temporary folder if it exists
        os.system('rm -r {}'.format(temp_storage_path))

        #create a temporary folder to store a day's worth of downloads
        os.system('mkdir {}'.format(temp_storage_path))

        os.system(aws_base_command+"{} {}".format(day,
                                            temp_storage_path))

        day_position_db = pb_todb.make_position_db_from_day(temp_storage_path)

        download_check.position_check(day_position_db, '{}/{}/{}'.format(
                                                    year, month, day))



        day_position_clean_db = clean_position_db(day_position_db)

        download_check.position_post_clean_check(day_position_clean_db,
                                                    '{}/{}/{}'.format(
                                                    year, month, day))

        write_to_table(day_position_clean_db, engine, table_name='bus_raw',
                                            if_exists='append')

        print("finished process for {}/{}/{} data".format(
                                                    year,
                                                    month,
                                                    day))

    #remove the temporary folder after for loop is finished
    os.system('rm -r {}'.format(temp_storage_path))



def clean_position_db(position_db):
    '''
    input
    ------
    position_db

    output
    -------
    position_db

    basic cleaning to remove duplicate timestamps and add a time_pct column '''
    position_db.drop_duplicates(['vehicle_id', 'trip_id','timestamp'], inplace=True)
    position_db.drop_duplicates(['vehicle_lat', 'vehicle_long'], inplace=True)
    position_db['time_utc'] = pd.to_datetime(position_db['timestamp'], unit='s')
    position_db['time_pct'] = position_db['time_utc'] - pd.Timedelta('08:00:00')
    return position_db

def get_shape_id_from_triproute(trip_id, route_id, schedule_df):
    '''get shape_id for that particular trip'''
    shape_id = schedule_df[(schedule_df['route_id']==int(route_id)) & (schedule_df['trip_id']==int(trip_id))]['shape_id'].unique()[0]
    return shape_id

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
