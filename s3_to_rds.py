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

        day_update_db = pb_todb.make_update_db_from_day(temp_storage_path)

        download_check.update_check(day_update_db, '{}/{}/{}'.format(
                                                    year, month, day))

        updates_gtfs = database_pipeline.join_gtfs_update(full_gtfs,
                                                        day_update_db)

        download_check.update_merge_check(updates_gtfs, '{}/{}/{}'.format(
                                                    year, month, day))

        update_db = database_pipeline.update_pipeline(updates_gtfs)

        download_check.update_check_post_pipeline(update_db,
                                                    '{}/{}/{}'.format(
                                                    year, month, day))

        write_to_table(update_db, engine, table_name='updates',
                                            if_exists='append')

        print("finished process for {}/{}/{} data".format(
                                                    year,
                                                    month,
                                                    day))

    #remove the temporary folder after for loop is finished
    os.system('rm -r {}'.format(temp_storage_path))


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
