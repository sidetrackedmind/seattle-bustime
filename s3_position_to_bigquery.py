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
import multiprocessing
from google.cloud import bigquery
import datetime


def s3_position_to_bigquery(start_date, end_date):
    '''
    INPUT
    ------
    start_date = 'MM/DD/YYYY'
    end_date = 'MM/DD/YYYY'

    OUTPUT
    ------
    pushing update files for all days in the list to RDS database

    parser = argparse.ArgumentParser()
    parser.add_argument("start_date", type=str,
                    help="enter the start_date")
    parser.add_argument("end_date", type=str,
                    help="enter the end_date")
    args = parser.parse_args()
    start_date = args.start_date
    end_date = args.end_date

    '''

    bucket_name = os.environ["BUS_BUCKET_NAME"]

    temp_storage_path = './temp_data_storage'

    start_datetime = datetime.datetime.strptime(start_date, '%m/%d/%Y')

    end_datetime = datetime.datetime.strptime(end_date, '%m/%d/%Y')

    step = datetime.timedelta(days=1)

    date_list = []
    while start_datetime <= end_datetime:
        temp_date = start_datetime.date()
        #print(temp_date)
        start_datetime += step
        date_list.append(temp_date)

    for date in date_list:

        day = '{:02d}'.format(date.day)
        month = '{:02d}'.format(date.month)
        year = str(date.year)

        aws_base_command = 'aws s3 sync s3://{}/{}/{}/'.format(bucket_name,
                                                                    year,
                                                                    month
                                                                    )
        print('day {}, month {}, year {}, base_command {}, temp_storage {}'.format(
                                                            day, month,
                                                            year, aws_base_command,
                                                            temp_storage_path))

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



        write_to_bigquery(day_position_clean_db)

        print("finished process for {}/{}/{} data".format(
                                                    year,
                                                    month,
                                                    day))

    #remove the temporary folder after for loop is finished
    os.system('rm -r {}'.format(temp_storage_path))

def s3_position_to_bigquery_multi(start_date, end_date):
    '''
    INPUT
    ------
    start_date = 'MM/DD/YYYY'
    end_date = 'MM/DD/YYYY'

    OUTPUT
    ------
    pushing update files for all days in the list to RDS database

    parser = argparse.ArgumentParser()
    parser.add_argument("start_date", type=str,
                    help="enter the start_date")
    parser.add_argument("end_date", type=str,
                    help="enter the end_date")
    args = parser.parse_args()
    start_date = args.start_date
    end_date = args.end_date

    n_pools = multiprocessing.cpu_count() - 2

    pool = multiprocessing.Pool(4)
    pool.map(s3_position_to_rds_single_day, date_list)
    '''

    bucket_name = os.environ["BUS_BUCKET_NAME"]

    temp_storage_path = './temp_data_storage'

    start_datetime = datetime.datetime.strptime(start_date, '%m/%d/%Y')

    end_datetime = datetime.datetime.strptime(end_date, '%m/%d/%Y')

    step = datetime.timedelta(days=1)

    date_list = []
    while start_datetime <= end_datetime:
        temp_date = start_datetime.date()
        #print(temp_date)
        start_datetime += step
        date_list.append(temp_date)

    n_pools = multiprocessing.cpu_count() - 2

    pool = multiprocessing.Pool(4)
    pool.map(s3_position_to_bigquery_single_day, date_list)


def s3_position_to_bigquery_single_day(date):
    '''
    INPUT
    ---------
    date - datetime format
    '''
    bucket_name = os.environ["BUS_BUCKET_NAME"]
    day = '{:02d}'.format(date.day)
    month = '{:02d}'.format(date.month)
    year = str(date.year)
    day_temp_storage_path = "./temp_data_storage_{}_{}".format(month, day)

    aws_base_command = 'aws s3 sync s3://{}/{}/{}/'.format(bucket_name,
                                                                year,
                                                                month
                                                                )

    print("starting process for {}/{}/{} data".format(
                                                year,
                                                month,
                                                day))
    #remove the temporary folder if it exists
    os.system('rm -r {}'.format(day_temp_storage_path))

    #create a temporary folder to store a day's worth of downloads
    os.system('mkdir {}'.format(day_temp_storage_path))

    os.system(aws_base_command+"{} {}".format(day,
                                        day_temp_storage_path))

    day_position_db = pb_todb.make_position_db_from_day(day_temp_storage_path)

    download_check.position_check(day_position_db, '{}/{}/{}'.format(
                                                year, month, day))



    day_position_clean_db = clean_position_db(day_position_db)

    download_check.position_post_clean_check(day_position_clean_db,
                                                '{}/{}/{}'.format(
                                                year, month, day))



    write_to_bigquery(day_position_clean_db, month, day)

    print("finished process for {}/{}/{} data".format(
                                                year,
                                                month,
                                                day))

    #remove the temporary folder after for loop is finished
    os.system('rm -r {}'.format(day_temp_storage_path))


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
    position_db['hour'] = position_db['time_pct'].dt.hour
    position_db['dow'] = position_db['time_pct'].dt.dayofweek
    position_db['day'] = position_db['time_pct'].dt.day
    position_db['month'] = position_db['time_pct'].dt.month
    col_list = ['route_id', 'timestamp', 'trip_id', 'vehicle_id',
            'vehicle_lat', 'vehicle_long', 'time_utc', 'time_pct',
            'hour', 'dow', 'day', 'month']
    position_db = position_db[col_list]
    return position_db

def get_shape_id_from_triproute(trip_id, route_id, schedule_df):
    '''get shape_id for that particular trip'''
    shape_id = schedule_df[(schedule_df['route_id']==int(route_id)) & (schedule_df['trip_id']==int(trip_id))]['shape_id'].unique()[0]
    return shape_id

def write_to_bigquery(df, month, day):
    '''
    '''
    #bigquery params
    client = bigquery.Client.from_service_account_json(
    'bustime-keys.json')
    filename = 'temp_day_positions_{}_{}.csv'.format(month,day)
    dataset_id = 'vehicle_data'
    table_id = 'vehicles_raw'

    #write df to csv
    df.to_csv(filename, index=False)

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True

    with open(filename, 'rb') as source_file:
        job = client.load_table_from_file(
            source_file,
            table_ref,
            location='US',  # Must match the destination dataset location.
            job_config=job_config)  # API request

    job.result()  # Waits for table load to complete.

    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset_id, table_id))

    os.system('rm -r {}'.format(filename))
