import pandas as pd
import numpy as np

def update_check(update_db, data_date):
    '''
    INPUT
    ------
    one day's worth of downloaded pb files turned into a pandas db


    OUTPUT
    -------
    print out of # rows, unique stops, unique trips and unique vehicles

    '''
    file_path = './data_dump_tracker.txt'
    num_rows = len(update_db)
    num_stops = len(update_db['stop_id'].unique())
    num_trips = len(update_db['trip_id'].unique())
    num_vehicles = len(update_db['vehicle_id'].unique())
    output_list = ['number of rows: ', 'number of stops: ',
                    'number of trips: ', 'number of vehicles: ']

    output_str = (
    "{}{}\n{}{}\n{}{}\n{}{}\n\
    ".format(output_list[0], num_rows,
            output_list[1], num_stops,
            output_list[2], num_trips,
            output_list[3], num_vehicles))

    with open(file_path, "a") as f:
        f.write(data_date+" pre-pipeline")
        f.write("\n")
        f.write(output_str)
        f.write("\n")

def update_merge_check(update_db, data_date):
    '''
    INPUT
    ------
    one day's worth of downloaded pb files turned into a pandas db


    OUTPUT
    -------
    print out of # rows, unique stops, unique trips and unique vehicles

    '''
    file_path = './data_dump_tracker.txt'
    num_rows = len(update_db)
    num_stops = len(update_db['stop_id'].unique())
    num_trips = len(update_db['trip_id'].unique())
    num_vehicles = len(update_db['vehicle_id'].unique())
    output_list = ['number of rows: ', 'number of stops: ',
                    'number of trips: ', 'number of vehicles: ']
    output_str = (
    "{}{}\n{}{}\n{}{}\n{}{}\n\
    ".format(output_list[0], num_rows,
            output_list[1], num_stops,
            output_list[2], num_trips,
            output_list[3], num_vehicles))

    with open(file_path, "a") as f:
        f.write(data_date+" post-merge")
        f.write("\n")
        f.write(output_str)
        f.write("\n")

def update_check_post_pipeline(update_db, data_date):
    '''
    INPUT
    ------
    one day's worth of downloaded pb files turned into a pandas db


    OUTPUT
    -------
    print out of # rows, unique stops, unique trips and unique vehicles

    '''
    file_path = './data_dump_tracker.txt'
    num_rows = len(update_db)
    num_stops = len(update_db['stop_id'].unique())
    num_trips = len(update_db['trip_id'].unique())
    num_vehicles = len(update_db['vehicle_id'].unique())
    output_list = ['number of rows: ', 'number of stops: ',
                    'number of trips: ', 'number of vehicles: ']
    output_str = (
    "{}{}\n{}{}\n{}{}\n{}{}\n\
    ".format(output_list[0], num_rows,
            output_list[1], num_stops,
            output_list[2], num_trips,
            output_list[3], num_vehicles))

    with open(file_path, "a") as f:
        f.write(data_date+" post-pipeline")
        f.write("\n")
        f.write(output_str)
        f.write("\n")

def position_check(position_db, data_date):
    '''
    INPUT
    ------
    one day's worth of downloaded pb files turned into a pandas db


    OUTPUT
    -------
    print out of # rows - i.e. unique vehicle observations

    '''
    file_path = './position_data_dump_tracker.txt'
    num_rows = len(position_db)
    output_list = ['number of rows: ']

    output_str = (
    "{}{}\n{}{}\n{}{}\n{}{}\n\
    ".format(output_list[0], num_rows,
            ))

    with open(file_path, "a") as f:
        f.write(data_date+" position_pre-pipeline")
        f.write("\n")
        f.write(output_str)
        f.write("\n")

def position_post_clean_check(position_db, data_date):
    '''
    INPUT
    ------
    one day's worth of downloaded pb files turned into a pandas db


    OUTPUT
    -------
    print out of # rows - i.e. unique vehicle observations

    '''
    file_path = './position_data_dump_tracker.txt'
    num_rows = len(position_db)
    output_list = ['number of rows: ']

    output_str = (
    "{}{}\n{}{}\n{}{}\n{}{}\n\
    ".format(output_list[0], num_rows,
            ))

    with open(file_path, "a") as f:
        f.write(data_date+" position_post-clean")
        f.write("\n")
        f.write(output_str)
        f.write("\n")
