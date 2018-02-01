import pandas as pd
import numpy as np

def prediction_check(full_route_output_df, route_dir):
    '''
    INPUT
    ------


    OUTPUT
    -------
    print out of # rows, unique stops, unique hours, unique dow

    '''
    file_path = './prediction_dump_tracker.txt'
    num_rows = len(full_route_output_df)
    num_stops = len(full_route_output_df['stop_name'].unique())
    num_hours = len(full_route_output_df['hour'].unique())
    num_dow = len(full_route_output_df['dow'].unique())
    output_list = ['number of rows: ', 'number of stops: ',
                    'number of hours: ', 'number of dow: ']

    output_str = (
    "{}{}\n{}{}\n{}{}\n{}{}\n\
    ".format(output_list[0], num_rows,
            output_list[1], num_stops,
            output_list[2], num_hours,
            output_list[3], num_dow))

    with open(file_path, "a") as f:
        f.write(route_dir+" prediction_df")
        f.write("\n")
        f.write(output_str)
        f.write("\n")
