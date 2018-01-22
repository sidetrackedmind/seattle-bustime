from gtfs_realtime import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToJson, MessageToDict
import json
from collections import defaultdict
import os
import re
import pandas as pd
import numpy as np

def make_update_db_from_day(generic_path):
    '''
    INPUT
    -------
    input the path to the .pb folder structure
    I'm querying the One Bus Away API every minute
    Every minute I'm getting three .pb files
    -- position.pb
    -- update.pb
    -- alert.pb
    I structured my S3 bucket to store data into 1 hour subfolders
    for instance, data for January 15th is stored in the following way:
    2018/01/15
    >>>>>>>>>> one folder for each hour '00','01','02',...,'23'
    this function takes a folder path to a day's worth of data
    saved in folders for each hour ('00','01','02',...,'23') and
    extracts the position and update file names

    OUTPUT
    --------
    one pandas databases
    - day_update_db

    '''
    day_update_list = []
    day_bad_update_header_list = []
    dirname = generic_path
    folder_list = [f for f in os.listdir(dirname)]
    for folder in folder_list:
        folder_path = os.path.join(dirname,folder)
        all_subfiles_list = [f for f in os.listdir(folder_path)]
        current_update_list = list(filter(lambda x: 'update' in x,
                                        all_subfiles_list))
        update_list, bad_update_header_list = make_update_list(current_update_list, folder_path)
        day_update_list.append(update_list)
        day_bad_update_header_list.append(bad_update_header_list)
        print("completed {} hour update extraction \
                {} bad update headers".format(folder,
                len(bad_update_header_list)))
    #make database
    day_update_db = make_update_pandas(day_update_list)
    return day_update_db

def make_position_db_from_day(generic_path):
    '''
    INPUT
    -------
    input the path to the .pb folder structure
    I'm querying the One Bus Away API every minute
    Every minute I'm getting three .pb files
    -- position.pb
    -- update.pb
    -- alert.pb
    I structured my S3 bucket to store data into 1 hour subfolders
    for instance, data for January 15th is stored in the following way:
    2018/01/15
    >>>>>>>>>> one folder for each hour '00','01','02',...,'23'
    this function takes a folder path to a day's worth of data
    saved in folders for each hour ('00','01','02',...,'23') and
    extracts the position and update file names

    OUTPUT
    --------
    one pandas databases
    - day_position_db

    '''
    day_vehicle_list = []
    day_bad_vehicle_header_list = []
    dirname = generic_path
    folder_list = [f for f in os.listdir(dirname)]
    for folder in folder_list:
        folder_path = os.path.join(dirname,folder)
        all_subfiles_list = [f for f in os.listdir(folder_path)]
        current_position_list = list(filter(lambda x: 'position' in x,
                                        all_subfiles_list))
        vehicle_list, bad_vehicle_header_list = make_vehicle_list(current_position_list, folder_path)
        day_vehicle_list.append(vehicle_list)
        day_bad_vehicle_header_list.append(bad_vehicle_header_list)
        print("completed {} hour vehicle extraction \
                {} bad vehicle headers".format(folder,
                len(bad_vehicle_header_list)))
    #make database
    day_position_db = make_position_pandas(day_vehicle_list)
    return day_position_db

def make_update_pandas(day_update_list):
    update_df = pd.DataFrame(day_update_list[0])
    for i, update in enumerate(day_update_list):
        if i > 0:
            current_update_df = pd.DataFrame(update)
            update_df = pd.concat([update_df, current_update_df])
    update_df['route_id'] = update_df['route_id'].astype(int)
    update_df['stop_id'] = update_df['stop_id'].astype(int)
    update_df['trip_id'] = update_df['trip_id'].astype(int)
    update_df['vehicle_id'] = update_df['vehicle_id'].astype(int)
    return update_df

def make_position_pandas(day_vehicle_list):
    position_df = pd.DataFrame(day_vehicle_list[0])
    for i, position in enumerate(day_vehicle_list):
        if i > 0:
            current_position_df = pd.DataFrame(position)
            position_df = pd.concat([position_df, current_position_df])
    position_df['vehicle_id'] = position_df['vehicle_id'].astype(int)
    return position_df

def make_vehicle_list(pb_file_list, folder_path):
    vehicle_list = []
    bad_vehicle_header_list = []
    dirname = folder_path
    for pb_file in pb_file_list:
        pb_file_path = os.path.join(dirname, pb_file)
        with open(pb_file_path, 'rb') as f:
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(f.read())
            dict_obj = MessageToDict(feed)
        if 'entity' in dict_obj.keys():
            for vehicles_idx in range(len(dict_obj['entity'])):
                vehicle_dict = {}
                j_in = json.dumps(dict_obj['entity'][vehicles_idx])
                j_out = json.loads(j_in)
                if 'position' in j_out['vehicle']:
                    vehicle_dict['vehicle_id'] = j_out['vehicle']['vehicle']['id']
                    vehicle_dict['timestamp'] = j_out['vehicle']['timestamp']
                    vehicle_dict['vehicle_lat'] = j_out['vehicle']['position']['latitude']
                    vehicle_dict['vehicle_long'] = j_out['vehicle']['position']['longitude']
                    vehicle_list.append(vehicle_dict)
                else:
                    bad_vehicle_header_list.append(dict_obj['header'])
        else:
            bad_vehicle_header_list.append(dict_obj['header'])
    return vehicle_list, bad_vehicle_header_list

def make_update_list(pb_file_list, folder_path):
    update_list = []
    bad_update_header_list = []
    dirname = folder_path
    for pb_file in pb_file_list:
        pb_file_path = os.path.join(dirname, pb_file)
        with open(pb_file_path, 'rb') as f:
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(f.read())
            dict_obj = MessageToDict(feed)
        if 'entity' in dict_obj.keys():
            for update_idx in range(len(dict_obj['entity'])):
                update_dict = {}
                j_in = json.dumps(dict_obj['entity'][update_idx])
                j_out = json.loads(j_in)
                if 'tripUpdate' in j_out.keys():
                    update_dict['delay'] = j_out['tripUpdate']['delay']
                    update_dict['stop_update_departure'] = j_out['tripUpdate']['stopTimeUpdate'][0]['departure']['time']
                    update_dict['stop_id'] = j_out['tripUpdate']['stopTimeUpdate'][0]['stopId']
                    update_dict['timestamp'] = j_out['tripUpdate']['timestamp']
                    update_dict['route_id'] = j_out['tripUpdate']['trip']['routeId']
                    update_dict['trip_id'] = j_out['tripUpdate']['trip']['tripId']
                    update_dict['vehicle_id'] = j_out['tripUpdate']['vehicle']['id']
                    update_list.append(update_dict)
                else:
                    bad_update_header_list.append(dict_obj['header'])
        else:
            bad_update_header_list.append(dict_obj['header'])
    return update_list, bad_update_header_list
