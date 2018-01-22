from pandasql import sqldf
import numpy as np
import pandas as pd

def update_pipeline(update_gtfs_db):

    '''times to datetime and various time feature creation'''
    update_gtfs_cols = list(update_gtfs_db.columns)
    if 'Unnamed: 0' in update_gtfs_cols:
        update_gtfs_db = update_gtfs_db.drop('Unnamed: 0', axis=1)
    update_gtfs_db['time_utc'] = pd.to_datetime(update_gtfs_db['timestamp'], unit='s')
    update_gtfs_db['time_pct'] = update_gtfs_db['time_utc'] - pd.Timedelta('08:00:00')
    update_gtfs_db['hour'] = update_gtfs_db['time_pct'].dt.hour
    update_gtfs_db['dow'] = update_gtfs_db['time_pct'].dt.dayofweek
    update_gtfs_db['day'] = update_gtfs_db['time_pct'].dt.day
    update_gtfs_db['month'] = update_gtfs_db['time_pct'].dt.month

    update_gtfs_db = update_gtfs_db.drop('stop_update_departure', axis=1)
    update_gtfs_db = update_gtfs_db.drop('time_utc', axis=1)
    #drop any general duplicates
    update_gtfs_db = update_gtfs_db.drop_duplicates()

    '''create a unique ID for each route_trip_stop this helps
        when we're trying to remove duplicate delays for the same trip
        at the same stop '''
    update_gtfs_db['route_trip_stop'] = (
                                    (update_gtfs_db['route_id']).astype(str)
                                    + "_"
                                    + (update_gtfs_db['trip_id']).astype(str)
                                    + "_"
                                    + (update_gtfs_db['stop_id']).astype(str))

    '''create a unique route_dir_stop id for summarizing stop
        statistics later'''
    update_gtfs_db['route_dir_stop'] = (
                            (update_gtfs_db['route_id']).astype(str)
                            + "_"
                            + (update_gtfs_db['direction_id']).astype(str)
                            + "_"
                            + (update_gtfs_db['stop_id']).astype(str))

    '''at the beginning and end of a route there are multiple updates for
        each stop, the following line sorts to get the largest delay
        for each unique route_trip_stop and vehicle on top'''
    update_gtfs_db = update_gtfs_db.sort_values(by=['route_trip_stop',
                            'vehicle_id', 'delay'], ascending=False)

    '''drop all delays except the "final" delay. this drop all except
        first should work since we sorted by delay above'''
    update_gtfs_db = update_gtfs_db.drop_duplicates(subset=['stop_id',
                                        'vehicle_id', 'trip_id'],
                                        keep='first')
    return update_gtfs_db

def join_gtfs_update(gtfs_db, update_db):
    update_cols = list(update_db.columns)
    gtfs_cols = list(gtfs_db.columns)
    if 'Unnamed: 0' in update_cols:
        update_db = update_db.drop('Unnamed: 0', axis=1)
    if 'Unnamed: 0' in gtfs_cols:
        gtfs_db = gtfs_db.drop('Unnamed: 0', axis=1)
    updates_gtfs = pd.merge(update_db, gtfs_db,
                            how='left',
                            on=['route_id', 'stop_id', 'trip_id'])
    '''drop any na's caused by missing values in the schedule
        after checking multiple days this loss is ~.4% (600/140,000)'''
    updates_gtfs = updates_gtfs.dropna(thresh=14)
    return updates_gtfs
