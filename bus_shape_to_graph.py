from google.cloud import bigquery
import json
from collections import defaultdict
import os
import re
import pandas as pd
import numpy as np
import geopandas as gpd
from geopandas import GeoDataFrame
from shapely.geometry import Point, LineString, MultiLineString
from shapely.ops import nearest_points
import pysal as ps
import networkx as nx
import pickle
import multiprocessing
from functools import partial
from contextlib import contextmanager
import argparse

#vehicle_table_name = 'vehicles_2017_11_15_join'
#shape_table_name = 'shapes_2017_11_15'

@contextmanager
def poolcontext(*args, **kwargs):
    pool = multiprocessing.Pool(*args, **kwargs)
    yield pool
    pool.terminate()

def bus_shape_to_graph(vehicle_table_name, shape_table_name):
    shape_id_list = get_vehicle_shape_ids(vehicle_table_name)
    n_pools = multiprocessing.cpu_count() - 2
    with poolcontext(processes=n_pools) as pool:
        pool.map(partial(update_one_shape,
                        vehicle_table_name=vehicle_table_name,
                        shape_table_name=shape_table_name),
                        shape_id_list)


def update_one_shape(shape_id, vehicle_table_name, shape_table_name):
    print("making route shape for {}".format(shape_id))
    route_vertex_geo = make_geopandas_shape_df(shape_table_name)
    #print("making trip_geo for {}".format(shape_id))
    vehicle_trip_geo_df, unique_trip_list = get_unique_trip_list_df(
                                    vehicle_table_name, shape_id)
    #print("making network for {}".format(shape_id))
    G = create_network_fromshape(route_vertex_geo, shape_id)
    for unique_trip in unique_trip_list:
        unique_trip_geo_df = vehicle_trip_geo_df[vehicle_trip_geo_df['month_day_trip']==unique_trip]
        #we need these ids in the edge row for future stats!
        trip_id = unique_trip_geo_df['veh_trip_id'].unique()[0]
        vehicle_id =unique_trip_geo_df['veh_vehicle_id'].unique()[0]
        route_id = unique_trip_geo_df['veh_route_id'].unique()[0]
        #print("making GCP edges for {}-{}".format(shape_id, trip_id))
        update_GCP_edges(unique_trip_geo_df, route_vertex_geo, G,
                        trip_id, vehicle_id, route_id, shape_id)


def make_geopandas_shape_df(shape_table_name):
    '''
    INPUT
    -------
    google bigquery shape table name
    google bigquery base = bustime-sandbox.kcm_gtfs_shapes
    OUTPUT
    -------
    route_vertex_geo <-- geopandas version of a particular schedule shapes.txt
    '''
    client = bigquery.Client.from_service_account_json(
        'bustime-keys.json')

    QUERY = (
        'SELECT * FROM `bustime-sandbox.kcm_gtfs_shapes.{}` '
        ).format(shape_table_name)
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    shapes_df = rows.to_dataframe()
    crs = {'init':'epsg:4326'}
    shape_geometry = [Point(xy) for xy in zip(shapes_df.shape_pt_lon, shapes_df.shape_pt_lat)]
    route_vertex_geo = GeoDataFrame(shapes_df, crs=crs, geometry=shape_geometry)
    return route_vertex_geo

def create_vehicle_geo(vehicle_location_df):
    '''
    '''
    crs = {'init':'epsg:4326'}
    vehicle_geometry = [Point(xy) for xy in zip(vehicle_location_df.veh_vehicle_long, vehicle_location_df.veh_vehicle_lat)]
    vehicle_location_geo = GeoDataFrame(vehicle_location_df, crs=crs, geometry=vehicle_geometry)
    return vehicle_location_geo


def create_network_fromshape(route_vertex_geo, shape_id):
    '''
    INPUT
    ------
    route_vertex_geo = king county route shapes, includes vertices for
                        each route shape
    shape_id = given a specific shape_id, create the network graph

    OUTPUT
    --------
    Networkx directional network graph G'''
    shape_name = "route_{}_geo".format(shape_id)
    shape_name = route_vertex_geo[route_vertex_geo['shape_id'] == shape_id]
    shape_name = shape_name.sort_values(by='shape_pt_sequence',
                                                axis=0, ascending=True)
    feature_list = [(x,y,dist) for x, y, dist in zip(shape_name.shape_pt_lon, shape_name.shape_pt_lat, shape_name.shape_dist_traveled)]
    edgelist = []
    list_len = len(feature_list)
    for i, item in enumerate(feature_list):
        if i+1 < list_len:
            x1 = item[0]
            y1 = item[1]
            dist1 = item[2]
            x2 = feature_list[i+1][0]
            y2 = feature_list[i+1][1]
            dist2 = feature_list[i+1][2]
            tot_dist = dist2-dist1
            edgelist.append(((x1,y1),(x2,y2),tot_dist))
    #create a directed graph
    G = nx.DiGraph()
    G.graph['rankdir'] = 'LR'
    G.graph['dpi'] = 120
    G.add_weighted_edges_from(edgelist, weight='dist')
    return G

def update_GCP_edges(vehicle_geo, route_vertex_geo, G,
                    trip_id, vehicle_id, route_id, shape_id):
    #(unique_trip_geo_df, route_vertex_geo, G,
    #trip_id, vehicle_id, route_id, shape_id)
    '''still need a function to separate all the vehicle data for one route into unique
    trips to update the graph'''
    len_veh_locs = len(vehicle_geo)
    vehicle_geo_sorted = vehicle_geo.sort_values(by='veh_time_pct', axis=0, ascending=True)
    for i, row in enumerate(vehicle_geo_sorted.iterrows()):
        if i + 1 < len_veh_locs:
            loc1 = vehicle_geo_sorted['geometry'].iloc[i].coords[:][0]
            loc2 = vehicle_geo_sorted['geometry'].iloc[i+1].coords[:][0]
            node1, node_num1, dist1 = get_close_node(loc1, route_vertex_geo)
            node2, node_num2, dist2 = get_close_node(loc2, route_vertex_geo)
            #print("node_num1 {}, node_num2 {}, dist1 {}, dist2 {}".format(node_num1, node_num2,
                                                                         #dist1, dist2))
            if (node_num1 < node_num2) and (dist1 < 0.2) and (dist2 < 0.2):
                '''print("coord1 {}, coord2 {} --> closest coord1 {}, coord2 {}".format(loc1, loc2,
                                                                                     node1, node2))
                print("node_num1 {}, node_num2 {}, dist1 {}, dist2 {}".format(node_num1, node_num2,
                                                                             dist1, dist2))'''
                try:
                    trav_dist = get_travel_distance(node1, node2, route_vertex_geo, G)
                    time1 = vehicle_geo_sorted['veh_time_pct'].iloc[i]
                    time2 = vehicle_geo_sorted['veh_time_pct'].iloc[i+1]
                    time_delta = time2 - time1
                    time_delta_hours = time_delta.total_seconds() / (60 * 60)
                    time_delta_half = time_delta.total_seconds() / 2
                    time_midway = time1 + pd.Timedelta('{} seconds'.format(time_delta_half))
                    hour = time_midway.hour
                    dow = time_midway.dayofweek
                    day = time_midway.day
                    month = time_midway.month
                    time_id = "{}_{}".format(dow, hour)
                    trav_rate_update = trav_dist/(time_delta_hours*5280)
                    '''need to find all edges in between loc1 and loc2 and update them'''
                    edge_list = get_edge_list(node1, node2, G)
                    edge_for_upload = []
                    col_list = ['pt1_lon', 'pt1_lat', 'pt2_lon', 'pt2_lat',
                                'start_time', 'end_time', 'mid_time',
                                'hour', 'dow', 'day', 'month', 'travel_rate',
                                'trip_id', 'vehicle_id', 'route_id', 'shape_id']
                    for edge in edge_list:
                        node1_lon = edge[0][0]
                        node1_lat = edge[0][1]
                        node2_lon = edge[1][0]
                        node2_lat = edge[1][1]
                        info_tuple = (node1_lon, node1_lat, node2_lon,
                                            node2_lat, time1, time2, time_midway,
                                            hour, dow, day, month, trav_rate_update,
                                            trip_id, vehicle_id, route_id, shape_id)
                        edge_for_upload.append(info_tuple)
                    edge_df = pd.DataFrame(edge_for_upload, columns=col_list)
                    #print("writing to GCP {}-{}".format(time_midway, trip_id))
                    write_to_bigquery(edge_df, trip_id, month, day)
                except nx.NetworkXNoPath:
                    time1 = vehicle_geo_sorted['veh_time_pct'].iloc[i]
                    output_str = (
                    "{}{}\n{}{}\n{}{}\n\
                    ".format('node1 -', node1,
                            'node2 -', node2,
                            'shape_id -', shape_id))
                    file_path = './bad_network_nodes.txt'
                    with open(file_path, "a") as f:
                        f.write(time1+" pre-pipeline")
                        f.write("\n")
                        f.write(output_str)
                        f.write("\n")

def get_close_node(raw_loc, route_vertex_geo):
    '''
    INPUT
    -----------
    raw_loc = lat/lon coord in a tuple form. e.g. (-122.30731999999999, 47.625236)
    route_vertex_geo = geodataframe from route shape
    OUTPUT
    -------
    near_node = nearest route node
    ---------
    given a raw GPS coordinate from one bus away, get the closest
    bus route shape vertex node
    we'll later use this node to update the graph attributes
    '''
    veh_pt = Point(raw_loc)
    route_vertex_geo['distance'] = route_vertex_geo.distance(veh_pt)
    route_vertex_geo_sorted = route_vertex_geo.sort_values(by=['distance'], axis=0, ascending=True)
    #add filter for distance too far away
    distance = route_vertex_geo_sorted.iloc[0]['distance']
    near_node = route_vertex_geo_sorted.iloc[0].geometry.coords[:][0]
    node_num = route_vertex_geo_sorted.iloc[0]['shape_pt_sequence']
    return near_node, node_num, distance

def get_travel_distance(loc1, loc2, route_vertex_geo, G):
    '''
    INPUT
    -------
    loc1 = raw bus location #1
    loc2 = raw bus location #2 - e.g. (-122.30731999999999, 47.625236)
    route_vertex_geo = geodataframe from route shape
    G = network graph of route shape
    OUTPUT
    -------
    trav_dist = distance traveled along the network between two bus locations'''
    node1, node_num, dist1 = get_close_node(loc1, route_vertex_geo)
    node2, node_num, dist2 = get_close_node(loc2, route_vertex_geo)
    trav_dist = nx.shortest_path_length(G,node1,node2, weight='dist')
    return trav_dist

def get_edge_list(loc1, loc2, G):
    '''get edge list in between two bus locations'''
    node_list = nx.dijkstra_path(G,loc1,loc2)
    edge_list = []
    for i, node in enumerate(node_list):
        if i+1 < len(node_list):
            node1 = node_list[i]
            node2 = node_list[i+1]
            edge_list.append((node1,node2))
    return edge_list

def get_vehicle_shape_ids(vehicle_table_name):
    '''
    INPUT
    -------
    google bigquery table name
    google bigquery base = bustime-sandbox.vehicle_data
    OUTPUT
    -------
    shape_id_list <-- all the unique shape_ids for that
    list of vehicle trips
    '''
    client = bigquery.Client.from_service_account_json(
        'bustime-keys.json')

    QUERY = (
        'SELECT sched_shape_id FROM `bustime-sandbox.vehicle_data.{}` '
        'WHERE sched_shape_id IS NOT NULL '
        'GROUP BY sched_shape_id').format(vehicle_table_name)
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    df = rows.to_dataframe()
    shape_values = df['sched_shape_id'].values
    shape_id_list = shape_values.tolist()
    return shape_id_list

def get_unique_trip_list_df(vehicle_table_name, shape_id):
    '''
    INPUT
    -------
    google bigquery table name
    google bigquery base = bustime-sandbox.vehicle_data
    shape_id = int shape id from kcm schedule
    OUTPUT
    -------
    vehicle_trip_df = vehicle_table_name with 'month_day_trip_id' col
    unique_trip_list <-- all the unique trips (month_day_trip_id)
    '''
    client = bigquery.Client.from_service_account_json(
        'bustime-keys.json')
    QUERY = (
        'SELECT sched_shape_id, veh_month, veh_day, veh_trip_id, veh_vehicle_id, veh_route_id, veh_time_pct, veh_vehicle_lat, veh_vehicle_long FROM `bustime-sandbox.vehicle_data.{}` '
        'WHERE sched_shape_id = {} '
        'GROUP BY sched_shape_id, veh_month, veh_day, veh_trip_id, veh_vehicle_id, veh_route_id, veh_time_pct, veh_vehicle_lat, veh_vehicle_long '
        ).format(vehicle_table_name, shape_id)
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    vehicle_trip_df = rows.to_dataframe()
    vehicle_trip_df['month_day_trip'] = vehicle_trip_df.apply(get_unique_trip_id, axis=1)
    month_day_trip_df = vehicle_trip_df.groupby(by='month_day_trip')
    unique_trip_list = list(month_day_trip_df.groups.keys())
    vehicle_trip_geo_df = create_vehicle_geo(vehicle_trip_df)
    return vehicle_trip_geo_df, unique_trip_list

def get_unique_trip_id(row):
    unique_trip = str(row['veh_month'])+"_"+str(row['veh_day'])+"_"+str(row['veh_trip_id'])
    return unique_trip

def write_to_bigquery(df, trip_id, month, day):
    '''
    '''
    #bigquery params
    client = bigquery.Client.from_service_account_json(
    'bustime-keys.json')
    filename = 'temp_veh_edges_{}_{}_{}.csv'.format(trip_id, month,day)
    dataset_id = 'vehicle_data'
    table_id = 'vehicle_edges'

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("vehicle_table_name", type=str,
                    help="enter the vehicle_table_name")
    parser.add_argument("shape_table_name", type=str,
                    help="enter the shape_table_name")
    args = parser.parse_args()
    vehicle_table_name = args.vehicle_table_name
    shape_table_name = args.shape_table_name
    bus_shape_to_graph(vehicle_table_name, shape_table_name)
