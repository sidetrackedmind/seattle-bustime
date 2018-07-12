from google.protobuf.json_format import MessageToJson, MessageToDict
import json
from collections import defaultdict
import os
import re
import pandas as pd
import numpy as np
from pandasql import sqldf
import matplotlib.pyplot as plt
import geopandas as gpd
from geopandas import GeoDataFrame
from shapely.geometry import Point, LineString, MultiLineString
from shapely.ops import nearest_points
import psycopg2
import pysal as ps
import networkx as nx
import pickle


def update_graphs_all_trips_to_dict(position_db, schedule_df, route_vertex_shapes, G_dict):
    '''all functions together'''
    position_db_clean = clean_position_db(position_db)
    position_geo = create_vehicle_geo(position_db_clean)
    route_list = list(position_geo['route_id'].unique())
    for route_id in route_list:
        trip_list = list(position_geo[(position_geo['route_id']==route_id)]['trip_id'].unique())
        for trip_id in trip_list:
            print("creating/updating graph for route_id:{}, trip_id:{}".format(route_id, trip_id))
            position_trip_geo = position_geo[(position_geo['route_id']==route_id) & (position_geo['trip_id']==trip_id)]
            shape_id = get_shape_id_from_triproute(trip_id, route_id, schedule_df)
            route_vertex_geo = create_route_vertex_geo(route_vertex_shapes, shape_id)
            G_route = check_and_get_graph(route_vertex_geo, shape_id, G_dict)
            G_route_updated = update_graph(position_trip_geo, route_vertex_geo, G_route)
            G_dict[shape_id] = G_route_updated
    return G_dict

def get_shape_id_from_triproute(trip_id, route_id, schedule_df):
    '''get shape_id for that particular trip'''
    shape_id = schedule_df[(schedule_df['route_id']==int(route_id)) & (schedule_df['trip_id']==int(trip_id))]['shape_id'].unique()[0]
    return shape_id

def create_route_vertex_geo(route_vertex_shapes, shape_id):
    '''
    '''
    one_route_shape = route_vertex_shapes[route_vertex_shapes['shape_id'] == shape_id]
    crs = {'init':'epsg:4326'}
    one_route_vertice_geometry = [Point(xy) for xy in zip(one_route_shape.shape_pt_lon, one_route_shape.shape_pt_lat)]
    one_route_vertex_geo = GeoDataFrame(one_route_shape, crs=crs, geometry=one_route_vertice_geometry)
    return one_route_vertex_geo

def create_vehicle_geo(vehicle_location_df):
    '''
    '''
    crs = {'init':'epsg:4326'}
    vehicle_geometry = [Point(xy) for xy in zip(vehicle_location_df.vehicle_long, vehicle_location_df.vehicle_lat)]
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



def update_graph(vehicle_geo, route_vertex_geo, G):
    '''still need a function to separate all the vehicle data for one route into unique
    trips to update the graph'''
    len_veh_locs = len(vehicle_geo)
    for i, row in enumerate(vehicle_geo.iterrows()):
        if i + 1 < len_veh_locs:
            loc1 = vehicle_geo['geometry'].iloc[i].coords[:][0]
            loc2 = vehicle_geo['geometry'].iloc[i+1].coords[:][0]
            node1, node_num1, dist1 = get_close_node(loc1, route_vertex_geo)
            node2, node_num2, dist2 = get_close_node(loc2, route_vertex_geo)
            #print("node_num1 {}, node_num2 {}, dist1 {}, dist2 {}".format(node_num1, node_num2,
                                                                         #dist1, dist2))
            if (node_num1 < node_num2) and (dist1 < 0.2) and (dist2 < 0.2):
                #print("coord1 {}, coord2 {} --> closest coord1 {}, coord2 {}".format(loc1, loc2,
                                                                                     #node1, node2))
                trav_dist = get_travel_distance(node1, node2, route_vertex_geo, G)
                time1 = vehicle_geo['time_pct'].iloc[i]
                time2 = vehicle_geo['time_pct'].iloc[i+1]
                time_delta = time2 - time1
                time_delta_hours = time_delta.total_seconds() / (60 * 60)
                time_delta_half = time_delta.total_seconds() / 2
                time_midway = time1 + pd.Timedelta('{} seconds'.format(time_delta_half))
                hour = time_midway.hour
                dow = time_midway.dayofweek
                time_id = "{}_{}".format(dow, hour)
                trav_rate_update = trav_dist/(time_delta_hours*5280)
                print(time_id)
                time_dict = defaultdict(list)
                '''need to find all edges in between loc1 and loc2 and update them'''
                edge_list = get_edge_list(node1, node2, G)

                for edge in edge_list:
                    time_dict = defaultdict(list)
                    loc1 = edge[0]
                    loc2 = edge[1]
                    if 'overall_trav_rate' in G.get_edge_data(loc1, loc2).keys():
                        overall_trav_rate = G.get_edge_data(loc1, loc2)['overall_trav_rate']
                        overall_trav_rate.append(trav_rate_update)
                        G.add_edge(loc1, loc2, overall_trav_rate = overall_trav_rate)
                    else:
                        overall_trav_rate = []
                        overall_trav_rate.append(trav_rate_update)
                        G.add_edge(loc1, loc2, overall_trav_rate = overall_trav_rate)
                    if 'time_dict' in G.get_edge_data(loc1, loc2).keys():
                        time_dict = G.get_edge_data(loc1, loc2)['time_dict']
                        time_dict[time_id].append(trav_rate_update)
                        G.add_edge(loc1, loc2, time_dict = time_dict)
                    else:
                        time_dict[time_id].append(trav_rate_update)
                        G.add_edge(loc1, loc2, time_dict = time_dict)
        '''need a function that updates all route vertices,
        --> trav_rate = trav_rate / num_obs
        --> think about storing trav_rate as a list
        at the end including edge data from list (i.e. +90 -90 percentile, avg, etc)'''
    return G


def check_and_get_graph(route_vertex_geo, shape_id, G_dict):
    '''
    purpose of this function is to check for an existing network graph (based on shape_id)
    if a graph exists, return the graph
    if a graph does not exist, make it'''
    if shape_id in G_dict.keys():
        G = G_dict[shape_id]
    else:
        G = create_network_fromshape(route_vertex_geo, shape_id)
    return G

def update_graph_dict(G_updated, shape_id, G_dict):
    '''function to update the G_dict with new (or updated) graph'''
    G_dict[shape_id] = G_updated

def make_geopandas_from_graph(graph_input):
    dataframe_list = []
    geometry = []
    for edge in graph_input.edges:
        if 'overall_trav_rate' in graph_input.get_edge_data(edge[0], edge[1]).keys():
            rate_array = np.array(graph_input.get_edge_data(edge[0], edge[1])['overall_trav_rate'])
            shape_id = 21221091
            avg_rate = np.average(rate_array)
            per_10 = np.percentile(rate_array, 10)
            per_90 = np.percentile(rate_array, 90)
            from_pt_lat = edge[0][0]
            from_pt_lon = edge[0][1]
            to_pt_lat = edge[1][0]
            to_pt_lon = edge[1][1]
            pt1 = (from_pt_lat, from_pt_lon)
            pt2 = (to_pt_lat, to_pt_lon)
            geo_temp = LineString([pt1, pt2])
            dataframe_list.append((shape_id, per_10, avg_rate, per_90, from_pt_lat, from_pt_lon,
                                  to_pt_lat, to_pt_lon))
            geometry.append(geo_temp)
    dataframe_out = pd.DataFrame(dataframe_list, columns= ['shape_id','per_10', 'avg_rate', 'per_90', 'from_pt_lat', 'from_pt_lon',
                              'to_pt_lat', 'to_pt_lon'])
    crs = {'init':'epsg:4326'}
    geo_dataframe_out = GeoDataFrame(dataframe_out, crs=crs, geometry=geometry)
    return geo_dataframe_out

def merge_route_graphs(graph1, graph2, new_G):
    '''need two graphs for the same route and 1 new graph as input
    the new graph could be a previously made overlapping graph or a completely
    new graph'''
    for edge1, edge2 in zip(graph1.edges, graph2.edges):
        overlap_val = 0
        if 'overall_trav_rate' in graph1.get_edge_data(edge1[0], edge1[1]).keys():
            overlap_val += 1
        if 'overall_trav_rate' in graph2.get_edge_data(edge2[0], edge2[1]).keys():
            overlap_val += 2
        if overlap_val == 1:
            loc1 = edge1[0]
            loc2 = edge1[1]
            dist = graph1.get_edge_data(loc1, loc2)['dist']
            overall_trav_rate_list = graph1.get_edge_data(loc1, loc2)['overall_trav_rate']
            time_dict = graph1.get_edge_data(loc1, loc2)['time_dict']
            new_G.add_edge(loc1, loc2, dist = dist, overall_trav_rate = overall_trav_rate_list,
                           time_dict=time_dict)
        if overlap_val == 2:
            loc1 = edge2[0]
            loc2 = edge2[1]
            dist = graph2.get_edge_data(loc1, loc2)['dist']
            overall_trav_rate_list = graph2.get_edge_data(loc1, loc2)['overall_trav_rate']
            time_dict = graph2.get_edge_data(loc1, loc2)['time_dict']
            new_G.add_edge(loc1, loc2, dist = dist, overall_trav_rate = overall_trav_rate_list,
                           time_dict=time_dict)
        if overlap_val == 3:
            loc1 = edge1[0]
            loc2 = edge1[1]
            dist = graph2.get_edge_data(loc1, loc2)['dist']
            overall_trav_rate_list1 = graph1.get_edge_data(loc1, loc2)['overall_trav_rate']
            overall_trav_rate_list2 = graph2.get_edge_data(loc1, loc2)['overall_trav_rate']
            overall_trav_rate_list1.append(overall_trav_rate_list2)
            new_time_dict = defaultdict(list)
            time_dict1 = graph1.get_edge_data(loc1, loc2)['time_dict']
            time_dict2 = graph2.get_edge_data(loc1, loc2)['time_dict']
            time_key1 = graph1.get_edge_data(loc1, loc2)['time_dict'].keys()
            time_key2 = graph2.get_edge_data(loc1, loc2)['time_dict'].keys()
            for time_key in time_key1:
                if time_key in time_key2:
                    val_list1 = time_dict1[time_key]
                    val_list2 = time_dict2[time_key]
                    val_list1.append(val_list2)
                    new_time_dict[time_key] = val_list1
                else:
                    new_time_dict[time_key] = time_dict1[time_key]
            for time_key in time_key2:
                if time_key not in time_key1:
                    new_time_dict[time_key] = time_dict2[time_key]
            new_G.add_edge(loc1, loc2, dist = dist, overall_trav_rate = overall_trav_rate_list1,
                           time_dict=new_time_dict)
    return new_G
