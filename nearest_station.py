#grabbed the following from moses marsh -- https://github.com/sidetrackedmind/gimme-bus/blob/master/gimmebus/utilities.py

from datetime import datetime as dt
from math import radians, cos, sin, acos, asin, sqrt
import networkx as nx

## These functions will go in model.py for matching historical GPS
## positions to the defined route shapes

def haversine(pt1, pt2):
    """
    INPUT: tuples (lon1, lat1), (lon2, lat2)

    OUTPUT: The great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [pt1[0], pt1[1], pt2[0], pt2[1]])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2.)**2 + cos(lat1) * cos(lat2) * sin(dlon/2.)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

def get_closest_shape_pt(lat, lon, shape):
    dist = shape.apply(lambda x: haversine((x['shape_pt_lon'], \
                        x['shape_pt_lat']), (lon, lat)), axis=1)
    return dist.argmin()

def distance_along_route(pt_1_ind, pt_2_ind, shape):
    d1 = shape.loc[pt_1_ind]['shape_dist_traveled']
    d2 = shape.loc[pt_2_ind]['shape_dist_traveled']
    return d2 - d1

def distance_from_segment(pt, seg_pt_1, seg_pt_2):
    c = haversine(seg_pt_1, seg_pt_2)
    b = haversine(seg_pt_1, pt)
    a = haversine(seg_pt_2, pt)

    num1 = (b**2 + c**2 - a**2)
    num2 = (a**2 + c**2 - b**2)

    if (num1 < 0) or (num2 < 0):
        return min(a, b)

    theta = acos( num1 / (2.*b*c))
    h = b * sin(theta)

    return h
