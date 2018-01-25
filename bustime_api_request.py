import requests
import boto3
import datetime
import time
import logging
import os

api_key = os.environ["ONEBUSAWAY_KEY"]

agency = '1'  # this is the agency ID for King County Metro
base_url = 'http://pugetsound.onebusaway.org/api/'
endpoints = {'position': 'gtfs_realtime/vehicle-positions-for-agency/{agency}.pb',
           'alert': 'gtfs_realtime/alerts-for-agency/{agency}.pb',
           'update': 'gtfs_realtime/trip-updates-for-agency/{agency}.pb'}

def build_filename(endpoint_name):
    year = datetime.datetime.now().strftime("%Y")
    month = datetime.datetime.now().strftime("%m")
    day = datetime.datetime.now().strftime("%d")
    hour = datetime.datetime.now().strftime("%H")
    prefix = year+'/'+month+'/'+day+'/'+hour+'/'
    current_datetime = datetime.datetime.now().strftime("%M_%S")
    filename = prefix + current_datetime + '_1_' + endpoint_name + '.pb'
    return filename


def single_request(url, api_key):
    params = {'key': api_key}
    r = requests.get(url,params=params)
    try:
        if r.status_code == 200:
            return r.content
    except:
        logging.error('status code {}'.format(r.status_code))

def request_realtime(base_url, endpoints, agency, api_key):
    bucket_name = os.environ["BUS_BUCKET_NAME"]
    for endpoint_name, endpoint in endpoints.items():
        url = base_url + endpoint.format(agency=agency)
        result = single_request(url, api_key)
        filename = build_filename(endpoint_name)
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_name, Body=result, Key=filename)
        #with open(filename, 'wb') as outfile:
        #    outfile.write(result)
        time.sleep(5)

if __name__ == '__main__':
    request_realtime(base_url, endpoints, agency, api_key)
