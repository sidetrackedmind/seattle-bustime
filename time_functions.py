#taken from moses marsh

from datetime import datetime as dt

def diff_timestamps(s1, s2):
    """
    INPUT: strings of the format "HH:mm:ss"
            Per GTFS rules, HH can take values greater than 24
            (due to trips that extend past midnight)
    OUTPUT: time in seconds between s1 and s2
    """
    if int(s1[:2]) >= 24:
        s1 = '0' + str(int(s1[:2])%24) + s1[2:]
    if int(s2[:2]) >= 24:
        s2 = '0' + str(int(s2[:2])%24) + s2[2:]
    tdelt = dt.strptime(s2, '%H:%M:%S') - dt.strptime(s1, '%H:%M:%S')
    return tdelt.seconds

#from nyc-bus-performance/metrics

def generateTime(hour):
    '''
    function to generate
    7-10 am : 1
    10-16 pm : 2
    16-19 pm : 3
    19- 22: 4
    ignoring late nights for these metrics : 5
    '''
    if (hour > 6) & (hour < 11):
        return 1
    elif (hour > 10) & (hour < 17):
        return 2
    elif (hour > 16) & (hour < 20):
        return 3
    elif (hour > 19) & (hour < 23):
        return 4
    else:
        return 5

merged['timeperiod'] = merged['arrival_time'].apply(lambda x: generateTime(x))
