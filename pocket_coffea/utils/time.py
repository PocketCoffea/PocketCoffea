from datetime import datetime
from time import sleep

def wait_until(time):
    '''Wait until the given time'''
    if type(time) == str:
        date_format = "%d-%m-%Y %H:%M:%S"
        time = datetime.strptime(time, date_format)
    elif type(time) == datetime:
        pass
    else:
        raise TypeError("The time argument must be a string or a datetime object.")        
    while datetime.now() < time:
        sleep(1)
        pass
