from datetime import datetime, timedelta,timezone
import logging
import pytz
from database import DynamodbHandler as db
from math import sin,cos,asin,sqrt,radians
from collections import defaultdict
from Endpoint_Object import Endpoint_Object
import redis
import random
from json import loads
#start, end, intervals
def get_prediction_times(**kwargs):
    try:
        time_zone = pytz.timezone(kwargs['time_zone'])    
    except Exception:        
        time_zone = pytz.timezone("Asia/Kolkata")    
    start_day = kwargs['start_day']
    prediction_intervals = list()
    required_time = start_day    
    
    
    if kwargs['days'] != None :
        end_day = start_day+timedelta(days=kwargs['days'])
        end_day = end_day.replace(hour=0,minute=0,second=0,microsecond=0,tzinfo=time_zone)
        
    else:
        end_day = start_day+timedelta(days=1)
        end_day = end_day.replace(hour=0,minute=0,second=0,microsecond=0,tzinfo=time_zone)
    

    if kwargs['interval']!=None:
        while required_time<=end_day:
            prediction_intervals.append(required_time)
            required_time+=timedelta(minutes=kwargs['interval'])
    else:
        required_time = required_time.replace(hour=0,minute=0,second=0,microsecond=0,tzinfo=time_zone)        
        while required_time<=end_day:
            prediction_intervals.append(required_time)
            required_time+=timedelta(days=1)
    
    return prediction_intervals

def get_distance(node_location,location):
    #does not take into account
    r = 6371    
    location = {i:radians(float(location[i])) for i in location.keys()}
    node_location = {i:radians(float(node_location[i])) for i in node_location.keys()}
    lat1 = node_location['lat']
    lat2 = location['lat']
    dlon = location['lng'] - node_location['lng'] 
    dlat = location['lat'] - node_location['lat']     
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))     
    return c * r
    #cannot use euclidean distance
    #return ((location['lat']-node_location['lat'])**2+(location['lng']-node_location['lng'])**2)**0.5 
    
def get_closest_node(location):
    db_handler = db.DynamodbHandler()
    response = db_handler.view_database('Nodes_Available')
    if 'Items' in response.keys():
    #model is default model        
        distances=defaultdict(None)
        for node in response['Items']:
            node_location = dict()
            try:
                node_location['lat'] = node['lat']['S'] 
                node_location['lng'] = node['lng']['S']
                if node_location['lat'] !='' and node_location['lng'] != '' and node['registration']['S'] == 'complete':
                    distance = get_distance(location,node_location)
                    distances[distance] = node['Device ID']['S'] 
            except Exception:
                continue    
            
        return distances[min(distances.keys())]
        


    elif 'status' in response.keys():
        return None

def get_log(log_level,request,error):
    curr_time = datetime.now().strftime(format="%y-%m-%d %H:%M:%S:%f")
    if log_level==logging.INFO or log_level==logging.WARN :
        return f"Address: {request.remote_addr}%s - Request Path {request.path} - Time {curr_time}"                        
    elif log_level == logging.ERROR:
        return f"Address: {request.remote_addr}%s - Request Path: {request.path} - Time: {curr_time} - Reason: {error}"

def get_closest_half_hour(curr_time):        
    if curr_time.minute<=30:
        start_time = curr_time+timedelta(minutes=30-curr_time.minute) 
    else:
        start_time = curr_time+timedelta(minutes=60-curr_time.minute) 
    return start_time

def get_detailed_forecast(day,config,client_data):    
    model_object = Endpoint_Object.Endpoint_Calls(config['region'],config['access_key'],config['secret_access_key'],config['models'])
    all_times=list()
    if datetime.now(tz=pytz.timezone("Asia/Kolkata")).day == day.day:
        start_time = get_closest_half_hour(datetime.now(tz=pytz.timezone("Asia/Kolkata")))
        all_times = get_prediction_times(start_day=start_time,interval=30,days=None,time_zone="Asia/Kolkata")
    else:
        all_times = get_prediction_times(start_day=day,interval=30,days=None,time_zone="Asia/Kolkata")
    
    # data structure for the prediction    
    day_forecast = {"temperature":{},"pressure":{},"humidity":{},"rain":{},"forecast":{},"rain_probability":{}}
    for time in all_times:
        time_string = time.strftime(format="%Y-%m-%d %H:%M:%S")
        

        #working models
        day_forecast["temperature"][time_string] = model_object.temp_model(client_data['lat'],client_data['lng'],time_string)
        day_forecast['pressure'][time_string] = str(round(float(model_object.press_model()),1))                                
        day_forecast['humidity'][time_string] = model_object.humid_model().split(",")[0]

        #models to add. currently dummy models                
        day_forecast['rain_probability'][time_string] = model_object.rain_model()
        day_forecast["forecast"][time_string] = {"sunny":str(random.random()),"cloudy":str(random.random()),"rainy":str(random.random())}    
        day_forecast["feels like"] = str(random.randrange(0,50))
        day_forecast["dew point"] = str(random.randrange(0,50))
        day_forecast["sun rise"] = "6 am"
        day_forecast["sun set"] = "6 pm"
        day_forecast["uv index"] = "5.5"
        day_forecast["day light duration"] = "11"            
    return day_forecast

def get_data_from_redis(cluster_end_point,node_id):
    try:        
        node_data = loads(cluster_end_point[node_id])
        return node_data
    except Exception as e:
        return None