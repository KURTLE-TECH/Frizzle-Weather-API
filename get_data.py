from datetime import datetime, timedelta,timezone
import logging
import pytz
from database import DynamodbHandler as db
from math import sin,cos,asin,sqrt,radians
from collections import defaultdict
from Endpoint_Object import Endpoint_Object
# from models.humid_script import humid_model
import redis
import random
import numpy as np
from json import loads,dumps
from math import ceil
import pandas as pd
import h2o
from production_script.weather_forecast import Forecast
import joblib
import copy
import requests
from sun_data import get_extra_info
from timestream import run_query
from concurrent.futures import ThreadPoolExecutor,as_completed



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
    db_handler = db.DynamodbHandler(region='ap-south-1')
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
            
        return distances
        
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
    
    all_times=list()
    if datetime.now(tz=pytz.timezone("Asia/Kolkata")).day == day.day:
        start_time = get_closest_half_hour(datetime.now(tz=pytz.timezone("Asia/Kolkata")))
        all_times = get_prediction_times(start_day=start_time,interval=30,days=None,time_zone="Asia/Kolkata")
    else:
        all_times = get_prediction_times(start_day=day,interval=30,days=None,time_zone="Asia/Kolkata")
    
    # data structure for the prediction   
     
    day_forecast = {"temperature":{},"pressure":{},"humidity":{},"forecast":{},"rain_probability":{},'rain_class':{},"condition":{}}
    model_object = Forecast()
    for time in all_times:
        time_string = time.strftime(format="%Y-%m-%d %H:%M:%S")
        

        #working models
        # forecasts temperature, pressure, humidity, cloud, probability of rain(converted to percentage) and forecast(along with probabilities)
        # day_forecast["temperature"][time_string] = model_object.temp_model(client_data['lat'],client_data['lng'],time.strftime(format="%Y-%m-%d %H:%M:%S"))
        # day_forecast['pressure'][time_string] = str(round(float(model_object.press_model()),1))
        
        body = model_object.transform_data(client_data['lat'],client_data['lng'],time_string)        
        # time_string = time.strftime(format="%y-%m-%d %H:%M:%S")            
        day_forecast["temperature"][time_string] = model_object.temp_forecast(body,config)
        # print(weather_forecast['temp'])
        body = np.append(body, day_forecast["temperature"][time_string]).reshape(1,-1)

        day_forecast['pressure'][time_string] = model_object.press_forecast(body,config)[0]
        body = np.append(body, day_forecast['pressure'][time_string]).reshape(1,-1)

        humid_reg_body = copy.deepcopy(body)
        humid_class_body = copy.deepcopy(body)

        # splitting into 2 as clouds work better with regression and rain works better with classification
        day_forecast['humidity'][time_string] = model_object.humid_forecast(humid_reg_body,config)[0]
        humid_reg_body = np.append(humid_reg_body, day_forecast['humidity'][time_string]).reshape(1,-1)

        humidity_class = model_object.humid_class(humid_class_body,config)
        humid_class_body = np.append(humid_class_body, humidity_class).reshape(1,-1)
        

        clouds = model_object.cloud_forecast(humid_reg_body,config)[0]
        humid_reg_body = np.append(humid_reg_body, clouds).reshape(1,-1)
        humid_class_body = np.append(humid_class_body, clouds).reshape(1,-1)

        
    
        rain_op = model_object.rain_forecast(humid_class_body,config)
        
        day_forecast['rain_probability'][time_string] = int(rain_op[1][int(rain_op[0])]*100)
        day_forecast['rain_class'][time_string] = int(rain_op[0])
        humid_reg_body = np.append(humid_reg_body, rain_op[0]).reshape(1,-1)

        
        weather_op = model_object.weath_forecast(humid_reg_body,config)

        day_forecast["condition"][time_string] = config["weather_condition"][str(weather_op[0])]                
        day_forecast['forecast'][time_string] = {config['weather_condition'][str(i)]:f"{weather_op[1][i]:.4f}" for i in range(5)}


        day_forecast['temperature'][time_string] = str(int(day_forecast['temperature'][time_string]))
        day_forecast['pressure'][time_string] = str(int(day_forecast['pressure'][time_string]))
        day_forecast['humidity'][time_string] = str(int(day_forecast['humidity'][time_string]))
        # day_forecast['clouds'][time_string] = str(int(day_forecast['clouds'][time_string]))
        day_forecast['rain_probability'][time_string] = str(int(day_forecast['rain_probability'][time_string]))
        day_forecast['rain_class'][time_string] = str(int(day_forecast['rain_class'][time_string]))

    day_forecast["feels like"] = str(random.randrange(0,50))
    day_forecast["dew_point"] = str(random.randrange(0,50))
    # print(day)
    try:
        sun_data = get_extra_info(client_data['lat'],client_data['lng'],day)
        day_forecast["Sunrise"] = sun_data["Sunrise"]
        day_forecast["Sunset"] = sun_data["Sunset"]
        day_forecast["Daylight"] = sun_data["Daylight"]
    except Exception:
        day_forecast["Sunrise"] = "NA"
        day_forecast["Sunset"] = "NA"
        day_forecast["Daylight"] = "NA"        

    # day_forecast["UV Index"] = sun_data["UV Index"]
    # day_forecast["Sunset"] = "6 pm"
    # day_forecast["UV Index"] = "5.5"
    # day_forecast["Daylight"] = "11"            
    # day_forecast["wind_speed"] = "125"            
    return day_forecast

def  get_default_forecast(time,config,client_data):
    try:
        time_string = time.strftime(format="%Y-%m-%d %H:%M:%S")
        # model_object = Endpoint_Object.Endpoint_Calls(config['region'],config['access_key'],config['secret_access_key'],config['models'])
        
        model_object = Forecast()
        weather_forecast = dict()

        body = model_object.transform_data(client_data['lat'],client_data['lng'],time_string)        
        weather_forecast['temp'] = model_object.temp_forecast(body,config)[0]
        # print(weather_forecast['temp'])
        body = np.append(body, weather_forecast['temp']).reshape(1,-1)

        weather_forecast['pressure'] = model_object.press_forecast(body,config)[0]
        body = np.append(body, weather_forecast['pressure']).reshape(1,-1)

        humid_reg_body = copy.deepcopy(body)
        humid_class_body = copy.deepcopy(body)

        # splitting into 2 as clouds work better with regression and rain works better with classification
        weather_forecast['humidity'] = model_object.humid_forecast(humid_reg_body,config)[0]
        humid_reg_body = np.append(humid_reg_body, weather_forecast['humidity']).reshape(1,-1)

        humidity_class = model_object.humid_class(humid_class_body,config)
        humid_class_body = np.append(humid_class_body, humidity_class).reshape(1,-1)
        # print(humidity_class[0])

        clouds = model_object.cloud_forecast(humid_reg_body,config)[0]
        humid_reg_body = np.append(humid_reg_body, clouds).reshape(1,-1)
        humid_class_body = np.append(humid_class_body, clouds).reshape(1,-1)

        
    
        rain_op = model_object.rain_forecast(humid_class_body,config)
        
        weather_forecast['rain_probability'] = int(rain_op[1][int(rain_op[0])]*100)
        weather_forecast['rain_class'] = int(rain_op[0])
        humid_reg_body = np.append(humid_reg_body, rain_op[0]).reshape(1,-1)

        
        weather_op = model_object.weath_forecast(humid_reg_body,config)
        
        weather_forecast['forecast'] = config["weather_condition"][str(weather_op[0])]        
        #formatting to string
        weather_forecast['temp'] = str(int(weather_forecast['temp']))
        weather_forecast['pressure'] = str(int(weather_forecast['pressure']))
        weather_forecast['humidity'] = str(int(weather_forecast['humidity']))
        # weather_forecast['clouds'] = str(int(weather_forecast['clouds']))
        weather_forecast['rain_probability'] = str(int(weather_forecast['rain_probability']))

        #the rain part should show the strength/probabolity of the weather condition and that number will replace rain probability in the dashboard
        weather_forecast['rain_probability'] = str(int(weather_op[1][weather_op[0]]*100))
        weather_forecast['rain_class'] = str(int(weather_forecast['rain_class']))
        return weather_forecast
    except Exception as e:
        (e.__traceback__.tb_lineno)
        print(e)
    

def get_data_from_redis(cluster_end_point,node_id):
    try:                
        
        response = cluster_end_point.lrange(node_id,0,0)
        node_data = loads(response[0].decode("utf-8"))                
        return node_data
        
    except Exception as e:
        return {"Status":"Failed","Reason":f"Unable to get node data {str(e)}"}

def get_data_from_timestream(node_id,client):
    SELECT_ALL = f'SELECT measure_value::varchar FROM "Frizzle_Realtime_Database"."{node_id}" ORDER BY time DESC limit 1'
    try:    
        result = run_query(SELECT_ALL,client)        
        logging.info(f"Got data from timestream for node {node_id}")
        return loads(result[0].split("=")[1][0:-3])
    except Exception as e:
        logging.error(f"Could not get data from timestream for node {node_id} due to {str(e)}")
        return {}

def get_past_data_from_timestream(node_id,client):
    SELECT_ALL = f'SELECT measure_value::varchar FROM "Frizzle_Realtime_Database"."{node_id}" ORDER BY time DESC'
    try:    
        result = run_query(SELECT_ALL,client)        
        final_op = [loads(i.split("=")[1][0:-3]) for i in result]        
        values = {i['time-stamp']:i for i in final_op}
        logging.info(f"Obtained past data from timestream for node {node_id}")
        return values
    except Exception as e:
        logging.error(f"Could not get past data from timestream for node {node_id} due to {str(e)}")
        return {}

def forecast(type,client_data,config):
    if type == "current":
        try:
            curr_time = datetime.now()            
            forecast_today = get_default_forecast(curr_time,config,client_data)       
            return {"status":"success","data":forecast_today}
        except Exception as e:
            return {"status":"fail","reason":str(e)}

    elif type == "detailed":
        all_days = get_prediction_times(start_day = datetime.now(tz=pytz.timezone("Asia/Kolkata")),interval=None,days=client_data["days"],time_zone="Asia/Kolkata")                
        forecasted_weather = dict()                
        try:
            all_days = get_prediction_times(start_day = datetime.now(),interval=None,days=client_data["days"],time_zone="Asia/Kolkata")             
            forecasted_weather = dict()                
            with ThreadPoolExecutor(max_workers=client_data["days"]) as e:            
                futures = {e.submit(get_detailed_forecast,day,config,client_data):day for day in all_days}
                for future in as_completed(futures):                    
                    forecasted_weather[futures[future].strftime("%y-%m-%d")] = future.result()            
            return {"status":"success","data":forecasted_weather}
        except Exception as e:
            {"status":"fail","reason":str(e)}

    elif type == "particular":
        client_data['year'] = int(client_data['year'])
        client_data['month'] = int(client_data['month'])
        client_data['day'] = int(client_data['day'])
        client_data['hour'] = int(client_data['hour'])
        client_data['minute'] = int(client_data['minute'])
        client_data['second'] = int(client_data['second'])  
        client_data['lat'] = float(client_data['lat'])
        client_data['lng'] = float(client_data['lng'])
        try:
        
            time_object = datetime(client_data['year'],client_data['month'],client_data['day'],hour=client_data['hour'],minute=client_data['minute'],second=client_data['second'],tzinfo=pytz.timezone("Asia/Kolkata"))   
        except Exception as e:            
            return {"status":"fail","reason":str(e)}      
            
        try:
            result = get_default_forecast(time_object,config,client_data)
            return {"status":"pass","data":result}
        except Exception as e:                    
            return {"status":"fail","reason":str(e)}   

    


