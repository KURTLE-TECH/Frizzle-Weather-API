from datetime import datetime, timedelta,timezone
import logging
from time import strftime
# from pyexpat import model
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
import aqi
from weasyprint import HTML,CSS
import os

def get_prediction_times(**kwargs):
    try:
        time_zone = pytz.timezone(kwargs['time_zone'])  
    except Exception:        
        time_zone = pytz.timezone("Asia/Kolkata")    
    if kwargs['days']==None:
        start_day = time_zone.localize(kwargs['start_day'])
    else:
        start_day = kwargs['start_day']
    prediction_intervals = list()
    required_time = start_day    
    
    
    if kwargs['days'] != None :
        end_day = start_day+timedelta(days=kwargs['days'])
        end_day = end_day.replace(hour=0,minute=0,second=0,microsecond=0)
        
    else:
        end_day = start_day+timedelta(days=1)
        end_day = end_day.replace(hour=0,minute=0,second=0,microsecond=0)
    

    if kwargs['interval']!=None:
        while required_time<=end_day:
            prediction_intervals.append(required_time)
            required_time+=timedelta(minutes=kwargs['interval'])
    else:
        required_time = required_time.replace(hour=0,minute=0,second=0,microsecond=0)        
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

def round_to_hour(curr_time):        
    if curr_time.minute<30:
        start_time = curr_time-timedelta(minutes=curr_time.minute) 
    else:
        start_time = curr_time+timedelta(minutes=60-curr_time.minute) 
    return start_time


def predict_weather(time, config, client_data):
    try:
        time_string = time.strftime(format="%Y-%m-%d %H:%M:%S")
        # model_object = Endpoint_Object.Endpoint_Calls(config['region'],config['access_key'],config['secret_access_key'],config['models'])

        model_object = Forecast()
        weather_forecast = dict()

        body = model_object.transform_data(client_data['lat'], client_data['lng'], client_data['alt'], time_string)
        
        weather_forecast['temp'] = model_object.temp_forecast(body, config)[0]
        
        body = np.append(body, weather_forecast['temp']).reshape(1, -1)
        
        weather_forecast['pressure'] = model_object.press_forecast(body, config)[0]
        body = np.append(body, weather_forecast['pressure']).reshape(1, -1)
        

        humidity_output = model_object.humid_class(body, config)[0]
        weather_forecast['humidity'] = config["humidity_class"][str(
            humidity_output)]
        body = np.append(body, humidity_output).reshape(1, -1)    
        
        clouds = model_object.cloud_forecast(pd.DataFrame(body, columns=['lat', 'lon', 'dayofweek', 'quarter', 'month', 'dayofyear', 'dayofmonth', 'weekofyear', 'minutes', 'year', 'altitude', 'temp', 'pressure', 'humidity']), config)[0]
        # clouds = model_object.cloud_forecast(body,config)
        body = np.append(body, clouds).reshape(1, -1)
        rain_op = model_object.rain_forecast(body, config)

        weather_forecast['rain_class_probability'] = int(
            rain_op[1][int(rain_op[0])]*100)
        # weather_forecast['rain_class_probability'] = rain_op[1]

        weather_forecast['rain_class'] = int(rain_op[0])
        body = np.append(body, rain_op[0]).reshape(1, -1)

        weather_op = model_object.weath_forecast(body, config)
        weather_forecast['weather'] = weather_op
        weather_forecast['weather_probabilities'] = model_object.weath_forecast_op(body,config)
        # print(weather_forecast['w'])
        return weather_forecast
    except Exception as e:
        logging.error("predict weather error "+ e.__str__+" "+f" {e.__traceback__.tb_lineno}")
        return {}

def predict_weather_batch(times_dataframe,client_data,config):
    try:
        # model_object = Forecast()
        # # print(times_dataframe)
        # times_dataframe['temp'] = model_object.temp_forecast(times_dataframe, config)
        # times_dataframe['pressure'] = model_object.press_forecast(times_dataframe, config)
        # times_dataframe['humidity'] = model_object.humid_class(times_dataframe, config)
        # times_dataframe['clouds_all'] = model_object.cloud_forecast(times_dataframe, config)        
        # # times_dataframe['clouds_all'] = times_dataframe['clouds_all'].apply(lambda x:int(x))
        # times_dataframe['rain_1h'], rain_op_prob = model_object.rain_forecast_new(times_dataframe, config)
        # # times_dataframe['rain_1h'] = times_dataframe['rain_1h'].apply(lambda x:int(x))        
        # times_dataframe['forecast'],_ = model_object.weath_forecast(times_dataframe, config)                 
        # times_dataframe['rain_class_probability'] = rain_op_prob     

        model_object = Forecast()    

        times_dataframe['temp'] = model_object.temp_forecast(times_dataframe, config)                
        

        times_dataframe['pressure']= model_object.press_forecast(times_dataframe, config)
        

        times_dataframe['humidity']  = model_object.humid_class(times_dataframe, config)
        

        times_dataframe['clouds_all'] = model_object.cloud_forecast(times_dataframe, config)           
        

        times_dataframe['rain_1h'] = model_object.rain_forecast_new(times_dataframe, config)
        

        #times_dataframe['rain_class_probability'] = model_object.rain_forecast_prob(times_dataframe, config)         
        rain_op_probability = model_object.rain_forecast_prob(times_dataframe, config)
             
        weather_class_op = model_object.weath_forecast(times_dataframe, config)        
	
        times_dataframe['weather_probability'] = max(model_object.weath_forecast_op(times_dataframe, config)[0])
        times_dataframe['rain_class_probability'] = rain_op_probability        
        times_dataframe['forecast'] = weather_class_op
        return times_dataframe
    
    except Exception as e:
        logging.error(str(e)+f"{e.__traceback__.tb_lineno}")
        return {}

def predict_weather_batch_dashboard(times_dataframe,client_data,config):
    try:
        model_object = Forecast()    

        tempa = model_object.temp_forecast(times_dataframe, config)                
        times_dataframe['temp'] = tempa

        pres= model_object.press_forecast(times_dataframe, config)
        times_dataframe['pressure'] = pres

        hum = model_object.humid_class(times_dataframe, config)
        times_dataframe['humidity'] = hum

        clouda = model_object.cloud_forecast(times_dataframe, config)  
        times_dataframe['clouds_all'] = clouda   
        

        raina= model_object.rain_forecast_new(times_dataframe, config)
        times_dataframe['rain_1h'] = raina


        rain_op_prob = model_object.rain_forecast_prob(times_dataframe, config)
        times_dataframe['rain_class_probability'] = rain_op_prob
        
             
        forecast = model_object.weath_forecast(times_dataframe, config)        
        times_dataframe['forecast'] = forecast

        forecast_op = model_object.weath_forecast_op(times_dataframe, config) 
        #logging.info(forecast_op)        

        times_dataframe['0'] = np.array([i[0] for i in forecast_op])
        times_dataframe['1'] = np.array([i[1] for i in forecast_op])
        times_dataframe['2'] = np.array([i[2] for i in forecast_op])
        times_dataframe['3'] = np.array([i[3] for i in forecast_op])
        times_dataframe['4'] = np.array([i[4] for i in forecast_op])
        times_dataframe['5'] = np.array([i[5] for i in forecast_op])


        return times_dataframe
    except Exception as e:
        logging.error(str(e)+f"{ e.__traceback__.tb_lineno}")
        return {}

def get_summary_detailed_forecast(day,config,client_data):            
    all_times = get_prediction_times(start_day=day,interval=60,days=None,time_zone="Asia/Kolkata")
    
    # data structure for the prediction   
     
    day_forecast = {i.strftime("%Y-%m-%d %H:%M:%S"):{} for i in all_times}
    
    all_times_dataframe = pd.DataFrame(all_times,columns=["datetime"])      
    
    all_times_dataframe = pre_process_times(all_times,client_data)       
    
    # all_times_dataframe = all_times_dataframe[['lat','lon','dayofweek', 'quarter', 'month','dayofyear', 'dayofmonth', 'weekofyear','minutes','year','altitude']]
    
    
    #holy grail
    predicted_dataframe = predict_weather_batch_dashboard(all_times_dataframe.drop('datetime',axis=1),client_data,config)   
    #print(predicted_dataframe[['hour','minutes','year','dayofmonth','year','temp','pressure','humidity','clouds_all','rain_1h','0','1','2','3','4','5']])
    
    # df = predicted_dataframe[['temp','pressure','humidity','clouds_all','rain_1h','rain_class_probability','forecast','forecast_probabilities']]                      
    weather_forecast = post_process_predictions_dashboard(predicted_dataframe,all_times,config)
    # print(weather_forecast)

    
    for time in all_times:
        time_string = time.strftime(format="%Y-%m-%d %H:%M:%S")
        day_forecast[time_string]['forecast'] = None        
        day_forecast[time_string]['temperature'] = None
        day_forecast[time_string]['forecast_class_probability'] = None
        day_forecast[time_string]['rain_class'] = None
                
        day_forecast[time_string]["forecast"] = weather_forecast[time_string]['forecast']        
        day_forecast[time_string]['temperature'] = str(weather_forecast[time_string]['temp'])    
        try:
            day_forecast[time_string]['forecast_class_probability'] = str(int(max([float(i) for i in weather_forecast[time_string]['forecast_probabilities'].values()])*100))
        except Exception as e:
            logging.error("Error while getting probability of class")
            logging.error(weather_forecast[time_string]['forecast_probabilities'])
        day_forecast[time_string]['rain_class'] = str(weather_forecast[time_string]['rain_class'])
    # del weather_forecast

    # day_forecast["feels like"] = str(random.randrange(0,50))
    # day_forecast["dew_point"] = str(random.randrange(0,50))
    # print(day)
    # try:
    #     sun_data = get_extra_info(client_data['lat'],client_data['lng'],day)
    #     #logging.info(sun_data)
    #     day_forecast["Sunrise"] = sun_data["Sunrise"]
    #     day_forecast["Sunset"] = sun_data["Sunset"]
    #     day_forecast["Daylight"] = sun_data["Daylight"]
    # except Exception:
    #     day_forecast["Sunrise"] = "NA"
    #     day_forecast["Sunset"] = "NA"
    #     day_forecast["Daylight"] = "NA"        
       
    return day_forecast

def generate_report_page(client_data,day_info,config):
    try:
        base_url = os.path.dirname(os.path.realpath(__file__))
        forecast_data = copy.deepcopy(config['0-11_template'])
        file_prefix = None
        all_times = sorted(day_info.keys())
        for i in all_times:
            logging.info(i)
            hour = i.split(" ")[1].split(":")[0]
            logging.info("hour here")
            logging.info(hour)
            forecast_data = forecast_data.replace(f"{{{hour}temp}}", day_info[i]['temperature']+"°C")
            forecast_data = forecast_data.replace(f"{{{hour}percentage}}", day_info[i]['forecast_class_probability']+"%")
            forecast_data = forecast_data.replace(f"{{{hour}condition}}", "report_templates/img/"+day_info[i]['forecast']+".svg")
            forecast_data = forecast_data.replace(f"{{{hour}rain}}", config['rain_class'][day_info[i]['rain_class']])
            forecast_data = forecast_data.replace("{Date_Stamp}",i.split(" ")[0])
            file_prefix = f"{client_data['lat']}_{client_data['lng']}_{i.split(' ')[0]}"

        forecast_data = forecast_data.replace("{location}",f"{client_data['lat']},{client_data['lng']}")
        forecast_data = forecast_data.replace("{timestamp}",datetime.now().strftime("%H:%M:%S %d-%m-%Y"))    

        html = HTML(string=forecast_data, base_url=base_url)
        
        first_page = f"report_templates/{file_prefix}_0-11.pdf"
        html.write_pdf(first_page, stylesheets=[CSS('report_templates/home.css')])
        file_names = list()
        file_names.append(first_page)
        forecast_data = copy.deepcopy(config['12-23_template'])

        for i in all_times:
            hour = i.split(" ")[1].split(":")[0]
            forecast_data = forecast_data.replace(f"{{{hour}temp}}", day_info[i]['temperature']+"°C")
            forecast_data = forecast_data.replace(f"{{{hour}percentage}}", day_info[i]['forecast_class_probability']+"%")
            forecast_data = forecast_data.replace(f"{{{hour}condition}}", "report_templates/img/"+day_info[i]['forecast']+".svg")
            forecast_data = forecast_data.replace(f"{{{hour}rain}}", config['rain_class'][day_info[i]['rain_class']])
            forecast_data = forecast_data.replace("{Date_Stamp}",i.split(" ")[0])
            file_prefix = f"{client_data['lat']}_{client_data['lng']}_{i.split(' ')[0]}"
        
        forecast_data = forecast_data.replace("{location}",f"{client_data['lat']},{client_data['lng']}")
        forecast_data = forecast_data.replace("{timestamp}",datetime.now().strftime("%H:%M:%S %d-%m-%Y"))    

        html = HTML(string=forecast_data, base_url=base_url)
        second_page = f"report_templates/{file_prefix}_12-23.pdf"
        html.write_pdf(second_page, stylesheets=[CSS('report_templates/home.css')])
        file_names.append(second_page)
        return file_names
    
    except Exception as e:
        logging.error(e)
        logging.error(e.__traceback__.tb_lineno)
        return f"Failed due to {e}"

def get_detailed_forecast(day,config,client_data):    
    
    all_times=list()
    if datetime.now().day == day.day:
        start_time = get_closest_half_hour(datetime.now())
        
        all_times = get_prediction_times(start_day=start_time,interval=30,days=None,time_zone="Asia/Kolkata")
    else:
        all_times = get_prediction_times(start_day=day,interval=30,days=None,time_zone="Asia/Kolkata")
    
    # data structure for the prediction   
     
    day_forecast = {"clouds":{},"temperature":{},"pressure":{},"humidity":{},"forecast":{},"rain_class_probability":{},'rain_class':{},"condition":{}}
    
    all_times_dataframe = pd.DataFrame(all_times,columns=["datetime"])      
    
    all_times_dataframe = pre_process_times(all_times,client_data)       
    
    # all_times_dataframe = all_times_dataframe[['lat','lon','dayofweek', 'quarter', 'month','dayofyear', 'dayofmonth', 'weekofyear','minutes','year','altitude']]
    
    
    #holy grail
    predicted_dataframe = predict_weather_batch_dashboard(all_times_dataframe.drop('datetime',axis=1),client_data,config)   
    #print(predicted_dataframe[['hour','minutes','year','dayofmonth','year','temp','pressure','humidity','clouds_all','rain_1h','0','1','2','3','4','5']])
    
    # df = predicted_dataframe[['temp','pressure','humidity','clouds_all','rain_1h','rain_class_probability','forecast','forecast_probabilities']]                      
    weather_forecast = post_process_predictions_dashboard(predicted_dataframe,all_times,config)
    # print(weather_forecast)

    
    for time in all_times:
        time_string = time.strftime(format="%Y-%m-%d %H:%M:%S")
        day_forecast['condition'][time_string] = None
        day_forecast['forecast'][time_string] = None
        day_forecast['temperature'][time_string] = None
        day_forecast['pressure'][time_string] = None
        day_forecast['humidity'][time_string] = None
        day_forecast['rain_class_probability'][time_string] = None
        day_forecast['rain_class'][time_string] = None
        day_forecast['clouds'][time_string] = None
        #print("clouds",weather_forecast[time_string]['clouds'])
        # weather_forecast = predict_weather(time,config,client_data)        
        day_forecast["condition"][time_string] = weather_forecast[time_string]['forecast']
        day_forecast['forecast'][time_string] = weather_forecast[time_string]['forecast_probabilities']

        day_forecast['temperature'][time_string] = str(weather_forecast[time_string]['temp'])
        day_forecast['pressure'][time_string] = str(weather_forecast[time_string]['pressure'])
        day_forecast['humidity'][time_string] = str(weather_forecast[time_string]['humidity'])
        day_forecast['clouds'][time_string] = str(int(weather_forecast[time_string]['clouds']))
        day_forecast['rain_class_probability'][time_string] = str(weather_forecast[time_string]['rain_class_probability'])
        day_forecast['rain_class'][time_string] = str(weather_forecast[time_string]['rain_class'])
    # del weather_forecast

    day_forecast["feels like"] = str(random.randrange(0,50))
    day_forecast["dew_point"] = str(random.randrange(0,50))
    # print(day)
    try:
        sun_data = get_extra_info(client_data['lat'],client_data['lng'],day)
        #logging.info(sun_data)
        day_forecast["Sunrise"] = sun_data["Sunrise"]
        day_forecast["Sunset"] = sun_data["Sunset"]
        day_forecast["Daylight"] = sun_data["Daylight"]
    except Exception:
        day_forecast["Sunrise"] = "NA"
        day_forecast["Sunset"] = "NA"
        day_forecast["Daylight"] = "NA"        
       
    return day_forecast



def get_detailed_forecast_api(all_times,config,client_data):    
    try:
        # all_times = get_prediction_times(start_day = day,interval=30,days=None,time_zone="Asia/Kolkata")           
        all_times_dataframe = pd.DataFrame(all_times,columns=["datetime"])   
        # print(all_times,"*****",all_times_dataframe)        
        

        all_times_dataframe = pre_process_times(all_times,client_data)       
        
        all_times_dataframe = all_times_dataframe[['lat','lon','dayofweek', 'quarter', 'month','dayofyear', 'dayofmonth', 'weekofyear','minutes','year','altitude']]
        # print(all_times_dataframe)
        #holy grail
        predicted_dataframe = predict_weather_batch(all_times_dataframe,client_data,config)   
        
        df = predicted_dataframe[['temp','pressure','humidity','clouds_all','rain_1h','rain_class_probability','forecast']]                      
        forecasted_dict = dict()
        forecasted_dict = post_process_predictions(df,all_times,config)
        # print("Forecasted dict",forecasted_dict)
    except Exception as e:
            print(str(e))
            print(str(e.__traceback__.tb_lineno))
            logging.error(str(e)+" "+str(e.__traceback__.tb_lineno))

    
    return forecasted_dict
    


def get_default_forecast(time, config, client_data):
    try:
        all_times = [time]
        time_dataframe = pre_process_times(all_times,client_data)   
        # time_dataframe = 
        weather_forecast = predict_weather_batch(time_dataframe[['lat','lon','dayofweek', 'quarter', 'month','dayofyear', 'dayofmonth', 'weekofyear','minutes','year','altitude']], client_data, config)                
        # weather_forecast['forecast'] = config["weather_condition"][str(weather_forecast['weather'][0])]
        
        # weather_forecast['temp'] = str(int(weather_forecast['temp']))
        # weather_forecast['pressure'] = str(int(weather_forecast['pressure']))
        # weather_forecast['humidity'] = str(int(weather_forecast['humidity']))
        # weather_forecast['rain_class_probability'] = str(int(weather_forecast['weather_probabilities'][0][weather_forecast['weather'][0]]*100))
        # weather_forecast['rain_class'] = str(int(weather_forecast['rain_class']))
        # weather_forecast.pop('weather',None)
        # weather_forecast.pop('weather_probabilities',None)
        #print("Before",weather_forecast['rain_class_probability'])
        weather_op = weather_forecast['weather_probability'].tolist()[0]
        final_weather_op = float(weather_op)
        weather_forecast['rain_class_probability'] = final_weather_op
        #print("After",weather_forecast['rain_class_probability'])
        #logging.info(weather_forecast['weather_probability'].tolist())
        #logging.info(type(weather_forecast['weather_probability']))
        forecasted_dict = post_process_predictions(weather_forecast[['temp','pressure','humidity','clouds_all','rain_1h','rain_class_probability','forecast']],all_times,config)      
        return list(forecasted_dict.values())[0]
    
    except Exception as e:        
        logging.error("default forecast "+str(e)+" "+str(e.__traceback__.tb_lineno))



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
            all_times = [curr_time]           
            all_times_dataframe = pre_process_times(all_times,client_data)
            all_times_dataframe = all_times_dataframe[['lat','lon','dayofweek', 'quarter', 'month','dayofyear', 'dayofmonth', 'weekofyear','minutes','year','altitude']]
            
            result = predict_weather_batch(all_times_dataframe,client_data,config)
            df = result[['temp','pressure','humidity','clouds_all','rain_1h','rain_class_probability','forecast']]                      
            
            forecasted_dict = post_process_predictions(df,all_times,config)      
            return {"status":"success","data":forecasted_dict}
        except Exception as e:
            return {"status":"fail","reason":str(e)}
    
    elif type == "detailed_new":
        all_days = get_prediction_times(start_day = datetime.now(),interval=None,days=client_data["days"],time_zone="Asia/Kolkata") 
        forecasted_weather = dict()                
        #print(all_days)         
        try:                       
            for days in all_days:
                forecasted_weather[days.strftime("%y-%m-%d")] = get_detailed_forecast_api(days,config,client_data)            
            return {"status":"success","data":forecasted_weather}
        except Exception as e:
            return {"status":"fail","reason":str(e)+str(e.__traceback__.tb_lineno)}               





    elif type == "detailed":
        # all_days = get_prediction_times(start_day = datetime.now(tz=pytz.timezone("Asia/Kolkata")),interval=None,days=client_data["days"],time_zone="Asia/Kolkata")                
        forecasted_weather = dict()                
        try:
            all_days = get_prediction_times(start_day = datetime.now(),interval=None,days=client_data["days"],time_zone="Asia/Kolkata")  
            all_times = list()           
            for day in all_days:
                if datetime.now().day == day.day:
                    start_time = round_to_hour(datetime.now())
                    all_times.extend(get_prediction_times(start_day=start_time,interval=240,days=None,time_zone="Asia/Kolkata"))
                else:
                    all_times.extend(get_prediction_times(start_day=day,interval=240,days=None,time_zone="Asia/Kolkata"))
                # all_times.extend(get_prediction_times(start_day = day,interval=60,days=None,time_zone="Asia/Kolkata"))
            forecasted_weather = dict()                
            # with ThreadPoolExecutor(max_workers=client_data["days"]) as e:            
            #     futures = {e.submit(get_detailed_forecast_api,day,config,client_data):day for day in all_days}
            #     for future in as_completed(futures):                    
            #         forecasted_weather[futures[future].strftime("%y-%m-%d")] = future.result()            
            # print("all times",all_times)
            forecasted_weather = get_detailed_forecast_api(all_times,config,client_data)
            return {"status":"success","data":forecasted_weather}
        except Exception as e:
            return {"status":"fail","reason":str(e)+" "+str(e.__traceback__.tb_lineno)}

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
        
            time_object = datetime(client_data['year'],client_data['month'],client_data['day'],hour=client_data['hour'],minute=client_data['minute'],second=client_data['second'])   
            time_object = pytz.timezone("Asia/Kolkata").localize(time_object)
        except Exception as e:            
            return {"status":"fail","reason":str(e)}      

        try:
            all_times = [time_object]
            #print(all_times)            
            all_times_dataframe = pre_process_times(all_times,client_data)
            all_times_dataframe = all_times_dataframe[['lat','lon','dayofweek', 'quarter', 'month','dayofyear', 'dayofmonth', 'weekofyear','minutes','year','altitude']]
            
            result = predict_weather_batch(all_times_dataframe,client_data,config)
            df = result[['temp','pressure','humidity','clouds_all','rain_1h','rain_class_probability','forecast']]                      
            
            forecasted_dict = post_process_predictions(df,all_times,config)
            
            return {"status":"pass","data":forecasted_dict[time_object.strftime("%y-%m-%d %H:%M:%S")]}
        except Exception as e:                    
            return {"status":"fail","reason":str(e)+str(e.__traceback__.tb_lineno)}   

def get_elevation(client_data):    
    url = f"http://elevation.frizzleweather.com/v1/gebco2020?locations={client_data['lat']},{client_data['lng']}"
    try:
        response = requests.get(url)        
        return float(response.json()['results'][0]['elevation'])
    except Exception as e:
        logging.error(str(e)+" Unable to fetch altitude")
        return 0

def get_aqi(pm25,pm10,o3):
    myaqi = aqi.to_aqi([
            (aqi.POLLUTANT_PM25, pm25),
            (aqi.POLLUTANT_PM10, pm10),
            (aqi.POLLUTANT_O3_8H, o3)
            ], algo=aqi.ALGO_INDIA)
    return myaqi

def get_air_quality(client_data,aqi_type):    
    if aqi_type=="current":
        url = f"http://pro.openweathermap.org/data/2.5/air_pollution?lat={float(client_data['lat'])}&lon={float(client_data['lng'])}&appid=10d37b1710d191f034b1eb440707f19a"
        try:
            response = requests.get(url)
            data = response.json()['list'][0]['components']
            aqi = get_aqi(data['pm2_5'],data['pm10'],data['o3'])            
            return {"status":"pass","data":{"aqi":int(aqi),"components":data}}
        except Exception as e:
            logging.error(str(e)+" Unable to fetch AQI")
            return {"status":"failed","reason":str(e)}
    elif aqi_type == "forecast":
        url = f"http://pro.openweathermap.org/data/2.5/air_pollution/forecast?lat={float(client_data['lat'])}&lon={float(client_data['lng'])}&appid=10d37b1710d191f034b1eb440707f19a"
        try:
            response = requests.get(url)
            timestamp = int(response.json()['list'][0]['dt'])            
            return {"status":"pass","data":{datetime.fromtimestamp(i['dt']).strftime('%y-%m-%d %H:%M:%S'):{'aqi':int(get_aqi(i['components']['pm2_5'],i['components']['pm10'],i['components']['o3'])),'components':i['components']} for i in response.json()['list']}}
        except Exception as e:
            logging.error(str(e)+" Unable to fetch AQI")
            return {"status":"failed","reason":str(e)}

def pre_process_times(all_times,client_data):
    all_times_dataframe = pd.DataFrame(all_times,columns=["datetime"])  
            
    all_times_dataframe['lat'] = [float(client_data['lat'])] * len(all_times)
    all_times_dataframe['lon'] = [float(client_data['lng'])] * len(all_times)
    all_times_dataframe['altitude'] = [float(client_data['alt'])] * len(all_times)        
    
    all_times_dataframe['hour'] = all_times_dataframe['datetime'].dt.hour
    # all_times_dataframe['hour'] = all_times_dataframe['hour'].apply(lambda x:int(x))

    all_times_dataframe['dayofweek'] = all_times_dataframe['datetime'].dt.dayofweek
    # all_times_dataframe['dayofweek'] = all_times_dataframe['datetime'].apply(lambda x:int(x))
    
    all_times_dataframe['quarter'] = all_times_dataframe['datetime'].dt.quarter
    # all_times_dataframe['quarter'] = all_times_dataframe['datetime'].apply(lambda x:int(x))
    
    all_times_dataframe['month'] = all_times_dataframe['datetime'].dt.month
    # all_times_dataframe['month'] = all_times_dataframe['datetime'].apply(lambda x:int(x))
    
    all_times_dataframe['dayofyear'] = all_times_dataframe['datetime'].dt.dayofyear
    # all_times_dataframe['dayofyear'] = all_times_dataframe['datetime'].apply(lambda x:int(x))
    
    all_times_dataframe['dayofmonth'] = all_times_dataframe['datetime'].dt.day
    # all_times_dataframe['dayofmonth'] = all_times_dataframe['datetime'].apply(lambda x:int(x))
    
    all_times_dataframe['weekofyear'] = all_times_dataframe['datetime'].dt.weekofyear
    # all_times_dataframe['weekofyear'] = all_times_dataframe['datetime'].apply(lambda x:int(x))
    
    all_times_dataframe['year'] = all_times_dataframe['datetime'].dt.year
    # all_times_dataframe['year'] = all_times_dataframe['datetime'].apply(lambda x:int(x))
    
    
    all_times_dataframe['minutes'] = all_times_dataframe['hour'].apply(lambda x:x*60)+all_times_dataframe['datetime'].dt.minute

    return all_times_dataframe

def post_process_predictions(df,all_times,config):
    forecasted_dict = {all_times[i].strftime("%y-%m-%d %H:%M:%S"):{
                "temp":str(int(df.loc[i,"temp"])),
            "pressure":str(round(df.loc[i,"pressure"])),
            "humidity":config["humidity_class"][str(df.loc[i,"humidity"])],
            "clouds":str(int(df.loc[i,"clouds_all"])),
            "rain_class":str(df.loc[i,"rain_1h"]),
            "rain_class_probability":str(int(df.loc[i,"rain_class_probability"]*100)),
            "forecast":config["weather_condition"][str(df.loc[i,"forecast"])]
            } for i in range(len(df))}
    
    return forecasted_dict


def post_process_predictions_dashboard(df,all_times,config):
    forecasted_dict = {all_times[i].strftime("%Y-%m-%d %H:%M:%S"):{
                "temp":str(int(df.loc[i,"temp"])),
            "pressure":str(round(df.loc[i,"pressure"])),
            "humidity":config["humidity_class"][str(df.loc[i,"humidity"])],
            "clouds":str(int(df.loc[i,"clouds_all"])),
            "rain_class":str(df.loc[i,"rain_1h"]),
            "rain_class_probability":str(int(df.loc[i,"rain_class_probability"]*100)),
            "forecast":config["weather_condition"][str(df.loc[i,"forecast"])],
            "forecast_probabilities": {
                config['weather_condition'][str(j)]:f"{df.loc[i,str(j)]:.4f}" for j in range(6)}
            } for i in range(len(df))}
    
    return forecasted_dict


def flood_risk(required_time,client_data,config):
    try:
        all_times = [required_time]
        
        data = pre_process_times(all_times,client_data)
        logging.info("preprocessed data")
        logging.info(data)
        #all_times_dataframe = all_times_dataframe[['lat','lon','dayofweek', 'quarter', 'month','dayofyear', 'dayofmonth', 'weekofyear','minutes','year','altitude']]   
	
	#data = predict_weather_batch(all_times_dataframe,client_data,config)
        
        data['temp'] = config['temp_model'].predict(data.drop('datetime', axis = 1))
        logging.info(data['temp'])
        data['pressure'] = config['press_model'].predict(data.drop('datetime', axis = 1))
        logging.info(data['pressure'])
        data['humidity'] = config['humid_class'].predict(data.drop('datetime', axis = 1))
        logging.info(data['humidity'])
        data['clouds_all'] = config['cloud_model'].predict(data.drop('datetime', axis = 1))
        logging.info(data['temp'])
        logging.info(data['pressure'])
        logging.info(data['humidity'])
        logging.info(data['clouds_all'])
	#post processing of weather attributes
        data['humidity'] = data['humidity'].replace([0,1,2,3,4,5,6,7],[7.5,22.5,35,45,55,65,75,90])

        data['clouds_all'] = data['clouds_all'].replace([0,1,2,3],[12.5,37.5,62.5,87.5])	

        data_daily_temp = data[['datetime','temp']]

        data_daily_temp = data_daily_temp.set_index('datetime')
        data_daily_temp_avg = data_daily_temp.resample('D').mean()

        data_daily_temp_max = data_daily_temp.resample('D').max()
        data_daily_temp_max.rename(columns = {'temp':'temp_max'}, inplace = True)

        data_daily_temp_min = data_daily_temp.resample('D').min()
        data_daily_temp_min.rename(columns = {'temp':'temp_min'}, inplace = True)

        data_daily_temp1 = data_daily_temp_avg.join([data_daily_temp_max,data_daily_temp_min])
        logging.info(data_daily_temp1)
        data_daily_press = data[['datetime','pressure']]

        data_daily_press = data_daily_press.set_index('datetime')
        data_daily_press_avg = data_daily_press.resample('D').mean()

        data_daily_press_max = data_daily_press.resample('D').max()
        data_daily_press_max.rename(columns = {'pressure':'pressure_max'}, inplace = True)

        data_daily_press_min = data_daily_press.resample('D').min()
        data_daily_press_min.rename(columns = {'pressure':'pressure_min'}, inplace = True)

        data_daily_press1 = data_daily_press_avg.join([data_daily_press_max,data_daily_press_min])

        data_daily_hum = data[['datetime','humidity']]

        data_daily_hum = data_daily_hum.set_index('datetime')
        data_daily_hum_avg = data_daily_hum.resample('D').mean()

        data_daily_hum_max = data_daily_hum.resample('D').max()
        data_daily_hum_max.rename(columns = {'humidity':'humidity_max'}, inplace = True)

        data_daily_hum_min = data_daily_hum.resample('D').min()
        data_daily_hum_min.rename(columns = {'humidity':'humidity_min'}, inplace = True)

        data_daily_hum1 = data_daily_hum_avg.join([data_daily_hum_max,data_daily_hum_min])
        logging.info(data_daily_hum1)
        data_daily_clouds = data[['datetime','clouds_all']]

        data_daily_clouds = data_daily_clouds.set_index('datetime')
        data_daily_clouds_avg = data_daily_clouds.resample('D').mean()

        data_daily_clouds_max = data_daily_clouds.resample('D').max()
        data_daily_clouds_max.rename(columns = {'clouds_all':'clouds_all_max'}, inplace = True)

        data_daily_clouds_min = data_daily_clouds.resample('D').min()
        data_daily_clouds_min.rename(columns = {'clouds_all':'clouds_all_min'}, inplace = True)

        data_daily_clouds1 = data_daily_clouds_avg.join([data_daily_clouds_max,data_daily_clouds_min])
        logging.info(data_daily_clouds1)
        data_all = data_daily_temp1.join([data_daily_press1,data_daily_hum1,data_daily_clouds1])

        data_all = data_all.reset_index()
        logging.info(data_all)
        data_all['altitude'] = [client_data['alt']]*data_all.shape[0]
        data_all['lat'] = [client_data['lat']]*data_all.shape[0]
        data_all['lon'] = [client_data['lng']]*data_all.shape[0]

        data_all['hour'] = data_all['datetime'].dt.hour
        data_all['dayofweek'] = data_all['datetime'].dt.dayofweek
        data_all['quarter'] = data_all['datetime'].dt.quarter
        data_all['month'] = data_all['datetime'].dt.month
        data_all['dayofyear'] = data_all['datetime'].dt.dayofyear
        data_all['dayofmonth'] = data_all['datetime'].dt.day
        data_all['weekofyear'] = data_all['datetime'].dt.weekofyear
        data_all['year'] = data_all['datetime'].dt.year
	# predicting rain
        data_all['rain_1h'] = config['custom_rain_model'].predict(data_all.drop('datetime', axis = 1))


        data_all = data_all.set_index('datetime')

        data_all = data_all.resample('D').sum()

        data_all = data_all[['dayofweek', 'quarter', 'month', 'dayofyear','dayofmonth', 'weekofyear', 'year', 'altitude','rain_1h']]
        data_all.rename(columns = {'rain_1h':'Rain'},inplace=True)
        
        pred = config['flood_risk'].predict(data_all)
        logging.info(int(pred[0]))
        return str(int(pred[0]))
    except Exception as e:
        logging.error(e)
        logging.error(e.__traceback__.tb_lineno)       
