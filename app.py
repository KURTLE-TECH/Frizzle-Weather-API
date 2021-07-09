from collections import defaultdict
import random
import boto3
import redis
import joblib
import base64
from flask import Flask, request, jsonify
from json import loads
from datetime import datetime
#from WeatherModel import api_model_pipeline
from Endpoint_Object import Endpoint_Object
from get_data import get_prediction_times, get_closest_node, get_log
from database import DynamodbHandler
import logging
import pytz

# external configuration; needs to be loaded from a json file
with open("config.json","r") as f:
    config = loads(f.read())

redis_endpoint = redis_cluster_endpoint = redis.Redis(host=config["redis_host"],port=config["redis_port"],db=0)
models = dict()
weather_condition = config["weather_condition"]
# Machine learning models endpoint object
model_object = Endpoint_Object.Endpoint_Calls(config['region'],config['access_key'],config['secret_access_key'],config['models'])
# app initialisation
app = Flask(__name__)
logging.basicConfig(filename='api_server.log', filemode="w", level=logging.INFO,format=config['log_format'])
app.logger.setLevel(logging.INFO)


@app.route('/api/status',methods=["GET","POST"])
def hello_world():
    
    if request.method == "GET":
        app.logger.info(get_log(logging.INFO,request,None))
        return 'Hello, World!'
    else:
        app.logger.error(get_log(logging.ERROR,request,"Wrong method"))
        return "Root method uses only GET, Please try again"


@app.route('/api/get_prediction', methods=["GET"])
def get_prediction():
    curr_time = datetime.now().strftime(format="%y-%m-%d %H:%M:%S:%f")
    try:
        client_data = loads(request.data)
        request_type = request.args.get('type')
        location = dict()
        location['lat'] = client_data['lat']
        location['lng'] = client_data['lng']
        # print(client_data)
    except Exception as e:
        app.logger.error(get_log(logging.ERROR,request,e.__str__))
        return jsonify({"Status": "Failed", "Reason": str(e)})
    

    forecasted_weather = dict()
    if request_type == "detailed":        
                
        # get the next 7 days
        all_days = get_prediction_times(start_day = datetime.now(),interval=None,days=7,time_zone="Asia/Kolkata")

        #get the times for each day
        all_times = {}
        forecasted_weather = dict()
        for day in all_days:
            if datetime.now(tz=pytz.timezone("Asia/Kolkata")).day == day.day:
                all_times[day] = get_prediction_times(start_day=datetime.now(tz=pytz.timezone("Asia/Kolkata")),interval=30,days=None,time_zone="Asia/Kolkata")
            else:
                all_times[day] = get_prediction_times(start_day=day,interval=30,days=None,time_zone="Asia/Kolkata")
        
            # data structure for the prediction
            day_string = day.strftime("%Y-%m-%d")
            forecasted_weather[day_string] = {"temperature":{},"pressure":{},"humidity":{},"rain":{},"forecast":{}}                       
            for time in all_times[day]:
                time_string = time.strftime(format="%Y-%m-%d %H:%M:%S")
                

                #working models
                forecasted_weather[day_string]["temperature"][time_string] = model_object.temp_model(client_data['lat'],client_data['lng'],time_string)
                forecasted_weather[day_string]['pressure'][time_string] = str(round(float(model_object.press_model()),1))                                
                forecasted_weather[day_string]['humidity'][time_string] = model_object.humid_model().split(",")[0]

                #models to add. currently dummy models                
                forecasted_weather[time_string]['rain_probability'][time_string] = model_object.humid_model().split(",")[0]
                forecasted_weather[day_string]["forecast"][time_string] = {"sunny":str(random()),"cloudy":str(random()),"rainy":str(random())}                
                # _cloud_percentage = model_object.cloud_model().split(",")[0]
                # forecasted_weather[day_string]['rain'][time_string] = model_object.rain_model()
                # forecasted_weather[day_string]['forecast'][time_string] = config["weather_condition"][str(random.randint(0,2))]
                
            
        app.logger.info(get_log(logging.INFO,request,None))
        return jsonify(forecasted_weather)
                

    elif request_type == "default":
        forecasted_weather = defaultdict()
        # get the closest node from the user
        # try:
        #     closest_node = get_closest_node(location)        
        #     if closest_node is not None:            
        #         image = loads(redis_endpoint[closest_node])['picture']
                
        # except Exception as e:                
        #     #return jsonify({"status":"failed","reason":str(e)})
        #     pass                            
        prediction_times = get_prediction_times(start_day = datetime.now(tz=pytz.timezone("Asia/Kolkata")),interval=30,days=None,time_zone="Asia/Kolkata")
        
        for time in prediction_times:
            time_string = time.strftime(format="%Y-%m-%d %H:%M:%S")
            forecasted_weather[time_string] = defaultdict()
            try:
                time_string = time.strftime(format="%Y-%m-%d %H:%M:%S")
                forecasted_weather[time_string]['temp'] = str(round(float(model_object.temp_model(client_data['lat'],client_data['lng'],time_string)),1))
                forecasted_weather[time_string]['pressure'] = str(round(float(model_object.press_model()),1)) 
                #rain is humidity here                               
                forecasted_weather[time_string]['rain_probability'] = model_object.humid_model().split(",")[0]
                forecasted_weather[time_string]['forecast'] = config["weather_condition"][str(random.randint(0,2))]
                
            except Exception as e:
                print(e)
                app.logger.error(get_log(logging.ERROR,request,str(e)))
                return jsonify({"Status": "Failed", "Reason": str(e)})
        app.logger.info(get_log(logging.INFO,request,None))
        return jsonify(forecasted_weather)




#load_models()
if __name__ == "__main__":
    app.run(debug=True,port=5000)
