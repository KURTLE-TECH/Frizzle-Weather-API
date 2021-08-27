from collections import defaultdict
import random
import threading
import boto3
import redis
import joblib
import base64
from flask import Flask, request, jsonify
from json import loads
from datetime import datetime
#from WeatherModel import api_model_pipeline
from Endpoint_Object import Endpoint_Object
from get_data import get_closest_half_hour, get_default_forecast, get_prediction_times, get_closest_node, get_log,get_detailed_forecast,get_data_from_redis
from database import DynamodbHandler
import logging
import pytz
from concurrent.futures import ThreadPoolExecutor,as_completed
from math import ceil
# from models.humid_script import humid_model

# external configuration; needs to be loaded from a json file
with open("config.json","r") as f:
    config = loads(f.read())

    #for the upcoming himidity models

    # humidity_model = humid_model()
    # humidity_model.load_model(config["models"]["humidity-model-value"])
    # config["humidity-model-object"] = humidity_model

redis_endpoint = redis_cluster_endpoint = redis.Redis(host=config["redis_host"],port=config["redis_port"],db=0)
models = dict()
weather_condition = config["weather_condition"]
# Machine learning models endpoint object

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
        # print("Start day calculation",datetime.now())
        all_days = get_prediction_times(start_day = datetime.now(),interval=None,days=6,time_zone="Asia/Kolkata")
        # print("End day calculation",datetime.now())
        #get the times for each day        
        forecasted_weather = dict()                
        with ThreadPoolExecutor(max_workers=7) as e:            
            futures = {e.submit(get_detailed_forecast,day,config,client_data):day for day in all_days}
            for future in as_completed(futures):
                # print("Future value",futures[future])
                forecasted_weather[futures[future].strftime("%y-%m-%d")] = future.result()

            
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
        prediction_times = get_prediction_times(start_day = get_closest_half_hour(datetime.now(tz=pytz.timezone("Asia/Kolkata"))),interval=30,days=None,time_zone="Asia/Kolkata")
        # model_object = Endpoint_Object.Endpoint_Calls(config['region'],config['access_key'],config['secret_access_key'],config['models'])
        # for time in prediction_times:
            # forecasted_weather[time.strftime("%Y-%m-%d %H:%M:%S")] = {}
        try:
            with ThreadPoolExecutor(max_workers=2) as e:            
                futures = {e.submit(get_default_forecast,time,config,client_data):time for time in prediction_times}
                for future in as_completed(futures):
                    # print("Future value",futures[future])
                    forecasted_weather[futures[future].strftime("%Y-%m-%d %H:%M:%S")] = future.result()                        
        except Exception as e:                
            app.logger.error(get_log(logging.ERROR,request,str(e)))
            return jsonify({"Status": "Failed", "Reason": str(e)})
        app.logger.info(get_log(logging.INFO,request,None))
        return jsonify(forecasted_weather)

@app.route("/api/get_live_data",methods=["GET"])
def get_live_data():
    try:
        client_data = loads(request.data)
        node_id = client_data["Device ID"]
    except Exception as e:
        app.logger.error(get_log(logging.ERROR,request,str(e)+" Unable to load client data"))
        return {}
    try:
        data = get_data_from_redis(redis_cluster_endpoint,node_id)
    except Exception as e:
        app.logger.error(get_log(logging.ERROR,request,str(e)+" Unable to fetch node data from redis as no connection"))
        return {}        
    app.logger.info(get_log(logging.info,request,None))    
    return data
    

#load_models()
if __name__ == "__main__":
    app.run(debug=True,port=5000)
