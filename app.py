import random
import boto3
import redis
import joblib
import base64
from flask import Flask, render_template, request,jsonify
from json import loads
from datetime import datetime
from WeatherModel import api_model_pipeline
from get_data import get_prediction_times,get_closest_node
from database import DynamodbHandler 
import logging

# external configuration; needs to be loaded from a json file
redis_host = "frizzle-redis-cluster.zcgu4a.ng.0001.aps1.cache.amazonaws.com"
redis_port = 6379
redis_endpoint = redis_cluster_endpoint = redis.Redis(
host=redis_host,
port=redis_port,
db=0)
models = dict()
weather_condition={0:"Sunny",1:"Cloudy",2:"Rainy"}

#app initialisation
app = Flask(__name__)
logging.basicConfig(filename='server.log',filemode="w",level=logging.DEBUG)
formatter = logging.Formatter("Level:%(levelname)s %(name)s : %(message)s")
handler = logging.FileHandler("requests.log",mode="w")        
handler.setFormatter(formatter)
app_logger = logging.getLogger("requests")
app_logger.setLevel(logging.INFO)
app_logger.addHandler(handler)

@app.route('/',methods=["GET","POST"])
def hello_world():
    curr_time = datetime.now().strftime(format="%y-%m-%d %H:%M:%S:%f")
    if request.method == "GET":
        app_logger.info("Address: %s - Request Path %s - Time %s",request.remote_addr,request.path,curr_time)
        return 'Hello, World!'
    else:
        app_logger.error("Address: %s - Request Path %s - Time %s - Reason: Wrong method",request.remote_addr,request.path,curr_time)
        return "Root method uses only GET, Please try again"

@app.route('/get_prediction',methods=["GET"])
def get_prediction():    
        curr_time = datetime.now().strftime(format="%y-%m-%d %H:%M:%S:%f")
        try:
            client_data = loads(request.data)
            request_type = request.args.get('type')
            # print(client_data)
        except Exception as e:
            app_logger.error("Address: %s - Request Path %s - Time %s - Reason: %s",request.remote_addr,request.path,curr_time,e.__str__)
            return jsonify({"Status":"Failed","Reason":str(e)})
        location = dict()
        location['lat'] = client_data['lat']
        location['lng'] = client_data['lng']
        
        forecasted_weather = dict()
        if request_type=="detailed":
            closest_node =  get_closest_node(location)
            # print("closest_node: ",closest_node)
            if closest_node is not None:
                try:                
                    image = loads(redis_endpoint[closest_node])['picture']            
                    model = api_model_pipeline.Model_Pipeline(image,models)
                except Exception:                    
                    # pure testing only
                    image = loads(redis_endpoint["7a317703-f266-4329-8bf8-7aedbcab92d8"])['picture']            
                    model = api_model_pipeline.Model_Pipeline(None,models) 
            else:
                model = api_model_pipeline.Model_Pipeline(None,models)
        elif request_type=="default":
            model = api_model_pipeline.Model_Pipeline(None,models)
        prediction_times = get_prediction_times()
        for time in prediction_times:
            try:                    
                forecasted_weather[time.strftime(format="%y-%m-%d %H:%M:%S")] = weather_condition[model.forecast_weath(time)[0]]
            except Exception as e:
                app_logger.error("Address: %s - Request Path %s - Time %s - Reason: %s",request.remote_addr,request.path,curr_time,str(e))                
                return jsonify({"Status":"Failed","Reason":str(e)})        
        app_logger.info("Address: %s - Request Path %s - Type: %s - Time %s",request.remote_addr,request.path,request_type,curr_time)
        return jsonify(forecasted_weather)

def load_models():
    models['temperature'] = joblib.load("models/temperature_model.joblib.dat")
    models['pressure']= joblib.load("models/pressure_model.joblib.dat")
    models['humidity'] = joblib.load("models/humidity_model.joblib.dat")
    #self.wind_speed_model = joblib.load("models/wind_speed_model.joblib.dat")
    models['weather'] = joblib.load("models/weath_model.joblib.dat")
#remove app.wsgi_app

 
load_models()
if __name__ == "__main__":     
    app.run(debug=True)
    