import random
import boto3
import redis
import joblib
import base64
from flask import Flask, render_template, request,jsonify
from json import loads
from datetime import datetime
from WeatherModel import api_model_pipeline
from get_intervals_from_time import get_prediction_times
encoded_string = ""
# need to be config variables or loaded from config
redis_host = "frizzle-redis-cluster.zcgu4a.ng.0001.aps1.cache.amazonaws.com"
redis_port = 6379
redis_endpoint = redis_cluster_endpoint = redis.Redis(
host=redis_host,
port=redis_port,
db=0)
models = dict()
weather_condition={0:"Sunny",1:"Cloudy",2:"Rainy"}

app = Flask(__name__)
@app.route('/')
def hello_world():
    if request.method == "GET":
        return 'Hello, World!'
    else:
        return "Root method uses only GET, Please try again"

@app.route('/get_prediction',methods=["GET"])
def get_prediction():
    if request.method=="GET":
        try:
            client_data = loads(request.data)
            type = request.args.get('type')
            # print(client_data)
        except Exception as e:
            print(e)
        prediction_times = get_prediction_times()
        forecasted_weather = dict()
        if type=="default":
            model = api_model_pipeline.Model_Pipeline(None,models)
        else:
            return "Image not found"
        
        for time in prediction_times:
            try:                    
                forecasted_weather[time.strftime(format="%y-%m-%d %H:%M:%S")] = weather_condition[model.forecast_weath(time)[0]]
            except Exception as e:
                print(e)

        # for i in forecasted_weather:
        #      print(i,forecasted_weather[i])
        # print(forecasted_weather)
        return jsonify(forecasted_weather)

def load_models():
    models['temperature'] = joblib.load("models/temperature_model.joblib.dat")
    models['pressure']= joblib.load("models/pressure_model.joblib.dat")
    models['humidity'] = joblib.load("models/humidity_model.joblib.dat")
    #self.wind_speed_model = joblib.load("models/wind_speed_model.joblib.dat")
    models['weather'] = joblib.load("models/weath_model.joblib.dat")
     
if __name__ == "__main__":
    load_models()
    app.run(debug=True)