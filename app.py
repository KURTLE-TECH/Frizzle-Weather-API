import random
import boto3
import redis
import joblib
import base64
from flask import Flask, request, jsonify
from json import loads
from datetime import datetime
from WeatherModel import api_model_pipeline
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
        model = api_model_pipeline.Model_Pipeline(None, models)
        
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
            day_string = day.strftime("%y-%m-%d")
            forecasted_weather[day_string] = {"temperature":{},"pressure":{},"humidity":{},"forecast":{}}            
                
            for time in all_times[day]:
                time_string = time.strftime(format="%y-%m-%d %H:%M:%S")
                forecasted_weather[day_string]["forecast"][time_string] = weather_condition[str(model.forecast_weath(time)[0])]
                forecasted_weather[day_string]["temperature"][time_string] = str(model.temperature[0])
                forecasted_weather[day_string]["humidity"][time_string] = str(model.humidity[0])
                forecasted_weather[day_string]["pressure"][time_string] = str(model.pressure[0])
        app.logger.info(get_log(logging.INFO,request,None))
        return jsonify(forecasted_weather)
                

    elif request_type == "default":
        
        # get the closest node from the user
        try:
            closest_node = get_closest_node(location)        
            if closest_node is not None:            
                image = loads(redis_endpoint[closest_node])['picture']
                model = api_model_pipeline.Model_Pipeline(image, models)
        except Exception:                
            model = api_model_pipeline.Model_Pipeline(None, models)
        else:
            model = api_model_pipeline.Model_Pipeline(None, models)        
            
        prediction_times = get_prediction_times(start_day = datetime.now(tz=pytz.timezone("Asia/Kolkata")),interval=30,days=None,time_zone="Asia/Kolkata")

        for time in prediction_times:
            try:
                forecasted_weather[time.strftime(format="%y-%m-%d %H:%M:%S")] = weather_condition[str(model.forecast_weath(time)[0])]
            except Exception as e:
                app.logger.error(get_log(logging.ERROR,request,str(e)))
                return jsonify({"Status": "Failed", "Reason": str(e)})
        app.logger.info(get_log(logging.INFO,request,None))
        return jsonify(forecasted_weather)


def load_models():
    models['temperature'] = joblib.load("models/temperature_model.joblib.dat")
    models['pressure'] = joblib.load("models/pressure_model.joblib.dat")
    models['humidity'] = joblib.load("models/humidity_model.joblib.dat")
    #self.wind_speed_model = joblib.load("models/wind_speed_model.joblib.dat")
    models['weather'] = joblib.load("models/weath_model.joblib.dat")


load_models()
if __name__ == "__main__":
    app.run(debug=True)
