from collections import defaultdict
import json
import random
import threading
from typing import MappingView
import boto3
import redis
import database
import joblib
import base64
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS, cross_origin
import pdfkit
from PyPDF2 import PdfFileMerger
from json import loads
from datetime import datetime, time, tzinfo
from get_data import get_closest_half_hour, get_data_from_timestream, get_default_forecast, get_prediction_times, get_closest_node, get_log,get_detailed_forecast,get_data_from_redis
from database import DynamodbHandler
#from CloudPercentage import Cloud_Percentage
import logging
import pytz
from concurrent.futures import ThreadPoolExecutor,as_completed
from math import ceil
from frizzle_models.humid_script import humid_model
import h2o
import os
from production_script.weather_forecast import Forecast


with open("config.json","r") as f:
    config = loads(f.read())    
    
    config['frizzle-humidity-wrapper'] = humid_model()
    config['temp_model'] = joblib.load('production_script/models/temp.sav')
    config['press_model'] = joblib.load('production_script/models/press.sav')
    config['humid_model'] = joblib.load('production_script/models/humid.sav')
    config['humid_class'] = joblib.load('production_script/models/humidity_15_09_class.sav')
    config['cloud_model'] = joblib.load('production_script/models/clouds.sav')
    config['rain_model'] = joblib.load('production_script/models/rain.sav')
    config['weather_model'] = joblib.load('production_script/models/weath.sav')        

    

redis_endpoint = redis_cluster_endpoint = redis.Redis(host=config["redis_host"],port=config["redis_port"],db=0)
models = dict()
weather_condition = config["weather_condition"]
database_handler = DynamodbHandler.DynamodbHandler()
time_stream_client = boto3.client('timestream-query',region_name='us-east-1')
# app initialisation
app = Flask(__name__)
logging.basicConfig(filename='api_server.log', filemode="a", level=logging.INFO,format=config['log_format'])
app.logger.setLevel(logging.INFO)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'



@app.route('/api/status',methods=["GET","POST"])
@cross_origin()
def hello_world():
    
    if request.method == "GET":
        app.logger.info(get_log(logging.INFO,request,None))
        return 'Hello, World!'
    else:
        app.logger.error(get_log(logging.ERROR,request,"Wrong method"))
        return "Root method uses only GET, Please try again!"

@app.route('/api/generate_report', methods=["GET","POST"])
@cross_origin()
def gen_report():
    try:
        client_data = loads(request.data)
        location = dict()
        client_data['lat'] = float(client_data['lat'])
        client_data['lng'] = float(client_data['lng'])
        if "email" in client_data.keys():
            try:
                user_info = database_handler.query(config['user_table'],"email",client_data["email"])        
                response = database_handler.query("Tiers","tier",user_info["Response"]['tier'])
                client_data['days'] = int(response['Response']['days'])
            except Exception:
                client_data['days']=2

        else:
            client_data['days']=2
        
        all_days = get_prediction_times(start_day=datetime.now(), interval=None, days=client_data['days'], time_zone="Asia/Kolkata")
        forecasted_weather = dict()
        with ThreadPoolExecutor(max_workers=7) as e:
            futures = {e.submit(get_detailed_forecast, day, config, client_data):day for day in all_days}
            for future in as_completed(futures):
                forecasted_weather[futures[future].strftime("%y-%m-%d")] = future.result()
        
        def format_time(time):
            time_part = time.split(" ")[1]
            hh, mm, ss = time_part.split(":")
            return ":".join([hh, mm])            

        def gen_html_stats_data(time1, temp1, pressure1, humidity1, rain1, time2, temp2, pressure2, humidity2, rain2):
            return f"""
                <tr>
                    <td class="time">{time1}</td>
                    <td class="forecast">{temp1}&deg;C</td>
                    <td class="forecast">{pressure1}</td>
                    <td class="forecast">{humidity1}%</td>
                    <td class="forecast">{rain1}%</td>
                    <td class="time">{time2}</td>
                    <td class="forecast">{temp2}&deg;C</td>
                    <td class="forecast">{pressure2}</td>
                    <td class="forecast">{humidity2}%</td>
                    <td class="forecast">{rain2}%</td>
                </tr>
            """
        def gen_html_forecast_data(time1, forecast1, time2, forecast2):
            return f"""
                <tr>
                    <td class="time">{time1}</td>
                    <td class="forecast">{forecast1}</td>
                    <td class="time">{time2}</td>
                    <td class="forecast">{forecast2}</td>
                </tr>
            """
        stats_template = ""
        forecast_template = ""
        cover_template = ""
        mid_template = ""
        with open("report_templates/stats.html") as stats_html:
            stats_template = stats_html.read()
        
        with open("report_templates/forecast.html") as forecast_html:
            forecast_template = forecast_html.read()

        with open("report_templates/cover.html") as cover_html:
            cover_template = cover_html.read()

        with open("report_templates/mid.html") as mid_html:
            mid_template = mid_html.read()

        options = {
            'page-size': 'Letter',
            'encoding': "UTF-8",
            'no-outline': None,
            'orientation': 'Landscape'
        }

        pdfFiles = []

        dates = list(forecasted_weather.keys())
        dates.sort()

        if client_data["address"]:
            current_cover = cover_template.replace("{{location}}", client_data["address"])
        else:
            current_cover = cover_template.replace("{{location}}", f"({client_data['lat']}, {client_data['lng']})")

        current_cover = current_cover.replace("{{period}}", f"{dates[0]} - {dates[-1]}")
        pdfkit.from_string(current_cover, 'report_templates/cover.pdf', options = options)
        pdfFiles.append('report_templates/cover.pdf')

        pdfkit.from_string(mid_template, 'report_templates/mid.pdf', options = options)
        pdfFiles.append('report_templates/mid.pdf')

        page = 0
        for date in dates:
            times = []
            temp_data = []
            rain_data = []
            pressure_data = []
            humidity_data = []
            condition_data = []
            for time in forecasted_weather[date]['temperature']:
                times.append(format_time(time))
                temp_data.append(forecasted_weather[date]['temperature'][time])
                rain_data.append(forecasted_weather[date]['rain_probability'][time])
                pressure_data.append(forecasted_weather[date]['pressure'][time])
                humidity_data.append(forecasted_weather[date]['humidity'][time])
                condition_data.append(forecasted_weather[date]['condition'][time])

            num_rows = ceil(len(times) / 2)
            stats_data = ""
            forecast_data = ""
            for i in range(num_rows):
                if i + num_rows < len(times):
                    stats_data += gen_html_stats_data(times[i], temp_data[i], pressure_data[i], humidity_data[i], rain_data[i],
                    times[i + num_rows], temp_data[i + num_rows], pressure_data[i + num_rows], humidity_data[i + num_rows], rain_data[i + num_rows])

                    forecast_data += gen_html_forecast_data(times[i], condition_data[i], times[i + num_rows], condition_data[i + num_rows])
                else:
                    stats_data += gen_html_stats_data(times[i], temp_data[i], pressure_data[i], humidity_data[i], rain_data[i], "-", "-", "-", "-", "-")
                    forecast_data += gen_html_forecast_data(times[i], condition_data[i], "-", "-")
            
            current_stats = stats_template.replace("{{data}}", stats_data)
            current_stats = current_stats.replace("{{date}}", date)            
            pdfkit.from_string(current_stats, f'report_templates/stats{page}.pdf', options = options)
            pdfFiles.append(f'report_templates/stats{page}.pdf')

            current_forecast = forecast_template.replace("{{data}}", forecast_data)
            pdfkit.from_string(current_forecast, f'report_templates/forecast{page}.pdf', options = options)
            pdfFiles.append(f'report_templates/forecast{page}.pdf')

            
            page += 1
            

        merger = PdfFileMerger()

        for pdf in pdfFiles:
            merger.append(pdf)

        merger.write("report_templates/report.pdf")
        merger.close()
        request_info = {"time-stamp":datetime.now().strftime(format="%Y-%m-%d %H:%M:%S"),"username":client_data['username'],"lat":client_data['lat'],'lng':client_data['lng'],"type":"report_generation"}
        try:
            _status = database_handler.insert(request_info,config["request_info_table"])        
        except Exception as e:            
            app.logger.error(get_log(logging.INFO,request,f"Unable to generate report,{e},{e.__traceback__.tb_lineno}"))   

        return send_file("report_templates/report.pdf", mimetype='application/pdf')


    except Exception as e:
        app.logger.error(get_log(logging.ERROR, request, e))
        return jsonify({"Status": "Failed", "Reason": str(e)})

@app.route('/api/get_prediction', methods=["GET","POST"])
@cross_origin()
def get_prediction():
    curr_time = datetime.now().strftime(format="%y-%m-%d %H:%M:%S:%f")
    try:
        client_data = loads(request.data)
        request_type = request.args.get('type')
        location = dict()
        client_data['lat'] = float(client_data['lat'])
        client_data['lng'] = float(client_data['lng'])   
        if 'username' not in client_data.keys():
            client_data['username'] = "unknown"
        # print(client_data)
         
    except Exception as e:
        app.logger.error(get_log(logging.ERROR,request,e.__str__))
        return jsonify({"Status": "Failed", "Reason": str(e)})

    if "email" in client_data.keys():
        try:
            user_info = database_handler.query(config['user_table'],"email",client_data["email"])        
            response = database_handler.query("Tiers","tier",user_info["Response"]['tier'])
            client_data['days'] = int(response['Response']['days'])
        except Exception:
            client_data['days']=2

    else:
        client_data['days']=2
    
    forecasted_weather = dict()
    if request_type == "detailed":        
                
        # get the next client_data['days'] days
        # print("Start day calculation",datetime.now())
        all_days = get_prediction_times(start_day = datetime.now(),interval=None,days=client_data["days"],time_zone="Asia/Kolkata")
        # print("End day calculation",datetime.now())
        #get the times for each day        
        forecasted_weather = dict()                
        with ThreadPoolExecutor(max_workers=client_data["days"]) as e:            
            futures = {e.submit(get_detailed_forecast,day,config,client_data):day for day in all_days}
            for future in as_completed(futures):
                # print("Future value",futures[future])
                forecasted_weather[futures[future].strftime("%y-%m-%d")] = future.result()
        # for day in all_days:
        #     forecasted_weather[day.strftime("%y-%m-%d")] = get_detailed_forecast(day,config,client_data)

            
        app.logger.info(get_log(logging.INFO,request,None))
        request_info = {"time-stamp":datetime.now().strftime(format="%Y-%m-%d %H:%M:%S"),"username":client_data['username'],"lat":f"{client_data['lat']}",'lng':f"{client_data['lng']}","type":"detailed"}
        _status = database_handler.insert(request_info,config["request_info_table"])
        return jsonify(forecasted_weather)
    


    elif request_type == "default":
        forecasted_weather = defaultdict()
          
        prediction_times = get_prediction_times(start_day = get_closest_half_hour(datetime.now(tz=pytz.timezone("Asia/Kolkata"))),interval=30,days=None,time_zone="Asia/Kolkata")
        
        try:
            with ThreadPoolExecutor(max_workers=2) as e:            
                futures = {e.submit(get_default_forecast,time,config,client_data):time for time in prediction_times}
                for future in as_completed(futures):
                    # print("Future value",futures[future])
                    forecasted_weather[futures[future].strftime("%Y-%m-%d %H:%M:%S")] = future.result()                        
        except Exception as e:
            app.logger.error("Line number "+e.__traceback__.tb_lineno)                
            # print(e.__traceback__.)                
            app.logger.error(get_log(logging.ERROR,request,str(e)))
            return jsonify({"Status": "Failed", "Reason": str(e)})
        app.logger.info(get_log(logging.INFO,request,None))
        request_info = {"time-stamp":datetime.now().strftime(format="%Y-%m-%d %H:%M:%S"),"username":client_data['username'],"lat":f"{client_data['lat']}",'lng':f"{client_data['lng']}","type":"default"}
        _status = database_handler.insert(request_info,config["request_info_table"])
        
        return jsonify(forecasted_weather)


@app.route("/api/live_prediction",methods=["POST"])
def live_prediction():
    try:
        client_data = loads(request.data)        
        client_data['lat'] = float(client_data['lat'])
        client_data['lng'] = float(client_data['lng'])  
        if 'username' not in client_data.keys():
        	client_data['username']="unknown"      
    
    except Exception as e:
        app.logger.error(get_log(logging.ERROR,request,e.__str__))
        return jsonify({"Status": "Failed", "Reason": str(e),"Line":f"{e.__traceback__.tb_lineno}"})


    try:                   
        curr_time = datetime.now(tz=pytz.timezone("Asia/Kolkata"))
        forecast_today = get_default_forecast(curr_time,config,client_data)       
    

    except Exception as e:
        app.logger.error(get_log(logging.ERROR,request,str(e)))
        return jsonify({"Status": "Failed", "Reason": str(e),"Line":f"{e.__traceback__.tb_lineno}"})

    try:
        nodes = get_closest_node({"lat":client_data['lat'],"lng":client_data['lng']})        
        ordered_nodes = sorted(nodes.items(),key=lambda x:float(x[0])) 
        live_data_node = get_data_from_redis(redis_cluster_endpoint,ordered_nodes[0][1])       
        request_info = {"time-stamp":datetime.now().strftime(format="%Y-%m-%d %H:%M:%S"),"username":client_data['username'],"lat":str(client_data['lat']),'lng':str(client_data['lng']),"type":"live_prediction"}
        try:
            _status = database_handler.insert(request_info,config["request_info_table"])        
        except Exception as e:            
            app.logger.error(get_log(logging.INFO,request,f"{e},{e.__traceback__.tb_lineno}"))   

        # checking if the closest node is 2km away and the data retrieved is present
        if int(ordered_nodes[0][0])<2 and "Status" not in live_data_node.keys():
            app.logger.info(get_log(logging.INFO,request,"Live prediction from node"))
            if float(live_data_node["Rain"]) in range(0,150):
                    return jsonify({"condition":forecast_today["forecast"],"temperature":live_data_node["Temperature"]})  
            
            elif float(live_data_node["Rain"]) in range(150,600):
                app.logger.info(get_log(logging.INFO,request,None))
                return jsonify({"condition":"drizzle","temperature":forecast_today["temp"]})                
            elif float(live_data_node["Rain"]) in range(600,1025):
                app.logger.info(get_log(logging.INFO,request,None))
                return jsonify({"condition":"rain","temperature":forecast_today["temp"]})                
            else:
                app.logger.info(get_log(logging.INFO,request,None))
                return jsonify({"condition":forecast_today["forecast"],"temperature":forecast_today["temp"]})            

        else:            
            app.logger.info(get_log(logging.INFO,request,"Live prediction from model"))
            return jsonify({"condition":forecast_today["forecast"],"temperature":forecast_today["temp"]})            
        
    except Exception as e:
        app.logger.error(get_log(logging.ERROR,request,f"{e.__str__}, Line no:{e.__traceback__.tb_lineno}"))
        return jsonify({"Status": "Failed", "Reason": str(e),"Line":f"{e.__traceback__.tb_lineno}"})
        


@app.route("/api/get_live_data",methods=["GET","POST"])
@cross_origin()
def get_live_data():
    try:
        client_data = loads(request.data)
        node_id = client_data["Device ID"]
    except Exception as e:
        app.logger.error(get_log(logging.ERROR,request,str(e)+" Unable to load client data"))
        return {}
    try:
        data = get_data_from_timestream(node_id,time_stream_client)    
                
    except Exception as e:
        app.logger.error(get_log(logging.ERROR,request,str(e)+" Unable to fetch node data from redis as no connection"))
        return {"Status":"failed","reason":str(e)}             
    app.logger.info(get_log(logging.info,request,None))    
    return jsonify(data)
    
@app.route("/api/closest_node",methods=["GET","POST"])
@cross_origin()
def closest_node():
    try:
        client_data = loads(request.data)
        client_data['lat'] = float(client_data['lat'])
        client_data['lng'] = float(client_data['lng'])
        
    except Exception as e:
        app.logger.error(get_log(logging.ERROR,request,str(e)+" Unable to load client data"))
        return {}
    try:
        data = get_closest_node(client_data)
    except Exception as e:
        app.logger.error(get_log(logging.ERROR,request,str(e)+" Unable to fetch node data from redis as no connection"))
        return {"Status":"failed","reason":str(e)}        
    app.logger.info(get_log(logging.info,request,None))    
    return {"Node ID":data}
    

@app.route("/api/predict",methods=["POST"])
@cross_origin()
def predict_forecast():
    try:
        client_data = loads(request.data)
        client_data['year'] = int(client_data['year'])
        client_data['month'] = int(client_data['month'])
        client_data['day'] = int(client_data['day'])
        client_data['hour'] = int(client_data['hour'])
        client_data['minute'] = int(client_data['minute'])
        client_data['second'] = int(client_data['second'])  
        client_data['lat'] = float(client_data['lat'])
        client_data['lng'] = float(client_data['lng'])   
        if 'username' not in client_data.keys():
            client_data['username'] = "unknown"
        if 'email' not in client_data.keys():
            client_data['email'] = "unknown"
        
    except Exception as e:
        app.logger.error(get_log(logging.ERROR,request,str(e)+"Unable to load client data"+str(e.__traceback__.tb_lineno)))
        return {"Status":"failed","reason":str(e)}      
        
    try:
        
        time_object = datetime(client_data['year'],client_data['month'],client_data['day'],hour=client_data['hour'],minute=client_data['minute'],second=client_data['second'],tzinfo=pytz.timezone("Asia/Kolkata"))
        app.logger.info(get_log(logging.INFO,request,time_object.strftime(format="%Y-%m-%d %H:%M:%S")+" Generated time object"))
    except Exception as e:
        app.logger.error(get_log(logging.ERROR,request,str(e)+"Client data invalid"))
        return {"Status":"failed","reason":str(e)}      
    

    try:
        result = get_default_forecast(time_object,config,client_data)
    except Exception as e:        
        app.logger.error(get_log(logging.ERROR,request,str(e)+" Unable to get forecast"))
        return {"Status":"failed","reason":str(e)}   

    request_info = {"time-stamp":datetime.now().strftime(format="%Y-%m-%d %H:%M:%S"),"username":client_data['username'],"lat":f"{client_data['lat']}",'lng':f"{client_data['lng']}","type":"particular time and location"}
    _status = database_handler.insert(request_info,config["request_info_table"])
    app.logger.info(get_log(logging.INFO,request,None)+f"Generated particular time forecast for {client_data['lat']}, {client_data['lng']} at {time_object.strftime(format='%Y-%m-%d %H:%M:%S')}")
    return jsonify(result)
    

#load_models()

if __name__ == "__main__":
    app.run(debug=True,port=5000)
