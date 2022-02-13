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
from flask import Flask, request, jsonify, send_file,make_response
from flask_cors import CORS, cross_origin
import pdfkit
from PyPDF2 import PdfFileMerger
from json import loads
from datetime import datetime, time, tzinfo
from get_data import forecast, get_closest_half_hour, get_data_from_timestream, get_default_forecast, get_past_data_from_timestream, get_prediction_times, get_closest_node, get_log,get_detailed_forecast,get_data_from_redis,get_elevation
from database import DynamodbHandler
#from CloudPercentage import Cloud_Percentage
import logging
import pytz
from concurrent.futures import ThreadPoolExecutor,as_completed
from math import ceil
from frizzle_models.humid_script import humid_model
import os
from production_script.weather_forecast import Forecast
import pandas as pd
import numpy as np
from api_authenticator import ApiAuthenticator
import hashlib

with open("config.json","r") as f:
    config = loads(f.read())    
    
    config['frizzle-humidity-wrapper'] = humid_model()
    config['temp_model'] = joblib.load('production_script/models/temp_20_12_NR_model.sav')
    config['press_model'] = joblib.load('production_script/models/press_06_01_22_nonreal_time_model.sav')
    # config['humid_model'] = joblib.load('production_script/models/humid.sav')
    config['humid_class'] = joblib.load('production_script/models/hum_8_class_07_01_NR_model.sav')
    config['cloud_model'] = joblib.load('production_script/models/clouds_class_7_1_NR_model_v3.sav')
    config['rain_model'] = joblib.load('production_script/models/rain_class_7_1_NR_model_Coimb_v2.sav')
    config['weather_model'] = joblib.load('production_script/models/weath_class_7_1_NR_model_Coimb_v2.sav')

    

redis_endpoint = redis_cluster_endpoint = redis.Redis(host=config["redis_host"],port=config["redis_port"],db=0)
models = dict()
weather_condition = config["weather_condition"]
database_handler = DynamodbHandler.DynamodbHandler(region="ap-south-1")
auth_object = ApiAuthenticator(config)
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
        client_data['alt'] = get_elevation(client_data)
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
        # mid_template = ""
        with open("report_templates/stats.html") as stats_html:
            stats_template = stats_html.read()
        
        with open("report_templates/forecast.html") as forecast_html:
            forecast_template = forecast_html.read()

        with open("report_templates/cover.html") as cover_html:
            cover_template = cover_html.read()

        # with open("report_templates/mid.html") as mid_html:
        #     mid_template = mid_html.read()

        options = {
            'page-size': 'Letter',
            'encoding': "UTF-8",
            'no-outline': None,
            'orientation': 'Landscape'
        }

        pdfFiles = []

        dates = list(forecasted_weather.keys())
        dates.sort()

        if "address" in client_data.keys():
            current_cover = cover_template.replace("{{location}}", client_data["address"])
        else:
            current_cover = cover_template.replace("{{location}}", f"({client_data['lat']}, {client_data['lng']})")

        current_cover = current_cover.replace("{{period}}", f"{dates[0]} - {dates[-1]}")
        pdfkit.from_string(current_cover, 'report_templates/cover.pdf', options = options)
        pdfFiles.append('report_templates/cover.pdf')

        # pdfkit.from_string(mid_template, 'report_templates/mid.pdf', options = options)
        # pdfFiles.append('report_templates/mid.pdf')

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
        
    
        response_file = make_response(send_file("report_templates/report.pdf",as_attachment=True))
        response_file.headers['Content-Type'] = 'application/pdf'
        response_file.headers['Content-Disposition'] = 'attachment'
        return response_file

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
        client_data['alt'] = float(client_data['elevation'])  
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
        # client_data['alt'] = float(client_data['elevation'])
        if 'username' not in client_data.keys():
        	client_data['username']="unknown"      
    
    except Exception as e:
        app.logger.error(get_log(logging.ERROR,request,e.__str__))
        return jsonify({"Status": "Failed", "Reason": str(e),"Line":f"{e.__traceback__.tb_lineno}"})

    client_data['alt'] = get_elevation(client_data)

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

@app.route("/api/get_past_data",methods=["GET","POST"])
@cross_origin()
def get_past_data():
    try:
        client_data = loads(request.data)
        node_id = client_data["Device ID"]
        file_type = "json" if "type" not in client_data else client_data["type"]
    except Exception as e:
        app.logger.error(get_log(logging.ERROR,request,str(e)+" Unable to load client data"))
        return {}
    try:
        uid = random.randint(1, 1000)
        data = get_past_data_from_timestream(node_id,time_stream_client)

        if len(data.keys()) == 0:
            return "Node not available", 400
            
        if file_type == "json":
            return jsonify(data)

        with open(f"temp_files/test-{uid}.csv", "w") as file:
            header_row = "Time stamp, Dew Point, Heat Index, Humidity, Light, Pressure, Rain, Temperature\n"
            file.write(header_row)
            for date in data:
                d = data[date]
                row = f'{date}, {d["Dew Point"]}, {d["Heat Index"]}, {d["Humidity"]}, {d["Light"]}, {d["Pressure"]}, {d["Rain"]}, {d["Temperature"]}\n'
                file.write(row)

        if file_type == "csv":
            return send_file(f"temp_files/test-{uid}.csv", mimetype='text/csv')

        csv_file = pd.read_csv(f"temp_files/test-{uid}.csv")
        excel_file = pd.ExcelWriter(f"temp_files/test-{uid}.xlsx")
        csv_file.to_excel(excel_file, index = False)
        excel_file.save()

        return send_file(f"temp_files/test-{uid}.xlsx", mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                
    except Exception as e:
        app.logger.error(get_log(logging.ERROR,request,str(e)+" Unable to fetch node data from redis as no connection"))
        return {"Status":"failed","reason":str(e)}             
    app.logger.info(get_log(logging.info,request,None))    
    return jsonify(data)

@app.route("/api/get_future_data", methods = ["GET", "POST"])
@cross_origin()
def get_future_data():
    try:
        client_data = loads(request.data)
        client_data['lat'] = float(client_data['lat'])
        client_data['lng'] = float(client_data['lng'])
        client_data['alt'] = float(client_data['alt'])  
        node_id = client_data["Device ID"]
        file_type = "json" if "type" not in client_data else client_data["type"]
    except Exception as e:
        app.logger.error(get_log(logging.ERROR,request,str(e) + " Unable to load client data"))
        return {}

    if "email" in client_data.keys():
        try:
            user_info = database_handler.query(config['user_table'], "email", client_data["email"])        
            response = database_handler.query("Tiers","tier", user_info["Response"]['tier'])
            client_data['days'] = int(response['Response']['days'])
        except Exception:
            client_data['days']=2

    else:
        client_data['days']=2
    
    try:

        all_days = get_prediction_times(start_day = datetime.now(), interval=None,days=client_data["days"], time_zone="Asia/Kolkata")   
        forecasted_weather = dict()                
        with ThreadPoolExecutor(max_workers = client_data["days"]) as e:
            futures = {e.submit(get_detailed_forecast, day, config, client_data):day for day in all_days}
            for future in as_completed(futures):
                forecasted_weather[futures[future].strftime("%y-%m-%d")] = future.result()

        if file_type == "json":
            return jsonify(forecasted_weather)

        rain_class_mapping = {
            "0": "0mm",
            "1": "0 - 0.5mm",
            "2": "0.5 - 1mm",
            "3": "1 - 5mm",
            "4": "5 - 10mm"
        }

        uid = random.randint(1, 1000)
        dates = list(forecasted_weather.keys())
        dates.sort()

        with open(f"temp_files/future-forecast-{uid}.csv", "w") as file:
            header_row = "Time Stamp, Condition, Humidity, Pressure, Rain Class, Rain Probability, Temperature\n"
            file.write(header_row)
            for date in dates:
                for timestamp in forecasted_weather[date]["forecast"]:
                    rain_class = rain_class_mapping[forecasted_weather[date]["rain_class"][timestamp]]
                    rain_probability = 0 if rain_class == "0" else forecasted_weather[date]["rain_probability"][timestamp]
                    line = f'{timestamp}, {forecasted_weather[date]["condition"][timestamp]}, {forecasted_weather[date]["humidity"][timestamp]}, {forecasted_weather[date]["pressure"][timestamp]}, {rain_class}, {rain_probability}, {forecasted_weather[date]["temperature"][timestamp]}\n'
                    file.write(line)

        if file_type == "csv":
            return send_file(f"temp_files/future-forecast-{uid}.csv", mimetype='text/csv')

        csv_file = pd.read_csv(f"temp_files/future-forecast-{uid}.csv")
        excel_file = pd.ExcelWriter(f"temp_files/future-forecast-{uid}.xlsx")
        csv_file.to_excel(excel_file, index = False)
        excel_file.save()

        return send_file(f"temp_files/future-forecast-{uid}.xlsx", mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    

    except Exception as e:
        app.logger.error(get_log(logging.ERROR,request,str(e)+" Unable to fetch node data from redis as no connection"))
        return {"Status":"failed","reason":str(e)}

    app.logger.info(get_log(logging.info,request,None))    
    return jsonify(forecasted_weather)
        

    
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
        
        if 'username' not in client_data.keys():
            client_data['username'] = "unknown"
        if 'email' not in client_data.keys():
            client_data['email'] = "unknown"
        
    except Exception as e:
        app.logger.error(get_log(logging.ERROR,request,str(e)+"Unable to load client data"+str(e.__traceback__.tb_lineno)))
        return {"Status":"failed","reason":str(e)}, 400      
    
    try:
        key = request.args.get('key')
        request_type = request.args.get('type')

    except Exception as e:
        app.logger.error(get_log(logging.ERROR,request,str(e)+"Parameters missing"+str(e.__traceback__.tb_lineno)))
        return "Parameters missing",401
    
    client_data['alt'] = get_elevation(client_data)
    
    #authenticate key
    validation = auth_object.validate_key(key,request_type)
    if validation==False:
        app.logger.error(f"No permission for {key}")
        return "No permission for key", 403    

    data = database_handler.query(config['api_table'],"key",hashlib.md5(key.encode('utf-8')).hexdigest())    
    if data['status'] == 'success':
        client_data['days'] = int(data['Response']['Days'])
    else:
        client_data['days'] = 2
    
    data = forecast(request_type,client_data,config)

    if data['status'] == 'fail':
        app.logger.error(f"Unable to forecast. Reason {data['reason']}")
        return data, 500

    request_info = {"time-stamp":datetime.now().strftime(format="%Y-%m-%d %H:%M:%S"),"username":key,"lat":f"{client_data['lat']}",'lng':f"{client_data['lng']}","type":type}
    _status = database_handler.insert(request_info,config["request_info_table"])
    app.logger.info(get_log(logging.INFO,request,None)+f"Generated {key} forecast for {key} at {datetime.now().strftime(format='%Y-%m-%d %H:%M:%S')}")
    return jsonify(data['data'])
    

    
    

#load_models()

if __name__ == "__main__":
    app.run(host="0.0.0.0",debug=True,port=5000)
