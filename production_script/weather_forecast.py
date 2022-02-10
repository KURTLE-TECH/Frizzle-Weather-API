import joblib
import pandas as pd
import numpy as np
from scipy.special import inv_boxcox

class Forecast(object):
    def __init__(self):
        return None        
        
    def transform_data(self,lat,lng,date_obj):
        date = pd.to_datetime(date_obj)              
        body = np.array([float(lat), float(lng), int(date.dayofweek) , int(date.quarter), int(date.month), int(date.dayofyear), int(date.day), int(date.weekofyear), int(date.year), int(date.hour*60 + date.minute)]).reshape(1,-1)
        return body

    def temp_forecast(self,body,config):
        temp_pred = config['temp_model'].predict(body)
        return temp_pred

    def press_forecast(self,body,config):
        press_pred = config['press_model'].predict(body)
        return press_pred

    def humid_forecast(self,body,config):
        humid_pred = inv_boxcox(config['humid_model'].predict(body), 2.5)
        return humid_pred

    def humid_class(self,body,config):
        humid_pred = config['humid_class'].predict(body)
        return humid_pred
    def cloud_forecast(self,body,config):
        cloud_pred = config['cloud_model'].predict(body)
        return cloud_pred

    def rain_forecast(self,body,config):
        rain_pred = config['rain_model'].predict(body)
        rain_pred_prob = config['rain_model'].predict_proba(body)
        return rain_pred[0],rain_pred_prob[0]

    def weath_forecast(self,body,config):
        try:
            weath_pred = config['weather_model'].predict(body)
            weath_pred_proba = config['weather_model'].predict_proba(body)
            return weath_pred[0],weath_pred_proba[0]
        except Exception as e:
            print("*******")
            print(e)
            print(e.__traceback__.tb_lineno)            
            return "no"