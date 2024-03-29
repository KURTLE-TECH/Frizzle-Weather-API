import logging
import joblib
import pandas as pd
import numpy as np
from scipy.special import inv_boxcox

class Forecast(object):
    def __init__(self):
        return None        
        
    def transform_data(self,lat,lng,alt,date_obj):
        try:
            date = pd.to_datetime(date_obj)              
            # body = np.array([float(lat), float(lng), int(date.dayofweek) , int(date.quarter), int(date.month), int(date.dayofyear), int(date.day), int(date.weekofyear), int(date.year), int(date.hour*60 + date.minute)]).reshape(1,-1)
            body = [float(lat), float(lng)]
            body.extend([int(date.dayofweek) , int(date.quarter), int(date.month), int(date.dayofyear), int(date.day), int(date.weekofyear), int(date.hour*60 + date.minute), int(date.year),float(alt)])
            return np.array([body])
        except Exception as e:
            logging.error(e,e.__traceback__.tb_lineno)

    def temp_forecast(self,body,config):
        try:
            temp_pred = config['temp_model'].predict(body)
            return temp_pred
        except Exception as e:
            logging.error(e)
            logging.error(str(e))
            logging.error(e.__traceback__.tb_lineno)

    def press_forecast(self,body,config):
        press_pred = config['press_model'].predict(body)
        return press_pred

    # def humid_forecast(self,body,config):
    #     humid_pred = inv_boxcox(config['humid_model'].predict(body), 2.5)
    #     return humid_pred

    def humid_class(self,body,config):
        humid_pred = config['humid_class'].predict(body)
        return humid_pred
        
    def cloud_forecast(self,body,config):
        cloud_pred = config['cloud_model'].predict(body)
        cloud_pred = cloud_pred.astype(int)
        return cloud_pred

    def rain_forecast(self,body,config):
        rain_pred = config['rain_model'].predict(body)
        rain_pred_prob = config['rain_model'].predict_proba(body)
        return rain_pred[0],rain_pred_prob[0]

    def rain_forecast_new(self,body,config):
        rain_pred = config['rain_model'].predict(body)
        #rain_pred_prob = config['rain_model'].predict_proba(body)
        return rain_pred

    def rain_forecast_prob(self,body,config):
        #rain_pred = config['rain_model'].predict(body)
        rain_pred_prob = config['rain_model'].predict_proba(body)
        rain_pred_prob = rain_pred_prob.astype(float)
        return np.array([max(i) for i in rain_pred_prob])

    def weath_forecast(self,body,config):
        try:
            weath_pred = config['weather_model'].predict(body)
            #weath_pred_proba = config['weather_model'].predict_proba(body)
            return weath_pred
        except Exception as e:
            print("*******")
            print(e)
            print(e.__traceback__.tb_lineno)            
            return "no"

    def weath_forecast_op(self,body,config):
        try:
            #weath_pred = config['weather_model'].predict(body)
            weath_pred_proba = config['weather_model'].predict_proba(body)
            return weath_pred_proba
        except Exception as e:
            print("*******")
            print(e)
            print(e.__traceback__.tb_lineno)            
            return "no"