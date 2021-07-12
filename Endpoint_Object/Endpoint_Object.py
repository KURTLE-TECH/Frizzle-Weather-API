import pandas as pd
import boto3

class Endpoint_Calls(object):
    def __init__(self,region_name,access_key,secret_access_key,models):
        self.client = boto3.client('runtime.sagemaker', 
                      region_name = region_name,
                      aws_access_key_id = access_key,
                      aws_secret_access_key = secret_access_key
                      )
        self.models = models
        # date = pd.to_datetime(date)
        # pre_feat = [lat, lng, date.hour, date.dayofweek , date.quarter, date.month, date.dayofyear, date.day, date.weekofyear, date.year]
        # self.feat = [str(i) for i in pre_feat]
        # self.model_values = dict()


    def temp_model(self,lat,lng,date):
        #print(date)
        date = pd.to_datetime(date)
        pre_feat = [lat, lng, date.dayofweek , date.quarter, date.month, date.dayofyear, date.day, date.weekofyear, date.year, date.hour*60 + date.minute]
        self.feat = [str(i) for i in pre_feat]
        body = ','.join(self.feat)
        response = self.client.invoke_endpoint(EndpointName=self.models["temperature_model"], 
                                  ContentType = 'text/csv',
                                  Body = body)
        pred_temp = response['Body'].read().decode('utf-8')
        #self.model_values['pred_temp'] = pred_temp
        self.feat.insert(-1,pred_temp)
        return pred_temp
   
    def press_model(self):
        body = ','.join(self.feat)
        response = self.client.invoke_endpoint(EndpointName=self.models["pressure_model"], 
                                  ContentType = 'text/csv',
                                  Body = body)
        pred_press = response['Body'].read().decode('utf-8')
        self.feat.insert(-1,pred_press)
        return pred_press
    
    def humid_model(self):
        body = ','.join(self.feat)
        response = self.client.invoke_endpoint(EndpointName=self.models["humidity_model"], 
                                  ContentType = 'text/csv',
                                  Body = body)
        pred_humid = response['Body'].read().decode('utf-8')
        self.feat.insert(-1,pred_humid[0])
        return pred_humid

    def cloud_model(self):
        body = ','.join(self.feat)
        response = self.client.invoke_endpoint(EndpointName=self.models["cloud_model"], 
                                  ContentType = 'text/csv',
                                  Body = body)
        pred_cloud = response['Body'].read().decode('utf-8')
        self.feat.insert(-1,pred_cloud[0])
        return pred_cloud

    def rain_model(self):
        body = ','.join(self.feat)
        response = self.client.invoke_endpoint(EndpointName=self.models["rain_model"], 
                                  ContentType = 'text/csv',
                                  Body = body)
        pred_rain = response['Body'].read().decode('utf-8')
        return pred_rain

    def forecast_model(self):
        body = ','.join(self.feat)
        response = self.client.invoke_endpoint(EndpointName=self.models["weath_model"], 
                                  ContentType = 'text/csv',
                                  Body = body)
        pred_weath = response['Body'].read().decode('utf-8')
        return pred_weath

