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
        date = pd.to_datetime(date)
        pre_feat = [lat, lng, date.hour, date.dayofweek , date.quarter, date.month, date.dayofyear, date.day, date.weekofyear, date.year]
        self.feat = [str(i) for i in pre_feat]
        self.model_values = dict()
        body = ','.join(self.feat)
        response = self.client.invoke_endpoint(EndpointName=self.models['temperature_model'],
                                  ContentType = 'text/csv',
                                  Body = body)
        pred_temp = float(response['Body'].read().decode('utf-8'))
        #self.model_values['pred_temp'] = pred_temp
        return pred_temp



#val = Endpoint_Calls(12.971599,	77.594563, '2021-07-08 23:00:00')
#print(val.temp_model())