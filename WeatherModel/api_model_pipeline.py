import pandas as pd
import numpy as np
import base64
from PIL import Image
from io import BytesIO
from cloud_image_ext import colour_ext_v3, cloud_classification
from sklearn.decomposition import PCA

class Model_Pipeline(object):
    def __init__(self,image_string,models):
        self.image = Image.open(BytesIO(base64.b64decode(image_string)))  if image_string is not None else None        
        #self.wind_speed_model = None
        self.weath_model = None
        self.temp_model = models['temperature']
        self.press_model = models['pressure']
        self.humid_model = models['humidity']
        #self.wind_speed_model = joblib.load("models/wind_speed_model.joblib.dat")
        self.weath_model = models['weather']
        

    def time_feat(self,time):
        self.feat = dict()
        temp_time = pd.DataFrame([time], columns = ['datetime'])
        temp_date = pd.to_datetime(temp_time['datetime'])

        # self.feat.append(temp_date.dt.month[0])
        # self.feat.append(temp_date.dt.dayofweek[0])
        # self.feat.append(temp_date.dt.weekofyear[0])
        # if temp_date.dt.hour[0] in range(5,12):
        #     self.feat.append(0)
        # elif temp_date.dt.hour[0] in [12,13,14,15]:
        #     self.feat.append(1)
        # elif temp_date.dt.hour[0] in [16,17,18,19]:
        #     self.feat.append(2)
        # elif temp_date.dt.hour[0] in [20,21,22,23,0,1,2,3,4]:
        #     self.feat.append(3)
        # self.feat.append(temp_date.dt.hour[0])
        # self.feat.append(temp_date.dt.dayofyear[0])
        # self.feat.append(temp_date.dt.day[0])
        # self.feat.append(temp_date.dt.quarter[0])
        
        # print(type(temp_date.dt.hour[0]))
        
        self.feat['hour'] = [temp_date.dt.hour[0]]
        self.feat['dayofweek'] = [temp_date.dt.dayofweek[0]]
        self.feat['quarter'] = [temp_date.dt.quarter[0]]
        self.feat['month'] = [temp_date.dt.month[0]]
        self.feat['dayofyear'] = [temp_date.dt.dayofyear[0]]
        self.feat['dayofmonth'] = [temp_date.dt.day[0]]
        self.feat['weekofyear'] = [temp_date.dt.weekofyear[0]]
        if temp_date.dt.hour[0] in range(5,12):
            self.feat['Time_Of_Day'] = [0]
        elif temp_date.dt.hour[0] in [12,13,14,15]:
            self.feat['Time_Of_Day'] = [1]
        elif temp_date.dt.hour[0] in [16,17,18,19]:
            self.feat['Time_Of_Day'] = [2]
        elif temp_date.dt.hour[0] in [20,21,22,23,0,1,2,3,4]:
            self.feat['Time_Of_Day'] = [3]

        self.feat = pd.DataFrame.from_dict(self.feat)
        
    def get_feat(self):
        
        self.temperature = self.temp_model.predict(self.feat)
        
        # self.feat['Temperature'] = temperature[0]   
        self.feat.insert(7,"Temperature",[self.temperature[0]],True)
        self.humidity = self.humid_model.predict(self.feat)
        self.feat.insert(7,"Humidity",[self.humidity[0]],True)
        self.pressure = self.press_model.predict(self.feat)

        self.feat.insert(7,"Pressure",[self.pressure[0]],True)
        # print(self.feat)
        #wind_speed = self.wind_speed_model.predict(self.feat)
        #self.feat.append(wind_speed)

    def get_feat_from_image(self,image):
        pca = PCA(n_components = 1)
        colour_init = colour_ext_v3.Image_Colour_Extract(image)
        final_color = colour_init.NNperc()
        #cloud_type_init = cloud_classification.Cloud_Classification(self.image)
        #final_cloud_type = cloud_type_init.cloud_classify()
        # final_color = np.array(list(final_color.values())).reshape(-1,1)
        # print(final_color)        
        final_color = pd.DataFrame.from_dict(final_color)
        # print(final_color)        
        pca_single_color = pca.fit_transform(final_color)
        print(pca_single_color)
        self.feat.insert(11,"colors",pca_single_color,True)
        #Algorithm to pca into single color value

    def forecast_weath(self,time):
        #Forecast general weather conditions for a long term perspective
        self.time_feat(time)
        self.get_feat()
        self.feat.insert(11,"colors",np.nan,True)
        # print(np.array(self.feat).shape)
        self.feat = self.feat[['hour', 'dayofweek', 'quarter', 'month', 'dayofyear', 'dayofmonth', 'weekofyear', 'Humidity', 'Pressure', 'Temperature', 'Time_Of_Day', 'colors']]
        weather_desc = self.weath_model.predict(self.feat)
        return weather_desc

    def forecast_micro_weath(self,time):
        self.time_feat(time)
        self.get_feat()
        self.get_feat_from_image(self.image) #room for optimisation ; is repeatedly called
        self.feat = self.feat[['hour', 'dayofweek', 'quarter', 'month', 'dayofyear', 'dayofmonth', 'weekofyear', 'Humidity', 'Pressure', 'Temperature', 'Time_Of_Day', 'colors']]
        weather_desc = self.weath_model.predict(self.feat)
        return weather_desc
        #Insert conditions for forecast of micro-weather conditions
        #Get difference in timing in order to write algorithm accordingly

#obj = Model_Pipeline(12.57,77.59,datetime.datetime.now(),None,None)
#print(obj.forecast_weath())
