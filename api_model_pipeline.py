import joblib
import datetime
import pandas as pd
import base64
from PIL import Image
from io import BytesIO
from cloud_image_ext import colour_ext, cloud_classification
import numpy as np

class Model_Pipeline(object):
    def __init__(self, lat, longitude, time, image_string, duration):
        self.feat = list()
        self.lat = lat
        self.long = longitude
        self.time = time
        self.image = Image.open(BytesIO(base64.b64decode(image_string))) if image_string is not None else None
        # self.temp_model = None
        # self.press_model= None
        # self.humid_model= None
        #self.wind_speed_model = None
        # self.weath_model = None
        self.model_initial()

    def model_initial(self):
        self.temp_model = joblib.load("models/temperature_model.joblib.dat")
        self.press_model = joblib.load("models/pressure_model.joblib.dat")
        self.humid_model = joblib.load("models/humidity_model.joblib.dat")
        #self.wind_speed_model = joblib.load("models/wind_speed_model.joblib.dat")
        self.weath_model = joblib.load("models/weath_model.joblib.dat")

    def time_feat(self):
        temp_time = pd.DataFrame([self.time], columns = ['datetime'])
        temp_date = pd.to_datetime(temp_time['datetime'])
        self.feat.append(temp_date.dt.hour)
        self.feat.append(temp_date.dt.dayofweek)
        self.feat.append(temp_date.dt.quarter)
        self.feat.append(temp_date.dt.month)
        self.feat.append(temp_date.dt.dayofyear)
        self.feat.append(temp_date.dt.day)
        self.feat.append(temp_date.dt.weekofyear)

    def get_feat(self):
        temperature = self.temp_model.predict(np.array(self.feat))
        self.feat.append(temperature)
        humidity = self.humid_model.predict(np.array(self.feat))
        self.feat.append(humidity)
        pressure = self.press_model.predict(np.array(self.feat))
        self.feat.append(pressure)
        #wind_speed = self.wind_speed_model.predict(self.feat)
        #self.feat.append(wind_speed)

    def get_feat_from_image(self):
        colour_init = colour_ext.Image_Colour_Extract(self.image)
        final_color = colour_init.percent_values()
        cloud_type_init = cloud_classification.Cloud_Classification(self.image)
        final_cloud_type = cloud_type_init.cloud_classify()
        # pca_single_color
        #Algorithm to pca into single color value

    def forecast_weath(self):
        #Forecast general weather conditions for a long term perspective
        self.get_feat()
        weather_desc = self.weath_model.predict(np.array(self.feat))
        return weather_desc

    def forecast_micro_weath(self):
        self.get_feat()
        #Insert conditions for forecast of micro-weather conditions
        #Get difference in timing in order to write algorithm accordingly