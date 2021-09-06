from scipy.special import boxcox, inv_boxcox
import h2o
import boto3
import pandas as pd


class humid_model(object):
    def __init__(self):
        # self.data = data
        # self.model_path = model_path
        #self.model = h2o.load_model(model)
        a=1

    # def load_model(self, model):
        
    #     # model_object = self.client.get_object(
    #     #     Bucket="arn:aws:ap-south-1:465788651017:s3:frizzle-models",
    #     #     Key="Humidity-Model"
    #     # )
    #     # self.model = h2o.load_model(model_object)

    def transform_data(self, data):
        data[9] = str((float(data[9])*1.8) + 32)
        return data

    def predict_humid(self, data,model):
        cols = ['lat', 'lon', 'dayofweek', 'quarter', 'month', 'dayofyear',
                'dayofmonth', 'weekofyear', 'year', 'temp', 'press', 'minutes']
        humid_box = h2o.as_list(model.predict(h2o.H2OFrame(
            pd.DataFrame.from_dict(dict(zip(cols, [[i] for i in data]))))))['predict'][0]
        return inv_boxcox(humid_box, 2.5)

# h2o.connect(ip="localhost",port=54321)
# humidity_model_object = h2o.import_file("https://frizzle-models.s3.ap-south-1.amazonaws.com/Humidity-Model")
# model = humid_model()
# model.load_model(humidity_model_object)
