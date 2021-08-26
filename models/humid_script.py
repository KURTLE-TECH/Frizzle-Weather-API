from scipy.special import boxcox, inv_boxcox
import h2o
import boto3
import pandas as pd
h2o.init(log_dir="../h2o_logs/",port = 8000,log_level="INFO")     

class humid_model(object):
    def __init__(self):
        # self.data = data
        # self.model_path = model_path
        self.client = boto3.client('s3')   
        
        
    def load_model(self,model_path):
        self.model = h2o.load_model("models/Humidity-Model")
        # model_object = self.client.get_object(
        #     Bucket="arn:aws:ap-south-1:465788651017:s3:frizzle-models",
        #     Key="Humidity-Model"
        # )
        # self.model = h2o.load_model(model_object)
        
    def transform_data(self,data):
        data[9] = str((float(data[9])*1.8) + 32)    
        return data

    def predict_humid(self,data):
        cols = ['lat','lon','dayofweek','quarter','month','dayofyear','dayofmonth','weekofyear','year','temp','press','minutes']
        humid_box = h2o.as_list(self.model.predict(h2o.H2OFrame(pd.DataFrame.from_dict(dict(zip(cols,[[i] for i in data]))))))['predict'][0]
        return inv_boxcox(humid_box, 2.5)




