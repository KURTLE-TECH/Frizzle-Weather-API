import boto3
from flask import Flask, render_template, request
from database import DynamodbHandler as db
db_handler = db.DynamodbHandler()
from json import loads
import redis
from datetime import datetime
import random

# table_name = "519eb77c-98ea-4b76-a017-5ff4abfe0e56"
redis_host = "frizzle-redis-cluster.zcgu4a.ng.0001.aps1.cache.amazonaws.com"
redis_port = 6379
redis_endpoint = redis_cluster_endpoint = redis.Redis(
host=redis_host,
port=redis_port,
db=0)
from api_model_pipeline import Model_Pipeline
weather_condition={0:"Sunny",1:"Cloudy",2:"Rainy"}

app = Flask(__name__)
@app.route('/')
def hello_world():
    if request.method == "GET":
        return 'Hello, World!'
    else:
        return "Root method uses only GET, Please try again"

@app.route('/get_prediction',methods=["GET"])
def get_prediction():
    if request.method=="GET":
        try:
            client_data = loads(request.data)
            # print(client_data)
        except Exception as e:
            print(e)
        num = random.randrange(0,3)
        # model = Model_Pipeline(client_data['lat'],client_data['lng'],datetime.now(),None,None)
        perc = random.randint(0,10)

        return {"Condition":weather_condition[num],'Percentage':perc*10}

if __name__ == "__main__":
    app.run(debug=True)