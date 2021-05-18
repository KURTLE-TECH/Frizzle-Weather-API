from datetime import datetime, timedelta,timezone
import pytz
from database import DynamodbHandler as db
#time_zone = timezone(offset=timedelta(hours=5,minutes=30),name="Asia/Kolkata")

#start, end, intervals
def get_prediction_times():
    time_zone = pytz.timezone('Asia/Kolkata')
    # now = datetime(year=2017,month=12,day=31,hour=11,minute=30,second=10,tzinfo=time_zone)
    now = datetime.now(tz=time_zone)
    tomorrow = now+timedelta(days=1)
    tomorrow = tomorrow.replace(hour=0,minute=0,second=0,microsecond=0,tzinfo=time_zone)
    prediction_times = list()
    required_time = now
    # print("Now:",type(now))
    # print("tomorrow:",type(tomorrow))
    while required_time<tomorrow:
        prediction_times.append(required_time)
        required_time+=timedelta(minutes=30)
    # for i in prediction_times:
        # print(i)
    return prediction_times

def get_distance(location,node_location):
    location = {i:float(location[i]) for i in location.keys()}
    node_location = {i:float(node_location[i]) for i in node_location.keys()}
    
    return ((location['lat']-node_location['lat'])**2+(location['lng']-node_location['lng'])**2)**0.5
    
def get_closest_node(location):
    db_handler = db.DynamodbHandler()
    response = db_handler.view_database('Nodes_Available')
    if 'Items' in response.keys():
    #model is default model        
        distances=dict()
        for node in response['Items']:
            node_location = dict()
            node_location['lat'] = node['lat']['S'] 
            node_location['lng'] = node['lng']['S']
            if node_location['lng'] !='' and node_location['lng'] != '':
                distance = get_distance(location,node_location)
                distances[distance] = node['Device ID']['S'] 
            
        return distances[min(distances.keys())]
        


    elif 'status' in response.keys():
        return None