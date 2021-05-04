from datetime import datetime, timedelta,timezone
import pytz
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