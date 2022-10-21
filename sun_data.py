from datetime import datetime, tzinfo
import requests
import pytz
from dateutil import tz
import logging

def get_extra_info(lat,lng,day):
    to_zone = tz.tzlocal()
    from_zone = tz.tzutc()
    day_string = day.strftime(format="%Y-%m-%d")
    day_string_uv = str(day).replace(' ','T')
    day_string_uv = day_string_uv[:-9] + 'Z'
   

    
    # uv_string = "https://api.openuv.io/api/v1/uv?lat={latitude}&lng={longitude}&dt={datetimeinput}".format(latitude=str(lat), longitude=str(lng), datetimeinput=str(day_string_uv))
    sun_string = "https://api.sunrise-sunset.org/json?lat={latitude}&lng={longitude}&date={datetimeinput}&formatted=0".format(latitude=lat, longitude=lng, datetimeinput=day_string)

    sun_data = requests.post(sun_string,verify=False)
    sun_data = sun_data.json()
    # uv_data = requests.get(uv_string,headers = { 'content-type': 'application/json','x-access-token': '8d4496a409009e18b9a2e167baf53a12' } )
    # uv_data = uv_data.json()
    # uv_value = uv_data['result']['uv_max']

    sunrise_time = sun_data['results']['sunrise'][:-6].replace('T'," ")
    sunrise_utc = datetime.strptime(sunrise_time, '%Y-%m-%d %H:%M:%S')
    sunrise_utc = sunrise_utc.replace(tzinfo=from_zone)
    sunrise_india = sunrise_utc.astimezone(to_zone)

    sunset_time = sun_data['results']['sunset'][:-6].replace('T'," ")
    sunset_utc = datetime.strptime(sunset_time, '%Y-%m-%d %H:%M:%S')
    sunset_utc = sunset_utc.replace(tzinfo=from_zone)
    sunset_india = sunset_utc.astimezone(to_zone)

    day_length = sun_data['results']['day_length']
    return {"Sunrise":sunrise_india.strftime("%H:%M"),"Sunset" :sunset_india.strftime("%H:%M"),"Daylight": str(day_length/3600)}
    
    #return uv_data


# a = get_extra_info(12.9354,77.5350,datetime.now())
#print('Sunrise Time is: ', a[0],', Sunset time is: ',a[1],', Daylight duration: ',a[2],', UV index:',a[3])
# print(a)
