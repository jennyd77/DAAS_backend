import boto3
import json
import requests

client = boto3.client('iot-data', region_name = 'ap-southeast-2')

def lambda_handler(event, context):
    '''
        TODO:
		Test comment in zip archive - delete this line
            - check whether the event JSON has open or closed state
            - check weather based on location in event data
            - log activity to elasticsearch for dashboard
    '''
    print("event: ", event)
    latitude=event["state"]["desired"]["latitude"]
    longitude=event["state"]["desired"]["longitude"]
    print("latitude: ", latitude)
    print("longitude: ", longitude)
    json_weather=requests.get("http://api.worldweatheronline.com/premium/v1/weather.ashx?key=d4dab5e2738542f884000750172904&q=Melbourne,Victoria,Australia&num_of_days=1&format=json&localObsTime=yes")
    print("json_weather: ", json_weather)
    '''| jq -r .data.weather[0].hourly[0].chanceofrain'''
    json_message = json.dumps({"state":{"desired":{"playbackStart": True}}})
    '''response = client.update_thing_shadow(thingName = "DiscoMaster2000", payload = json_message)'''
    response = client.update_thing_shadow(thingName = "DiscoMaster2000", payload = json_weather)
    print response
    return "done"
