import boto3
import json
import requests
import random
from botocore.exceptions import ClientError

client = boto3.client('iot-data', region_name = 'ap-southeast-2')
dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
registeredDevicesTable = dynamodb.Table('DAAS_device_registry')
songListTable = dynamodb.Table('DAAS_song_list')

def rain_check(latitude, longitude):
    print("Device's reported latitude: ", latitude)
    print("Device's reported longitude: ", longitude)
    print("Passing device geo-location to worldweatheronline to see if it is raining")
    apiurl="http://api.worldweatheronline.com/premium/v1/weather.ashx?key=d4dab5e2738542f884000750172904&q=%s,%s&num_of_days=1&format=json&localObsTime=yes" % (latitude,longitude)
    print("Making HTTP GET request to the following url: ", apiurl)
    response=requests.get(apiurl)
    try:
        json_weather = response.json()
    except ValueError:
        json_weather = response.text
    print("Full response from worldweatheronline: ", json_weather)
    chance_of_rain = json_weather["data"]["weather"][0]["hourly"][0]["chanceofrain"]
    print("chance_of_rain: ", int(chance_of_rain))
    return int(chance_of_rain)

def get_registered_owner(macAddress):
    try:
        response = registeredDevicesTable.get_item(
            Key={
                'macAddress': macAddress
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        item = response['Item']
        print("item: ", item)
        registeredOwner=item["registeredOwner"]
    return registeredOwner
    
def which_song_to_play():
    songscan = songListTable.scan()
    songcount = int(songscan["Count"])
    print("count: ",songcount)
    songref=random.randint(1, songcount)    
    print("songref: ",songref)
    song = songscan["Items"][songref]
    print("songname: ",song)
    return song

def lambda_handler(event, context):
    '''
        TODO:
		Test comment in zip archive - delete this line
            - check whether the event JSON has open or closed state
            - check weather based on location in event data
            - log activity to elasticsearch for dashboard
    '''
    print("event: ", event)
    doorstate=event["state"]["desired"]["doorstate"]

    if doorstate == "open":
        print("The door is open")
        macAddress=event["state"]["desired"]["macaddress"]
        print("macAddress: ", macAddress)
        registeredOwner=get_registered_owner(macAddress)
        print("Welcome home",registeredOwner)
        latitude=event["state"]["desired"]["latitude"]
        longitude=event["state"]["desired"]["longitude"]
        chance_of_rain = rain_check(latitude, longitude)
        if chance_of_rain == 100:
            songitem = songListTable.get_item(Key={'title': 'its_raining_men'})
            song = songitem["Item"]
        else:
            song = which_song_to_play()            
        print("song: ", song)
        payload = {'state':{'desired':{'playbackStart': 'True', 'song': {'mark_in': '01', 'song_name': 'Im so excited', 'artist': 'Pointer Sisters', 'title': 'im_so_excited'}}}}
        payload["state"]["desired"]["song"]=song
        json_message = json.dumps(payload)
        print("json_message: ", json_message)
        response = client.update_thing_shadow(thingName = "DiscoMaster2000", payload = json_message)
        print("response: ", response)
    else:
        print("The door is closed")


    return "done"
