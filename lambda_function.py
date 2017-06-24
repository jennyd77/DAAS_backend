import boto3
import json
import requests
import random
from botocore.exceptions import ClientError
from collections import namedtuple
from datetime import datetime
from datetime import timedelta
import time
#from time import gmtime

client = boto3.client('iot-data', region_name = 'ap-southeast-2')
dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
s3 = boto3.resource('s3', region_name='ap-southeast-2')
S3PollyBucket = s3.Bucket('daas-polly-files')
registeredDevicesTable = dynamodb.Table('DAAS_device_registry')
songListTable = dynamodb.Table('DAAS_song_list')
pollyClient = boto3.client('polly', region_name = 'us-west-2')
s3client = boto3.client('s3')

def weather_check(latitude, longitude):
    print("Device's reported latitude: ", latitude)
    print("Device's reported longitude: ", longitude)
    print("Passing device geo-location to worldweatheronline to see if it is raining")
    #apiurl="http://api.worldweatheronline.com/premium/v1/weather.ashx?key=d4dab5e2738542f884000750172904&q=%s,%s&num_of_days=1&format=json&localObsTime=yes" % (latitude,longitude)
    apiurl='http://api.worldweatheronline.com/premium/v1/weather.ashx?key=4d8f5ad3106b492ba1323348172206&q=%s,%s&num_of_days=1&format=json&localObsTime=yes' % (latitude,longitude)
    print("Making HTTP GET request to the following url: ", apiurl)
    response=requests.get(apiurl)
    try:
        json_weather = response.json()
        print("Successful call to Weather API")
    except ValueError:
        json_weather = response.text
        print("Unsuccessful call to Weather API")
    print("Full response from worldweatheronline: ", json_weather)
    if json_weather=="":
        cor = "unknown"
        ct = "unknown"
    else:
        cor = json_weather["data"]["weather"][0]["hourly"][0]["chanceofrain"]
        ct = json_weather["data"]["current_condition"][0]["temp_C"]
    print("chance_of_rain: ", cor)
    print("current_temp: ", ct)
    return {'chance_of_rain':cor, 'current_temp':ct}

def time_check(latitude, longitude):
    print("Passing device geo-location to Google Maps TimeZone API to find local time offset")
    epochtime=time.time()
    print("epochtime: ",int(epochtime))
    apiurl='https://maps.googleapis.com/maps/api/timezone/json?location=%s,%s&timestamp=%d&key=AIzaSyBy-rsJ2uG-CEAWglzqdZEqMArAvrGEuFs' % (latitude,longitude,int(epochtime))
    print("Making HTTP GET request to the following url: ", apiurl)
    response=requests.get(apiurl)
    try:
        json_timezone = response.json()
        print("Successful call to Google Maps Time Zone API")
    except ValueError:
        json_timezone = response.text
        print("Unsuccessful call to Google Maps Time Zone API")
    print("Full response from Google Maps Timezone API: ", json_timezone)
    if json_timezone["status"]=="OK":
        print("Received results from Google Maps")
        base_offset = json_timezone["rawOffset"]
        dst_offset = json_timezone["dstOffset"]
        utc_offset = base_offset+dst_offset
        print("utc_offset: ",utc_offset)
        utc_time_now=datetime.utcnow()
        print("utc_time_now: ", utc_time_now)
        local_time_now = utc_time_now + timedelta(seconds=utc_offset)
        print("local_time_now: ", local_time_now)
        print("hour: ", local_time_now.hour)
        print("string datetime: ", str(local_time_now))
        local_time_str=local_time_now.strftime('%I:%M%p')
    else:
        local_time_str = "unknown"    
        print("No results from Google Maps")

    return local_time_str

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
    songref=random.randint(0, songcount-1)    
    print("songref: ",songref)
    song = songscan["Items"][songref]
    print("songname: ",song)
    return song

def create_voice_message(registeredOwner, current_temp, song_title, song_artist, time_str):
	voiceMessage="Welcome home " + registeredOwner + ", that was " + song_title + " by " + song_artist + ", the time is " + time_str + ", and the current temperature is " + current_temp + " degrees." 
	print("Calling Polly with message: ",voiceMessage)
	response=pollyClient.synthesize_speech(Text=voiceMessage, VoiceId='Salli', OutputFormat='mp3')
	data_stream=response.get("AudioStream")
	filename = "%s.mp3" % registeredOwner
	print("Polly has created audio file: ",filename)
	S3PollyBucket.put_object(Key=filename, Body=data_stream.read())
	url=s3client.generate_presigned_url(
		ClientMethod='get_object',
		Params={
			'Bucket': 'daas-polly-files',
			'Key': filename
		},
		ExpiresIn=3000
	)
	print("Pre-signed url with synthesized voice message: ", url)
	return url

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
        # Using the MAC address of the device, lookup name of registered owner
        macAddress=event["state"]["desired"]["macaddress"]
        print("macAddress: ", macAddress)
        registeredOwner=get_registered_owner(macAddress)
        print("Welcome home",registeredOwner)
        
        # Using the received latitude and longitude, determine current temperature and chance of rain
        latitude=event["state"]["desired"]["latitude"]
        longitude=event["state"]["desired"]["longitude"]
        weather = weather_check(latitude, longitude)
        chance_of_rain=weather['chance_of_rain']
        current_temp=weather['current_temp']
        print("Current temp: ",current_temp)
        #chance_of_rain=50
        
        #If it's raining override song selection with "It's raining men", otherwise make song selection
        if chance_of_rain == 100:
            songitem = songListTable.get_item(Key={'title': 'its_raining_men'})
            song = songitem["Item"]
        else:
            song = which_song_to_play()            
        print("song: ", song)

        # Using the received latitude and longitude, determine local time
        time_str=time_check(latitude, longitude)
        payload = {'state':{'desired':{'playbackStart': 'True', 'volume': 1.0, 'duration': 5, 'song': {'mark_in': '01', 'song_name': 'Im so excited', 'artist': 'Pointer Sisters', 'title': 'im_so_excited'}, 'url': 'http:\\blah_blah.com'}}}
        payload["state"]["desired"]["song"]=song
        voicemessageurl = create_voice_message(registeredOwner, current_temp, song['song_name'], song['artist'], time_str)
        payload["state"]["desired"]["url"]=voicemessageurl
        json_message = json.dumps(payload)
        print("json_message: ", json_message)
        response = client.update_thing_shadow(thingName = "DiscoMaster2000", payload = json_message)
        print("response: ", response)
        print("return payload: ", payload)
        #To do. Log door opened status to elasticsearch
    else:
        print("The door is closed")
        #To do. Log door closed status to elasticsearch


    return "done"
