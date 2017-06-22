import boto3
import json
import requests
import random
from botocore.exceptions import ClientError
from collections import namedtuple

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
    apiurl="http://api.worldweatheronline.com/premium/v1/weather.ashx?key=4d8f5ad3106b492ba1323348172206&q=%s,%s&num_of_days=1&format=json&localObsTime=yes" % (latitude,longitude)
    print("Making HTTP GET request to the following url: ", apiurl)
    response=requests.get(apiurl)
    try:
        json_weather = response.json()
    except ValueError:
        json_weather = response.text
    print("Full response from worldweatheronline: ", json_weather)
    cor = json_weather["data"]["weather"][0]["hourly"][0]["chanceofrain"]
    ct = json_weather["data"]["current_condition"][0]["temp_C"]
    print("chance_of_rain: ", int(cor))
    print("current_temp: ", int(ct))
    #weather_tuple=namedtuple('weather_tuple', 'chance_of_rain current_temp')
    #results=weather_tuple(chance_of_rain=
    #return int(chance_of_rain)
    #return results
    return {'chance_of_rain':cor, 'current_temp':ct}

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

def create_voice_message(registeredOwner, current_temp, song_title, song_artist):
	voiceMessage="Welcome home " + registeredOwner + ", that was " + song_title + " by " + song_artist + ", the current temperature is " + current_temp + " degrees"
	print("Calling Polly with message: ",voiceMessage)
	pollyVoices = pollyClient.describe_voices(LanguageCode='en-AU')
	print("pollyVoices: ",pollyVoices)
	response=pollyClient.synthesize_speech(Text=voiceMessage, VoiceId='Nicole', OutputFormat='mp3')
	print(response)
	data_stream=response.get("AudioStream")
	print("data_stream: ",data_stream)
	filename = "%s.mp3" % registeredOwner
	print("filename: ",filename)
	S3PollyBucket.put_object(Key=filename, Body=data_stream.read())
	url=s3client.generate_presigned_url(
		ClientMethod='get_object',
		Params={
			'Bucket': 'daas-polly-files',
			'Key': filename
		}
	)
	print("url: ", url)
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
        macAddress=event["state"]["desired"]["macaddress"]
        print("macAddress: ", macAddress)
        registeredOwner=get_registered_owner(macAddress)
        print("Welcome home",registeredOwner)
        latitude=event["state"]["desired"]["latitude"]
        longitude=event["state"]["desired"]["longitude"]
        weather = weather_check(latitude, longitude)
        chance_of_rain=weather['chance_of_rain']
        current_temp=weather['current_temp']
        print("Current temp: ",current_temp)
        chance_of_rain=50
        if chance_of_rain == 100:
            songitem = songListTable.get_item(Key={'title': 'its_raining_men'})
            song = songitem["Item"]
        else:
            song = which_song_to_play()            
        print("song: ", song)
        payload = {'state':{'desired':{'playbackStart': 'True', 'song': {'mark_in': '01', 'song_name': 'Im so excited', 'artist': 'Pointer Sisters', 'title': 'im_so_excited'}, 'url': 'http:\\blah_blah.com'}}}
        payload["state"]["desired"]["song"]=song
        json_message = json.dumps(payload)
        print("json_message: ", json_message)
        response = client.update_thing_shadow(thingName = "DiscoMaster2000", payload = json_message)
        print("response: ", response)
        voicemessageurl = create_voice_message(registeredOwner, current_temp, song['song_name'], song['artist'])
        payload["state"]["desired"]["url"]=voicemessageurl
        print("return payload: ", payload)
    else:
        print("The door is closed")


    return "done"
