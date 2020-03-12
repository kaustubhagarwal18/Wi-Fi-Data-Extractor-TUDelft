"""
This program is a kafka producer program for the 
Client Details query on the Cisco Prime API.
It receives the data, processes it and sends it over
the kafka infrastructure as three separate streams.
"""

import subprocess
from flatten_json import flatten
from pandas.io.json import json_normalize
import pandas as pd
import json
import re
from kafka import KafkaProducer
import hmac
import hashlib
from datetime import date
import binascii
# number of entries
num = 20           

# query the Cisco prime infrastructure and store the output
output = subprocess.check_output("curl -k -u 'Username:Password' 'https://prime3.tudelft.nl/webacs/api/v3/data/ClientDetails.json?.maxResults=%d&.full=true' "%num, shell=True) 

output =json.loads(output)
#this function take a nested json and flattens it
def flatten_json(y):         
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

# Store the output and normalize it
flat = flatten_json(output)
json_normalize(flat)

# Convert the flattened json to a pandas dataframe
flat= pd.DataFrame(list(flat.items()))           

# Create a dataframe to store the sorted values
ClientDetails = pd.DataFrame() 
ClientDetails['MAC_add'] = ""
ClientDetails['Deviceipaddress']=""
ClientDetails['Device type'] = ""
ClientDetails['Displayname']= ""
ClientDetails['count']=""
ClientDetails['first']=""
ClientDetails['last']=""
ClientDetails['Firstseentime']=""
ClientDetails["APname"]=""
ClientDetails["MobilityStatus"]=""
ClientDetails["Location"]=""
ClientDetails["ssid"]=""
ClientDetails["clientDetailsDTO_status"]=""
ClientDetails["apIpAddress"]=""
ClientDetails["updateTime"]=""
ClientDetails["vendor"]=""
ClientDetails["vlan"]=""
ClientDetails["protocol"]=""
ClientDetails["encryptionCypher"]=""
ClientDetails["deviceName"]=""
ClientDetails["wgbStatus"]=""
ClientDetails["authenticationAlgorithm"]=""
ClientDetails["clientInterface"]=""
ClientDetails["connectionType"]=""
ClientDetails["clientDetailsDTO_ipType"]=""
ClientDetails["Daily_MAC"]=""
ClientDetails["Monthly_MAC"] =""
ClientDetails["Vendor_MAC"]=""

for index in range(num):

    for i in range(len(flat)):
        if "@count" in flat[0][i]:
            ClientDetails.loc[0,'count']= flat[1][i]

        if  re.search(".*entity_"+ str(index)+"_", flat[0][i]):

            if "mac" in flat[0][i]:
                if "entity_" in flat[0][i]:

                    ClientDetails.loc[index,'MAC_add'] = flat[1][i]
                    
            elif "apName" in flat[0][i]:
                if "clientDetailsDTO" in flat[0][i]:
                    ClientDetails.loc[index,'APname'] = flat[1][i]
                 
            elif "deviceIpAddress" in flat[0][i]:
                if "clientDetailsDTO" in flat[0][i]:
                    ClientDetails.loc[index,'Deviceipaddress'] = flat[1][i]

            elif "apIpAddress_address" in flat[0][i]:
                if "clientDetailsDTO" in flat[0][i]:
                    ClientDetails.loc[index,'apIpAddress'] = flat[1][i]
                    
            elif "deviceType" in flat[0][i]:
                if "clientDetailsDTO_deviceType" in flat[0][i]:
                    ClientDetails.loc[index,'Device type'] = flat[1][i]
                    
            elif "@displayName" in flat[0][i]:
                if "clientDetailsDTO" in flat[0][i]:
                    ClientDetails.loc[index,'Displayname']= flat[1][i]
                    
            elif "firstSeenTime" in flat[0][i]:
                if "clientDetailsDTO" in flat[0][i]:
                    ClientDetails.loc[index,'Firstseentime']= flat[1][i]
                    
            elif "mobilityStatus" in flat[0][i]:
                if "clientDetailsDTO" in flat[0][i]:
                    ClientDetails.loc[index,'MobilityStatus']= flat[1][i]
                    
            elif "location" in flat[0][i]:
                if "clientDetailsDTO" in flat[0][i]:
                    ClientDetails.loc[index,'Location']= flat[1][i]
                    
            elif "ssid" in flat[0][i]:
                if "clientDetailsDTO" in flat[0][i]:
                    ClientDetails.loc[index,'ssid']= flat[1][i]
                    
            elif "clientDetailsDTO_status" in flat[0][i]:
                if "clientDetailsDTO" in flat[0][i]:
                    ClientDetails.loc[index,'clientDetailsDTO_status']= flat[1][i]
                    
            elif "updateTime" in flat[0][i]:
                if "clientDetailsDTO" in flat[0][i]:
                    ClientDetails.loc[index,'updateTime']= flat[1][i]
                    
            elif "vendor" in flat[0][i]:
                if "clientDetailsDTO" in flat[0][i]:
                    ClientDetails.loc[index,'vendor']= flat[1][i]
                    
            elif "clientDetailsDTO_vlan" in flat[0][i]:
                if "clientDetailsDTO" in flat[0][i]:
                    ClientDetails.loc[index,'vlan']= flat[1][i]
                    
            elif "protocol" in flat[0][i]:
                 if "clientDetailsDTO" in flat[0][i]:
                    ClientDetails.loc[index,'protocol']= flat[1][i]
                    
            elif "encryptionCypher" in flat[0][i]:
                 if "clientDetailsDTO" in flat[0][i]:
                    ClientDetails.loc[index,'encryptionCypher']= flat[1][i]
                    
            elif "deviceName" in flat[0][i]:
                 if "clientDetailsDTO" in flat[0][i]:
                    ClientDetails.loc[index,'deviceName']= flat[1][i]
                    
            elif "wgbStatus" in flat[0][i]:
                 if "clientDetailsDTO" in flat[0][i]:
                    ClientDetails.loc[index,'wgbStatus']= flat[1][i]
                    
            elif "authenticationAlgorithm" in flat[0][i]:
                 if "clientDetailsDTO" in flat[0][i]:
                    ClientDetails.loc[index,'authenticationAlgorithm']= flat[1][i]
                    
            elif "clientInterface" in flat[0][i]:
                 if "clientDetailsDTO" in flat[0][i]:
                    ClientDetails.loc[index,'clientInterface']= flat[1][i]
                    
            elif "connectionType" in flat[0][i]:
                if "clientDetailsDTO" in flat[0][i]:
                    ClientDetails.loc[index,'connectionType']= flat[1][i]
                    
            elif "clientDetailsDTO_ipType" in flat[0][i]:
                if "clientDetailsDTO" in flat[0][i]:
                    ClientDetails.loc[index,'clientDetailsDTO_ipType']= flat[1][i]



# Use date and month as to change keys
today = date.today()
# get today's date 
d1 = today.strftime("%d/%m/%Y")
# remove / and space to get the date as int
day = d1.replace('/', '')
# get the month and year
month = day[2:8]


def daily_signature(key, message):
    """
    This function takes a key and message as input
    the key is the date which changes daily and message
    is the data to be hashed.
    It returns a hashed object for that message
    """
    byte_key = binascii.unhexlify(key)
    message = message.encode()
    return hmac.new(byte_key, message, hashlib.sha256).hexdigest().upper()

def monthly_signature(key, message):
    """
    Similar to the daily_signatue function. 
    The key(month) changes every first day of the month
    """
    byte_key = binascii.unhexlify(key)
    message = message.encode()
    return hmac.new(byte_key, message, hashlib.sha256).hexdigest().upper()

# Fill the hashed entries and Vendor details
for i in range(num):
     ClientDetails.loc[i]['Daily_MAC']=daily_signature(day,ClientDetails.loc[i][0])
     ClientDetails.loc[i]['Monthly_MAC']=monthly_signature(month,ClientDetails.loc[i][0])
     ClientDetails['Vendor_MAC'][i] = ClientDetails['MAC_add'][i][0:8] + ":00:00:00"

def connect_kafka_producer():
    """
    this function returns a kafka producer instance which connects
    to the broker
    """
    producer = KafkaProducer(bootstrap_servers='0.0.0.0:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    return producer


def publish_message(producer_instance, topic_name, key, value):
    """
    this function takes the producer , topic name , column name and the value
    to push to kafka on that particular topic
    """
    key_serializer = repr(key).encode()
    value_serializer = repr(value).encode()

    producer_instance.send(topic_name, key=key_serializer, value=value_serializer)
    producer_instance.flush()
    print('Message published successfully.')

if __name__ == '__main__':
     kafka_producer=connect_kafka_producer() 
     
     # publish count as a single entry
     publish_message(kafka_producer,'count','',ClientDetails['count'][0])

     # loop over the dataframe and push data to the respective topics
     for i in range(num):
        publish_message(kafka_producer,'ClientDetails_Day','Devicetype',ClientDetails['Device type'][i])
        publish_message(kafka_producer,'ClientDetails_Day','Displayname',ClientDetails['Displayname'][i])
        publish_message(kafka_producer,'ClientDetails_Day','Vendor',ClientDetails['vendor'][i])
        publish_message(kafka_producer,'ClientDetails_Day','VLAN',ClientDetails['vlan'][i])
        publish_message(kafka_producer,'ClientDetails_Day','Protocol',ClientDetails['protocol'][i])
        publish_message(kafka_producer,'ClientDetails_Day','Clientinterface',ClientDetails['clientInterface'][i])
        publish_message(kafka_producer,'ClientDetails_Day','Connectiontype',ClientDetails['connectionType'][i])
        publish_message(kafka_producer,'ClientDetails_Day','APname',ClientDetails['APname'][i])
        publish_message(kafka_producer,'ClientDetails_Day','APip',ClientDetails['apIpAddress'][i])
        publish_message(kafka_producer,'ClientDetails_Day','Location',ClientDetails['Location'][i])
        publish_message(kafka_producer,'ClientDetails_Day','SSID',ClientDetails['ssid'][i])
        publish_message(kafka_producer,'ClientDetails_Day','MAC',ClientDetails['Daily_MAC'][i])    
        
        publish_message(kafka_producer,'ClientDetails_Month','Devicetype',ClientDetails['Device type'][i])
        publish_message(kafka_producer,'ClientDetails_Month','Displayname',ClientDetails['Displayname'][i])
        publish_message(kafka_producer,'ClientDetails_Month','Vendor',ClientDetails['vendor'][i])
        publish_message(kafka_producer,'ClientDetails_Month','VLAN',ClientDetails['vlan'][i])
        publish_message(kafka_producer,'ClientDetails_Month','Protocol',ClientDetails['protocol'][i])
        publish_message(kafka_producer,'ClientDetails_Month','Clientinterface',ClientDetails['clientInterface'][i])
        publish_message(kafka_producer,'ClientDetails_Month','Connectiontype',ClientDetails['connectionType'][i])
        publish_message(kafka_producer,'ClientDetails_Month','APname',ClientDetails['APname'][i])
        publish_message(kafka_producer,'ClientDetails_Month','APip',ClientDetails['apIpAddress'][i])
        publish_message(kafka_producer,'ClientDetails_Month','Location',ClientDetails['Location'][i])
        publish_message(kafka_producer,'ClientDetails_Month','SSID',ClientDetails['ssid'][i])
        publish_message(kafka_producer,'ClientDetails_Month','MAC',ClientDetails['Monthly_MAC'][i])
        
        publish_message(kafka_producer,'ClientDetails_Vendor','Devicetype',ClientDetails['Device type'][i])
        publish_message(kafka_producer,'ClientDetails_Vendor','Displayname',ClientDetails['Displayname'][i])
        publish_message(kafka_producer,'ClientDetails_Vendor','Vendor',ClientDetails['vendor'][i])
        publish_message(kafka_producer,'ClientDetails_Vendor','VLAN',ClientDetails['vlan'][i])
        publish_message(kafka_producer,'ClientDetails_Vendor','Protocol',ClientDetails['protocol'][i])
        publish_message(kafka_producer,'ClientDetails_Vendor','Clientinterface',ClientDetails['clientInterface'][i])
        publish_message(kafka_producer,'ClientDetails_Vendor','Connectiontype',ClientDetails['connectionType'][i])
        publish_message(kafka_producer,'ClientDetails_Vendor','APname',ClientDetails['APname'][i])
        publish_message(kafka_producer,'ClientDetails_Vendor','APip',ClientDetails['apIpAddress'][i])
        publish_message(kafka_producer,'ClientDetails_Vendor','Location',ClientDetails['Location'][i])
        publish_message(kafka_producer,'ClientDetails_Vendor','SSID',ClientDetails['ssid'][i])        
        publish_message(kafka_producer,'ClientDetails_Vendor','MAC',ClientDetails['Vendor_MAC'][i])




        
