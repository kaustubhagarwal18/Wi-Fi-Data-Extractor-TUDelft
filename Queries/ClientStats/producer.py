"""
This program is a kafka producer program for the 
Client Stats query on the Cisco Prime API.
It receives the data, processes it and sends it over
the kafka infrastructure as three separate streams.
The data include entries such as SNR, RSSI etc
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
output = subprocess.check_output("curl -k -u 'Username:Password' 'https://prime3.tudelft.nl/webacs/api/v3/data/ClientStats.json?.maxResults=%d&.full=true' "%num, shell=True) 

# convert the json output into a dict for easy manipulations
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
ClientStats = pd.DataFrame()                              
ClientStats['MAC_add'] = ""     
ClientStats["displayName"]=""
ClientStats['bytesReceived']=""
ClientStats['bytesSent'] = ""
ClientStats['collectionTime']= ""
ClientStats['dataRate']=""
ClientStats['dataRetries']=""
ClientStats['packetsReceived']=""
ClientStats['packetsSent']=""
ClientStats["raPacketsDropped"]=""
ClientStats["rssi"]=""
ClientStats["rtsRetries"]=""
ClientStats["rxBytesDropped"]=""
ClientStats["rxPacketsDropped"]=""
ClientStats["snr"]=""
ClientStats["txBytesDropped"]=""
ClientStats["txPacketsDropped"]=""
ClientStats["Daily_MAC"] =""
ClientStats["Monthly_MAC"] =""
ClientStats["Vendor_MAC"]=""
ClientStats["count"]=""

# loop over all the entries
for index in range(num):

    for i in range(len(flat)):
        if "@count" in flat[0][i]:
            ClientStats.loc[0,'count']= flat[1][i]
        # search for the exact expression to remove the chance of mixing up the data
        if  re.search(".*entity_"+ str(index)+"_", flat[0][i]):
            # find the entry and store it in it's place
            if "macAddress" in flat[0][i]:
                if "clientStatsDTO" in flat[0][i]:
                    ClientStats.loc[index,'MAC_add'] = flat[1][i]

            elif "@displayName" in flat[0][i]:
                if "clientStatsDTO" in flat[0][i]:
                    ClientStats.loc[index,'displayName'] = flat[1][i]
                    
            elif "bytesReceived" in flat[0][i]:
                if "clientStatsDTO" in flat[0][i]:
                    ClientStats.loc[index,'bytesReceived'] = flat[1][i]
                    
            elif "bytesSent" in flat[0][i]:
                if "clientStatsDTO" in flat[0][i]:
                    ClientStats.loc[index,'bytesSent']= flat[1][i]
                    
            elif "collectionTime" in flat[0][i]:
                if "clientStatsDTO" in flat[0][i]:
                    ClientStats.loc[index,'collectionTime']= flat[1][i]
                    
            elif "dataRate" in flat[0][i]:
                if "clientStatsDTO" in flat[0][i]:
                    ClientStats.loc[index,'dataRate']= flat[1][i]
                    
            elif "dataRetries" in flat[0][i]:
                if "clientStatsDTO" in flat[0][i]:
                    ClientStats.loc[index,'dataRetries']= flat[1][i]
                    
            elif "packetsReceived" in flat[0][i]:
                if "clientStatsDTO" in flat[0][i]:
                    ClientStats.loc[index,'packetsReceived']= flat[1][i]
                    
            elif "packetsSent" in flat[0][i]:
                if "clientStatsDTO" in flat[0][i]:
                    ClientStats.loc[index,'packetsSent']= flat[1][i]
                    
            elif "raPacketsDropped" in flat[0][i]:
                if "clientStatsDTO" in flat[0][i]:
                    ClientStats.loc[index,'raPacketsDropped']= flat[1][i]
                    
            elif "rtsRetries" in flat[0][i]:
                if "clientStatsDTO" in flat[0][i]:
                    ClientStats.loc[index,'rtsRetries']= flat[1][i]
                    
            elif "rxBytesDropped" in flat[0][i]:
                if "clientStatsDTO" in flat[0][i]:
                    ClientStats.loc[index,'rxBytesDropped']= flat[1][i]
                    
            elif "rxPacketsDropped" in flat[0][i]:
                 if "clientStatsDTO" in flat[0][i]:
                    ClientStats.loc[index,'rxPacketsDropped']= flat[1][i]
                    
            elif "txBytesDropped" in flat[0][i]:
                 if "clientStatsDTO" in flat[0][i]:
                    ClientStats.loc[index,'txBytesDropped']= flat[1][i]
                    
            elif "txPacketsDropped" in flat[0][i]:
                 if "clientStatsDTO" in flat[0][i]:
                    ClientStats.loc[index,'txPacketsDropped']= flat[1][i]
                    
            elif "rssi" in flat[0][i]:
                if "clientStatsDTO" in flat[0][i]:
                    ClientStats.loc[index,'rssi']= flat[1][i]
            elif "snr" in flat[0][i]:
                if "clientStatsDTO" in flat[0][i]:
                    ClientStats.loc[index,'snr']= flat[1][i]

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
     ClientStats.loc[i]['Daily_MAC']=daily_signature(day,ClientStats.loc[i][0])
     ClientStats.loc[i]['Monthly_MAC']=monthly_signature(month,ClientStats.loc[i][0])
     ClientStats['Vendor_MAC'][i] = ClientStats['MAC_add'][i][0:8] + ":00:00:00"


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
     publish_message(kafka_producer,'count','',ClientStats['count'][0])
     # loop over the ClientStats dataframe and push data to the respective topics
     for i in range(num):
        publish_message(kafka_producer,'ClientStats_Day','Displayname',ClientStats['displayName'][i])
        publish_message(kafka_producer,'ClientStats_Day','bytesReceived',ClientStats['bytesReceived'][i])
        publish_message(kafka_producer,'ClientStats_Day','bytesSent',ClientStats['bytesSent'][i])
        publish_message(kafka_producer,'ClientStats_Day','collectionTime',ClientStats['collectionTime'][i])
        publish_message(kafka_producer,'ClientStats_Day','dataRate',ClientStats['dataRate'][i])
        publish_message(kafka_producer,'ClientStats_Day','dataRetries',ClientStats['dataRetries'][i])
        publish_message(kafka_producer,'ClientStats_Day','packetsReceived',ClientStats['packetsReceived'][i])
        publish_message(kafka_producer,'ClientStats_Day','packetsSent',ClientStats['packetsSent'][i])
        publish_message(kafka_producer,'ClientStats_Day','raPacketsDropped',ClientStats['raPacketsDropped'][i])
        publish_message(kafka_producer,'ClientStats_Day','rssi',ClientStats['rssi'][i])
        publish_message(kafka_producer,'ClientStats_Day','rtsRetries',ClientStats['rtsRetries'][i])
        publish_message(kafka_producer,'ClientStats_Day','rxBytesDropped',ClientStats['rxBytesDropped'][i])
        publish_message(kafka_producer,'ClientStats_Day','rxPacketsDropped',ClientStats['rxPacketsDropped'][i])
        publish_message(kafka_producer,'ClientStats_Day','snr',ClientStats['snr'][i])
        publish_message(kafka_producer,'ClientStats_Day','txBytesDropped',ClientStats['txBytesDropped'][i])
        publish_message(kafka_producer,'ClientStats_Day','txPacketsDropped',ClientStats['txPacketsDropped'][i])
        publish_message(kafka_producer,'ClientStats_Day','MAC',ClientStats['Daily_MAC'][i])

        publish_message(kafka_producer,'ClientStats_Month','Displayname',ClientStats['displayName'][i])
        publish_message(kafka_producer,'ClientStats_Month','bytesReceived',ClientStats['bytesReceived'][i])
        publish_message(kafka_producer,'ClientStats_Month','bytesSent',ClientStats['bytesSent'][i])
        publish_message(kafka_producer,'ClientStats_Month','collectionTime',ClientStats['collectionTime'][i])
        publish_message(kafka_producer,'ClientStats_Month','dataRate',ClientStats['dataRate'][i])
        publish_message(kafka_producer,'ClientStats_Month','dataRetries',ClientStats['dataRetries'][i])
        publish_message(kafka_producer,'ClientStats_Month','packetsReceived',ClientStats['packetsReceived'][i])
        publish_message(kafka_producer,'ClientStats_Month','packetsSent',ClientStats['packetsSent'][i])
        publish_message(kafka_producer,'ClientStats_Month','raPacketsDropped',ClientStats['raPacketsDropped'][i])
        publish_message(kafka_producer,'ClientStats_Month','rssi',ClientStats['rssi'][i])
        publish_message(kafka_producer,'ClientStats_Month','rtsRetries',ClientStats['rtsRetries'][i])
        publish_message(kafka_producer,'ClientStats_Month','rxBytesDropped',ClientStats['rxBytesDropped'][i])
        publish_message(kafka_producer,'ClientStats_Month','rxPacketsDropped',ClientStats['rxPacketsDropped'][i])
        publish_message(kafka_producer,'ClientStats_Month','snr',ClientStats['snr'][i])
        publish_message(kafka_producer,'ClientStats_Month','txBytesDropped',ClientStats['txBytesDropped'][i])
        publish_message(kafka_producer,'ClientStats_Month','txPacketsDropped',ClientStats['txPacketsDropped'][i])
        publish_message(kafka_producer,'ClientStats_Month','MAC',ClientStats['Monthly_MAC'][i])

        publish_message(kafka_producer,'ClientStats_Vendor','Displayname',ClientStats['displayName'][i])
        publish_message(kafka_producer,'ClientStats_Vendor','bytesReceived',ClientStats['bytesReceived'][i])
        publish_message(kafka_producer,'ClientStats_Vendor','bytesSent',ClientStats['bytesSent'][i])
        publish_message(kafka_producer,'ClientStats_Vendor','collectionTime',ClientStats['collectionTime'][i])
        publish_message(kafka_producer,'ClientStats_Vendor','dataRate',ClientStats['dataRate'][i])
        publish_message(kafka_producer,'ClientStats_Vendor','dataRetries',ClientStats['dataRetries'][i])
        publish_message(kafka_producer,'ClientStats_Vendor','packetsReceived',ClientStats['packetsReceived'][i])
        publish_message(kafka_producer,'ClientStats_Vendor','packetsSent',ClientStats['packetsSent'][i])
        publish_message(kafka_producer,'ClientStats_Vendor','raPacketsDropped',ClientStats['raPacketsDropped'][i])
        publish_message(kafka_producer,'ClientStats_Vendor','rssi',ClientStats['rssi'][i])
        publish_message(kafka_producer,'ClientStats_Vendor','rtsRetries',ClientStats['rtsRetries'][i])
        publish_message(kafka_producer,'ClientStats_Vendor','rxBytesDropped',ClientStats['rxBytesDropped'][i])
        publish_message(kafka_producer,'ClientStats_Vendor','rxPacketsDropped',ClientStats['rxPacketsDropped'][i])
        publish_message(kafka_producer,'ClientStats_Vendor','snr',ClientStats['snr'][i])
        publish_message(kafka_producer,'ClientStats_Vendor','txBytesDropped',ClientStats['txBytesDropped'][i])
        publish_message(kafka_producer,'ClientStats_Vendor','txPacketsDropped',ClientStats['txPacketsDropped'][i])
        publish_message(kafka_producer,'ClientStats_Vendor','MAC',ClientStats['Vendor_MAC'][i])


