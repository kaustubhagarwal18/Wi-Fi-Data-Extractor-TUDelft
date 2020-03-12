"""
This program is a kafka producer program for the 
Historical Client Counts query on the Cisco Prime API.
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
num = 10                               

# query the Cisco prime infrastructure and store the output
output = subprocess.check_output("curl -k -u 'Username:Password' 'https://prime3.tudelft.nl/webacs/api/v3/data/HistoricalClientCounts.json?.maxResults=%d&.full=true' "%num, shell=True)

# load json to make a dict
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
HistCounts = pd.DataFrame()

HistCounts['displayName'] = ""
HistCounts['id']=""
HistCounts['collectionTime'] = ""
HistCounts['key']= ""
HistCounts['subkey']=""
HistCounts['type']=""
HistCounts['count']=""
HistCounts['first']=""
HistCounts['last']=""
HistCounts["Daily_MAC"]="" 
HistCounts["Monthly_MAC"] ="" 
HistCounts["Vendor_MAC"]=""

for index in range(num):

    for i in range(len(flat)):
        if "@count" in flat[0][i]:
            HistCounts.loc[0,'count']= flat[1][i]
        if "@first" in flat[0][i]:
            HistCounts.loc[0,'first']= flat[1][i]
        if "@last" in flat[0][i]:
            HistCounts.loc[0,'last']= flat[1][i]

        if  re.search(".*entity_"+ str(index)+"_", flat[0][i]):

            if "displayName" in flat[0][i]:
                if "historicalClientCountsDTO" in flat[0][i]:
                    HistCounts.loc[index,'displayName'] = flat[1][i]            

        
            if "id" in flat[0][i]:
                if "historicalClientCountsDTO" in flat[0][i]:
                    HistCounts.loc[index,'id'] = flat[1][i]
                 
            if "collectionTime" in flat[0][i]:
                if "historicalClientCountsDTO" in flat[0][i]:
                    HistCounts.loc[index,'collectionTime'] = flat[1][i]

            if "key" in flat[0][i]:
                if "historicalClientCountsDTO_key" in flat[0][i]:
                    HistCounts.loc[index,'key'] = flat[1][i]
                    
            if "subkey" in flat[0][i]:
                if "historicalClientCountsDTO_subkey" in flat[0][i]:
                    HistCounts.loc[index,'subkey'] = flat[1][i]
                    
            if "type" in flat[0][i]:
                if "historicalClientCountsDTO" in flat[0][i]:
                    HistCounts.loc[index,'type']= flat[1][i]

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
     HistCounts.loc[i]['Daily_MAC']=daily_signature(day,HistCounts.loc[i][0])
     HistCounts.loc[i]['Monthly_MAC']=monthly_signature(month,HistCounts.loc[i][0])
     HistCounts['Vendor_MAC'][i] = HistCounts['key'][i][0:8] + ":00:00:00"
#print(HistCounts['Vendor_MAC'])


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
    # publish_message(kafka_producer,'Histcounts_Day','count',HistCounts['count'][0])
     publish_message(kafka_producer,'Histcounts_Month','count',HistCounts['count'][0])
     publish_message(kafka_producer,'Histcounts_Vendor','count',HistCounts['count'][0])
     
     # loop over the dataframe and push data to the respective topics
     for i in range(num):
        publish_message(kafka_producer,'Histcounts_Day','displayName',HistCounts['displayName'][i])
        publish_message(kafka_producer,'Histcounts_Day','id',HistCounts['id'][i])
        publish_message(kafka_producer,'Histcounts_Day','collectionTime',HistCounts['collectionTime'][i])
        publish_message(kafka_producer,'Histcounts_Day','subkey',HistCounts['subkey'][i])
        publish_message(kafka_producer,'Histcounts_Day','type',HistCounts['type'][i])
        publish_message(kafka_producer,'Histcounts_Day','Daily_MAC',HistCounts['Daily_MAC'][i])

        publish_message(kafka_producer,'Histcounts_Month','displayName',HistCounts['displayName'][i])
        publish_message(kafka_producer,'Histcounts_Month','id',HistCounts['id'][i])
        publish_message(kafka_producer,'Histcounts_Month','collectionTime',HistCounts['collectionTime'][i])
        publish_message(kafka_producer,'Histcounts_Month','subkey',HistCounts['subkey'][i])
        publish_message(kafka_producer,'Histcounts_Month','type',HistCounts['type'][i])
        publish_message(kafka_producer,'Histcounts_Month','Daily_MAC',HistCounts['Daily_MAC'][i])

        publish_message(kafka_producer,'Histcounts_Vendor','displayName',HistCounts['displayName'][i])
        publish_message(kafka_producer,'Histcounts_Vendor','id',HistCounts['id'][i])
        publish_message(kafka_producer,'Histcounts_Vendor','collectionTime',HistCounts['collectionTime'][i])
        publish_message(kafka_producer,'Histcounts_Vendor','subkey',HistCounts['subkey'][i])
        publish_message(kafka_producer,'Histcounts_Vendor','type',HistCounts['type'][i])
        publish_message(kafka_producer,'Histcounts_Vendor','Daily_MAC',HistCounts['Daily_MAC'][i])

