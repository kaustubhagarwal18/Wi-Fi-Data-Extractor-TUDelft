"""
This program is a kafka producer program for the 
Client Sessions query on the Cisco Prime API.
It receives the data, processes it and sends it over
the kafka infrastructure as three separate streams.
The data include entries such as vlanid, ipType,protocol etc
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
output = subprocess.check_output("curl -k -u 'Username:Password' 'https://prime3.tudelft.nl/webacs/api/v3/data/ClientSessions.json?.maxResults=%d&.full=true' "%num, shell=True) 

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
ClientSessions = pd.DataFrame()                
ClientSessions['MAC_add'] = ""     
ClientSessions["ipAddress"]=""
ClientSessions['Displayname']=""
ClientSessions['authenticationAlgorithm'] = ""
ClientSessions['bytesReceived']= ""
ClientSessions['bytesSent']=""
ClientSessions['clientInterface']=""
ClientSessions['connectionType']=""
ClientSessions['deviceName']=""
ClientSessions["eapType"]=""
ClientSessions["encryptionCypher"]=""
ClientSessions["ipType"]=""
ClientSessions["location"]=""
ClientSessions["packetsReceived"]=""
ClientSessions["packetsSent"]=""
ClientSessions["policyTypeStatus"]=""
ClientSessions["portSpeed"]=""
ClientSessions["postureStatus"]=""
ClientSessions["profileName"]=""
ClientSessions["protocol"]=""
ClientSessions["rssi"]=""
ClientSessions["securityPolicy"]=""
ClientSessions["sessionEndTime"]=""
ClientSessions["sessionStartTime"]=""
ClientSessions["snr"]=""
ClientSessions["ssid"]=""
ClientSessions["throughput"]=""
ClientSessions["vlan"]=""
ClientSessions["webSecurity"]=""
ClientSessions["wgbStatus"]=""
ClientSessions["Daily_MAC"] =""
ClientSessions["Monthly_MAC"] =""
ClientSessions["Vendor_MAC"]=""
ClientSessions["Daily_IP"] =""
ClientSessions["Monthly_IP"] =""
ClientSessions["count"]=""

for index in range(num):

    for i in range(len(flat)):
        if "@count" in flat[0][i]:
            ClientSessions.loc[0,'count']= flat[1][i]

        if  re.search(".*entity_"+ str(index)+"_", flat[0][i]):

            if "macAddress" in flat[0][i]:
                if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'MAC_add'] = flat[1][i]

            if "_ipAddress" in flat[0][i]:
                if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'ipAddress'] = flat[1][i]
                    
            elif "@displayName" in flat[0][i]:
                if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'Displayname'] = flat[1][i]

            elif "authenticationAlgorithm" in flat[0][i]:
                if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'authenticationAlgorithm'] = flat[1][i]
                    
            elif "bytesReceived" in flat[0][i]:
                if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'bytesReceived'] = flat[1][i]
                    
            elif "bytesSent" in flat[0][i]:
                if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'bytesSent']= flat[1][i]
                    
            elif "clientInterface" in flat[0][i]:
                if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'clientInterface']= flat[1][i]
                    
            elif "connectionType" in flat[0][i]:
                if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'connectionType']= flat[1][i]
                    
            elif "deviceName" in flat[0][i]:
                if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'deviceName']= flat[1][i]
                    
            elif "eapType" in flat[0][i]:
                if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'eapType']= flat[1][i]
                    
            elif "encryptionCypher" in flat[0][i]:
                if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'encryptionCypher']= flat[1][i]
                    
            elif "ipType" in flat[0][i]:
                if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'ipType']= flat[1][i]
                    
            elif "location" in flat[0][i]:
                if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'location']= flat[1][i]
                    
            elif "packetsReceived" in flat[0][i]:
                if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'packetsReceived']= flat[1][i]
                    
            elif "packetsSent" in flat[0][i]:
                 if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'packetsSent']= flat[1][i]
                    
            elif "policyTypeStatus" in flat[0][i]:
                 if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'policyTypeStatus']= flat[1][i]
                    
            elif "portSpeed" in flat[0][i]:
                 if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'portSpeed']= flat[1][i]
                    
            elif "postureStatus" in flat[0][i]:
                 if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'postureStatus']= flat[1][i]
                    
            elif "profileName" in flat[0][i]:
                 if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'profileName']= flat[1][i]
                    
            elif "protocol" in flat[0][i]:
                 if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'protocol']= flat[1][i]
                    
            elif "rssi" in flat[0][i]:
                if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'rssi']= flat[1][i]
                    
            elif "securityPolicy" in flat[0][i]:
                if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'securityPolicy']= flat[1][i]
     
            elif "sessionEndTime" in flat[0][i]:
                 if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'sessionEndTime']= flat[1][i]
                    
            elif "sessionStartTime" in flat[0][i]:
                 if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'sessionStartTime']= flat[1][i]
                    
            elif "snr" in flat[0][i]:
                if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'snr']= flat[1][i]
                    
            elif "ssid" in flat[0][i]:
                if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'ssid']= flat[1][i]

            elif "throughput" in flat[0][i]:
                 if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'throughput']= flat[1][i]
                    
            elif "vlan" in flat[0][i]:
                 if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'vlan']= flat[1][i]
                    
            elif "webSecurity" in flat[0][i]:
                if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'webSecurity']= flat[1][i]
                    
            elif "wgbStatus" in flat[0][i]:
                if "clientSessionsDTO" in flat[0][i]:
                    ClientSessions.loc[index,'wgbStatus']= flat[1][i]

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
     ClientSessions.loc[i]['Daily_MAC']=daily_signature(day,ClientSessions.loc[i][0])
     ClientSessions.loc[i]['Monthly_MAC']=monthly_signature(month,ClientSessions.loc[i][0])
     ClientSessions.loc[i]['Daily_IP']=daily_signature(day,ClientSessions.loc[i][1])
     ClientSessions.loc[i]['Monthly_IP']=monthly_signature(month,ClientSessions.loc[i][1])
     ClientSessions['Vendor_MAC'][i] = ClientSessions['MAC_add'][i][0:8] + ":00:00:00"




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
     publish_message(kafka_producer,'count','',ClientSessions['count'][0])
     # loop over the ClientStats dataframe and push data to the respective topics
     for i in range(num):
        publish_message(kafka_producer,'ClientSessions_Day','Displayname',ClientSessions['Displayname'][i])
        publish_message(kafka_producer,'ClientSessions_Day','authenticationAlgorithm',ClientSessions['authenticationAlgorithm'][i])
        publish_message(kafka_producer,'ClientSessions_Day','bytesReceived',ClientSessions['bytesReceived'][i])
        publish_message(kafka_producer,'ClientSessions_Day','bytesSent',ClientSessions['bytesSent'][i])
        publish_message(kafka_producer,'ClientSessions_Day','clientInterface',ClientSessions['clientInterface'][i])
        publish_message(kafka_producer,'ClientSessions_Day','connectionType',ClientSessions['connectionType'][i])
        publish_message(kafka_producer,'ClientSessions_Day','deviceName',ClientSessions['deviceName'][i])
        publish_message(kafka_producer,'ClientSessions_Day','eapType',ClientSessions['eapType'][i])
        publish_message(kafka_producer,'ClientSessions_Day','ipType',ClientSessions['ipType'][i])
        publish_message(kafka_producer,'ClientSessions_Day','location',ClientSessions['location'][i])
        publish_message(kafka_producer,'ClientSessions_Day','packetsReceived',ClientSessions['packetsReceived'][i])
        publish_message(kafka_producer,'ClientSessions_Day','packetsSent',ClientSessions['packetsSent'][i])
        publish_message(kafka_producer,'ClientSessions_Day','policyTypeStatus',ClientSessions['policyTypeStatus'][i])
        publish_message(kafka_producer,'ClientSessions_Day','portSpeed',ClientSessions['portSpeed'][i])
        publish_message(kafka_producer,'ClientSessions_Day','postureStatus',ClientSessions['postureStatus'][i])
        publish_message(kafka_producer,'ClientSessions_Day','profileName',ClientSessions['profileName'][i])
        publish_message(kafka_producer,'ClientSessions_Day','protocol',ClientSessions['protocol'][i])
        publish_message(kafka_producer,'ClientSessions_Day','rssi',ClientSessions['rssi'][i])
        publish_message(kafka_producer,'ClientSessions_Day','securityPolicy',ClientSessions['securityPolicy'][i])
        publish_message(kafka_producer,'ClientSessions_Day','sessionEndTime',ClientSessions['sessionEndTime'][i])
        publish_message(kafka_producer,'ClientSessions_Day','sessionStartTime',ClientSessions['sessionStartTime'][i])
        publish_message(kafka_producer,'ClientSessions_Day','snr',ClientSessions['snr'][i])
        publish_message(kafka_producer,'ClientSessions_Day','ssid',ClientSessions['ssid'][i])
        publish_message(kafka_producer,'ClientSessions_Day','throughput',ClientSessions['throughput'][i])
        publish_message(kafka_producer,'ClientSessions_Day','vlan',ClientSessions['vlan'][i])
        publish_message(kafka_producer,'ClientSessions_Day','webSecurity',ClientSessions['webSecurity'][i])
        publish_message(kafka_producer,'ClientSessions_Day','wgbStatus',ClientSessions['wgbStatus'][i])
        publish_message(kafka_producer,'ClientSessions_Day','MAC',ClientSessions['Daily_MAC'][i])
        publish_message(kafka_producer,'ClientSessions_Day','IP',ClientSessions['Daily_IP'][i])

        publish_message(kafka_producer,'ClientSessions_Month','Displayname',ClientSessions['Displayname'][i])
        publish_message(kafka_producer,'ClientSessions_Month','authenticationAlgorithm',ClientSessions['authenticationAlgorithm'][i])
        publish_message(kafka_producer,'ClientSessions_Month','bytesReceived',ClientSessions['bytesReceived'][i])
        publish_message(kafka_producer,'ClientSessions_Month','bytesSent',ClientSessions['bytesSent'][i])
        publish_message(kafka_producer,'ClientSessions_Month','clientInterface',ClientSessions['clientInterface'][i])
        publish_message(kafka_producer,'ClientSessions_Month','connectionType',ClientSessions['connectionType'][i])
        publish_message(kafka_producer,'ClientSessions_Month','deviceName',ClientSessions['deviceName'][i])
        publish_message(kafka_producer,'ClientSessions_Month','eapType',ClientSessions['eapType'][i])
        publish_message(kafka_producer,'ClientSessions_Month','ipType',ClientSessions['ipType'][i])
        publish_message(kafka_producer,'ClientSessions_Month','location',ClientSessions['location'][i])
        publish_message(kafka_producer,'ClientSessions_Month','packetsReceived',ClientSessions['packetsReceived'][i])
        publish_message(kafka_producer,'ClientSessions_Month','packetsSent',ClientSessions['packetsSent'][i])
        publish_message(kafka_producer,'ClientSessions_Month','policyTypeStatus',ClientSessions['policyTypeStatus'][i])
        publish_message(kafka_producer,'ClientSessions_Month','portSpeed',ClientSessions['portSpeed'][i])
        publish_message(kafka_producer,'ClientSessions_Month','postureStatus',ClientSessions['postureStatus'][i])
        publish_message(kafka_producer,'ClientSessions_Month','profileName',ClientSessions['profileName'][i])
        publish_message(kafka_producer,'ClientSessions_Month','protocol',ClientSessions['protocol'][i])
        publish_message(kafka_producer,'ClientSessions_Month','rssi',ClientSessions['rssi'][i])
        publish_message(kafka_producer,'ClientSessions_Month','securityPolicy',ClientSessions['securityPolicy'][i])
        publish_message(kafka_producer,'ClientSessions_Month','sessionEndTime',ClientSessions['sessionEndTime'][i])
        publish_message(kafka_producer,'ClientSessions_Month','sessionStartTime',ClientSessions['sessionStartTime'][i])
        publish_message(kafka_producer,'ClientSessions_Month','snr',ClientSessions['snr'][i])
        publish_message(kafka_producer,'ClientSessions_Month','ssid',ClientSessions['ssid'][i])
        publish_message(kafka_producer,'ClientSessions_Month','throughput',ClientSessions['throughput'][i])
        publish_message(kafka_producer,'ClientSessions_Month','vlan',ClientSessions['vlan'][i])
        publish_message(kafka_producer,'ClientSessions_Month','webSecurity',ClientSessions['webSecurity'][i])
        publish_message(kafka_producer,'ClientSessions_Month','wgbStatus',ClientSessions['wgbStatus'][i])
        publish_message(kafka_producer,'ClientSessions_Month','MAC',ClientSessions['Monthly_MAC'][i])
        publish_message(kafka_producer,'ClientSessions_Month','IP',ClientSessions['Monthly_IP'][i])

        publish_message(kafka_producer,'ClientSessions_Vendor','Displayname',ClientSessions['Displayname'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','authenticationAlgorithm',ClientSessions['authenticationAlgorithm'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','bytesReceived',ClientSessions['bytesReceived'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','bytesSent',ClientSessions['bytesSent'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','clientInterface',ClientSessions['clientInterface'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','connectionType',ClientSessions['connectionType'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','deviceName',ClientSessions['deviceName'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','eapType',ClientSessions['eapType'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','ipType',ClientSessions['ipType'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','location',ClientSessions['location'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','packetsReceived',ClientSessions['packetsReceived'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','packetsSent',ClientSessions['packetsSent'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','policyTypeStatus',ClientSessions['policyTypeStatus'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','portSpeed',ClientSessions['portSpeed'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','postureStatus',ClientSessions['postureStatus'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','profileName',ClientSessions['profileName'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','protocol',ClientSessions['protocol'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','rssi',ClientSessions['rssi'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','securityPolicy',ClientSessions['securityPolicy'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','sessionEndTime',ClientSessions['sessionEndTime'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','sessionStartTime',ClientSessions['sessionStartTime'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','snr',ClientSessions['snr'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','ssid',ClientSessions['ssid'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','throughput',ClientSessions['throughput'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','vlan',ClientSessions['vlan'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','webSecurity',ClientSessions['webSecurity'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','wgbStatus',ClientSessions['wgbStatus'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','MAC',ClientSessions['Vendor_MAC'][i])
        publish_message(kafka_producer,'ClientSessions_Vendor','IP',ClientSessions['Monthly_IP'][i])

