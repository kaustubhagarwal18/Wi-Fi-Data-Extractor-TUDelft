from kafka import KafkaConsumer
import json
import unicodecsv as  csv
from ast import literal_eval
import os.path
from os import path

# Connect to a particular topic and decode the messages

consumer = KafkaConsumer('ClientSessions_Day',bootstrap_servers=['0.0.0.0:9092'],value_deserializer=lambda v: json.loads(v).decode('utf-8'))
if (path.exists("ClientSessions_day.csv")) == False:
    f = open("ClientSessions_day.csv","a")
    fWriter = csv.writer(f)
    #f.write('\n')
# write the header of the csv
    fWriter.writerow(['DisplayName','authenticationAlgorithm','bytesReceived','bytesSent','clientInterface','collectionTime','deviceName','eapType','ipType','location','packetsReceived','packetsSent','policyTypeStatus','portSpeed','postureStatus','profileName','protocol','rssi','securityPolicy','sessionEndTime','sessionStartTime','snr','ssid','throughput','vlan','webSecurity','wgbStatus','MAC','IP'])
    f.close()

# flag for spacing
a = 1
flag = False
f = open("ClientSessions_day.csv","a")
f.write('\n')
f.close()
# loop for messages
for msg in consumer:
    
    
    f = open("ClientSessions_day.csv","a")
    # new line after 29 entries
    if (a-1) % 29 == 0 and flag == True:
        f.write('\n')
    f = open("ClientSessions_day.csv", "a")
    f.write(str(literal_eval(msg.value)))
    f.write('\t')
    a = a+1
    f.close()
    flag = True
