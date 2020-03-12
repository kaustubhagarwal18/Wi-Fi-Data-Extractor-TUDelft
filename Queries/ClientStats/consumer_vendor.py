from kafka import KafkaConsumer
import json
import unicodecsv as  csv
from ast import literal_eval
import os.path
from os import path

# Connect to a particular topic and decode the messages

consumer = KafkaConsumer('ClientStats_Vendor',bootstrap_servers=['0.0.0.0:9092'],value_deserializer=lambda v: json.loads(v).decode('utf-8'))
if (path.exists("ClientStats_vendor.csv")) == False:
    f = open("ClientStats_vendor.csv","a")
    fWriter = csv.writer(f)
    #f.write('\n')
# write the header of the csv
    fWriter.writerow(['DisplayName','bytesReceived','bytesSent','collectionTime','dataRate','dataRetries','packetsReceived','packetsSent','raPacketsDropped','rssi','rtsRetries','rxBytesDropped','rxPacketsDropped','snr','txBytesDropped','txPacketsDropped','MAC'])
    f.close()

# flag for spacing
a = 1
flag = False
f = open("ClientStats_vendor.csv","a")
f.write('\n')
f.close()
# loop for messages
for msg in consumer:
    
    
    f = open("ClientStats_vendor.csv","a")
    # new line after 17 entries
    if (a-1) % 17 == 0 and flag == True:
        f.write('\n')
    f = open("ClientStats_vendor.csv", "a")
    f.write(str(literal_eval(msg.value)))
    f.write('\t')
    a = a+1
    f.close()
    flag = True
