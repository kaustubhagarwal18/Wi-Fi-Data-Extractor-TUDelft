from kafka import KafkaConsumer
import json
import unicodecsv as  csv
from ast import literal_eval
import pandas as pd
import os.path
from os import path
# Connect to a particular topic and decode the messages

consumer = KafkaConsumer('Histcounts_Day',bootstrap_servers=['0.0.0.0:9092'],value_deserializer=lambda v: json.loads(v).decode('utf-8'))
if (path.exists("histclientcounts_day.csv")) == False:
    f = open("histclientcounts_day.csv","a")
    fWriter = csv.writer(f)
    #f.write('\n')
# write the header of the csv
    fWriter.writerow(['DisplayName','id','CollectionTime','Subkey','type','MAC'])
    f.close()

# flag for spacing
a = 1
flag = False
f = open("histclientcounts_day.csv","a")
f.write('\n')
f.close()
# loop for messages
for msg in consumer:
    
    
    f = open("histclientcounts_day.csv","a")
    # new line after 6 entries
    if (a-1) % 6 == 0 and flag == True:
        f.write('\n')
    f = open("histclientcounts_day.csv", "a")
    f.write(str(literal_eval(msg.value)))
    f.write('\t')
    a = a+1
    f.close()
    flag = True
