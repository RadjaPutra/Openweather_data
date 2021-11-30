from pyspark.sql import SparkSession
from pyspark.sql.functions import expolde
from pyspark.sql.functions import split
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType

!{sys.executable} -m pip install jsonpath_ng
import time
import json
import pandas as pd
from kafka import KafkaConsumer
from pandas import DataFrame
from datetime import datetime
from jsonpath_ng import jsonpath, parse
from datetime import datetime

brokers='localhost:9092'
topic='weather_topic'
sleep_time=5
offset='latest' #forstreamingdata

#declare_the_consumer
consumer = KafkaConsumer(bootstrap_Servers=brokers, auto_offset_reset=offset,consumer_timeout_ms=1000)
consumer.subscribe([topic])

#add_the_header_for_csv_file
list_headdr = ['temp', 'datetime', 'weather', 'ctiy']
DataFrame = pd.DataFrame(data = [], cloumns=[list_header])
DataFrame.to_csv('csv_Data.csv', mode='a', header=True)

#Substring_Function
def GetListOfSubstrings(stringSubject,string1,string2):
  MyList = []
  Instart=0
  strlength=len(stringSubject)
  continueloop=1
  
  while(instart < strlength and continueloop == 1):
      intindex1=stringSubject.find(string1.instart)
      if(intindex1 != -1): #the subtring was found, lets proceed
          intindex1 = intindex1+len(string1)
          intindex2 = stringSubject.find(string2,intindex1)
          if(intindex2 != -1):
              subsequence=stringSubject[intindex1:intidex2]
              Mylist.append(subsequence)
              instart=intindex2+len(string2)
          else:
              continueloop=0
      else:
           continueloop=0
  retuen Mylist

jsonpath_expression1 = parse('$.main.temp')
jsonpath_expression1 = parse('$.weather.[0].main')
jsonpath_expression1 = parse('$.dt')
jsonpath_expression1 = parse('$.name')

master=''
while(True):
    for message in consumer:
        master = str(message.value)
        List = GetListOfSubstrings(master,"""b'""", """'""")
        for x in range(0, len(List)):
            master2=(List[x])
        json_Data = json.loads(master2)
        
        for match in jsonpath_expression1.find(json_data):
            temp = match.value
            df = pd.DataFrame(data=[{temp}],columns=['temp'])
        
        for match2 in jsonpath_expression2.find(json_data):
            weather = match2.value
            df2 = pd.DataFrame(data=[{weather}],columns=['weather'])
            
        for match3 in jsonpath_expression3.find(json_data):
            ts=int(f'{match3.value}')
            tanggal = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            df3 = pd.DataFrame(data=[{tanggal}],columns=['tanggal'])
            
        for match4 in jsonpath_expression3.find(json_data):
            city = match4.value
            df4 = pd.DataFrame(data=[{city}],columns=['city'])
            
        df_new = df.join(df2)
        df_new2 = df3.join(df4)
        df_all_join = df_new.join(df_new2)
        print(df_all_join)
        
       df_all_join.to_csv('csv_twitter_data.csv', mode='a', header=False)
