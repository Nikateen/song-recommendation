#!/usr/bin/python3

import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from random import sample
from pyspark.sql.types import StructType,StructField, StringType
from random import sample


topSongsNum_arg = None
randomize_arg = None
randomize_count_arg = None

try:
    topSongsNum_arg = sys.argv[1]
    randomize_arg = sys.argv[2]
    randomize_count_arg = int(sys.argv[3])
except:
    pass

randomize = 0
topSongsNum = 20

if(topSongsNum_arg != None): topSongsNum = int(topSongsNum_arg)
if(randomize_arg != None): 
    randomize = int(randomize_arg)
    
    if(randomize):
        if(randomize_count_arg == None):
            print("Error! invalid arguments!")
            exit(0)
        if(randomize_count_arg > topSongsNum):
            print("Error! invalid arguments!")
            exit(0)


sc = SparkSession.builder.master("local").appName("PlaySongsGen").getOrCreate()

df_triplets_fileloc = "hdfs://127.0.0.1:9000/project/input_data/train_triplets.txt"

df_triplets_schema = StructType(
  [StructField('user_id', StringType()),
   StructField('song_id', StringType()),
   StructField('listen_count', IntegerType())]
)

df_triplets = sc.read.format('com.databricks.spark.csv') \
                              .options(delimiter = '\t', header=True,inferSchema=False) \
                              .schema(df_triplets_schema) \
                              .load(df_triplets_fileloc)

df_songData_name = "hdfs://127.0.0.1:9000/project/input_data/song_data.csv"
df_songData = sc.read.option("header",True).csv(df_songData_name,sep =",")

df_triplets = df_triplets.join(df_songData,on=['song_id'],how = 'inner')
df_triplets = df_triplets.drop("user_id")
df_triplets = df_triplets.drop("release")
df_triplets = df_triplets.groupBy('song_id').agg(F.sum('listen_count').alias('listen_count'))

df = df_triplets.join(df_songData,on=['song_id'],how = 'inner')
df = df.orderBy('listen_count',ascending=False)
df = df.drop("release")
df = df.distinct()


listOfTopSongID = []
count = 0

if(df.count() == 0):
    print('0')
else:
    final = df.orderBy('listen_count',ascending=False)
    wuw = final.rdd.collect()
    for i in wuw:
        if(count < topSongsNum):
            listOfTopSongID.append( [i.song_id, str(i.listen_count)] )
            count+=1
        else:
            break

if(randomize):
    outputSongID = sample(listOfTopSongID,randomize_count_arg)
else:
    outputSongID = listOfTopSongID

df_popularSongs_columns = ["song_id","listen_count"]
df_popularSongs = sc.createDataFrame(data=outputSongID, schema = df_popularSongs_columns)
df = df_popularSongs.join(df_songData,on=['song_id'],how = 'inner')
df.show()
