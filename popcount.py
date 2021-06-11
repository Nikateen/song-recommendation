#!/usr/bin/python3

import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from random import sample
from pyspark.sql.types import StructType,StructField, StringType

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

sc = SparkSession.builder.master("local").appName("PopularSongsGen").getOrCreate()

df_userFavSongs_name = "hdfs://127.0.0.1:9000/project/input_data/jam_to_msd.tsv"
df_userFavSongs = sc.read.option("header",True).csv(df_userFavSongs_name,sep ="\t")

df_songName_name = "hdfs://127.0.0.1:9000/project/input_data/subset_unique_tracks.csv"
df_songName = sc.read.option("header",True).csv(df_songName_name,sep =",")

df_userFavSongs = df_userFavSongs.join(df_songName,on=['MSD_song_id'],how = 'inner')
df_userFavSongs = df_userFavSongs.withColumn('value', F.lit(1))
df_userFavSongs = df_userFavSongs.drop("user_id")
df_userFavSongs = df_userFavSongs.groupBy('MSD_song_id').agg(F.sum('value').alias('value'))

listOfTopSongID = []
count = 0

if(df_userFavSongs.count() == 0):
    print('0')
else:
    final = df_userFavSongs.orderBy('value',ascending=False)
    wuw = final.rdd.collect()
    for i in wuw:
        if(count < topSongsNum):
            listOfTopSongID.append( [i.MSD_song_id, str(i.value)] )
            count+=1
        else:
            break

if(randomize):
    outputSongID = sample(listOfTopSongID,randomize_count_arg)
else:
    outputSongID = listOfTopSongID

df_popularSongs_columns = ["MSD_song_id","value"]
df_popularSongs = sc.createDataFrame(data=outputSongID, schema = df_popularSongs_columns)
df = df_popularSongs.join(df_songName,on=['MSD_song_id'],how = 'inner')
df.show()