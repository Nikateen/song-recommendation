#!/usr/bin/python3

import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from random import sample
from pyspark.sql.types import StructType,StructField, StringType



threshold_songlist = 10
threshold_songpick = 2
selected_user_id = int(sys.argv[1])

i = 0

def id_gen(value):
    global i
    i += 1
    return i

def num_listens(userID,songID,df_triplets):
    try:
        result = df_triplets.where(df_triplets.user_id == userID).where(df_triplets.song_id == songID).select("listen_count")
        r = result.rdd.collect()
        for i in r:
            return int(i.listen_count)
    except:
        return 1



songid = []


sc = SparkSession.builder.master("local").appName("Recommender").getOrCreate()


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




id_func = F.udf(id_gen, IntegerType())
i = 0
userId_change = df_triplets.select('user_id').distinct().withColumn('new_userId', id_func("user_id"))
i = 0
songId_change = df_triplets.select('song_id').distinct().withColumn('new_songId', id_func("song_id"))
df_triplets = df_triplets.join(userId_change, 'user_id').join(songId_change, 'song_id')
#df_triplets.show()

list_selectedUser_schema = ["new_userId"] 
list_selectedUser = [ [selected_user_id] ]
df_selectedUser = sc.createDataFrame(data=list_selectedUser, schema = list_selectedUser_schema)
df_selectedUser = df_selectedUser.join(df_triplets,on=['new_userId'],how = 'inner')
df_selectedUser = df_selectedUser.orderBy('listen_count',ascending=False)
info_selectedUser = df_selectedUser.rdd.collect()

info_selectedUser_counter = 0
for i in info_selectedUser:
    if(info_selectedUser_counter < threshold_songpick):
        songid.append(str(i.song_id))
    else:
        break
    info_selectedUser_counter += 1

weights = [1 for i in range(len(songid))]


df_triplets = df_triplets.filter(df_triplets.new_userId != selected_user_id)

df_songData_name = "hdfs://127.0.0.1:9000/project/input_data/song_data.csv"
df_songData = sc.read.option("header",True).csv(df_songData_name,sep =",")


df_users = df_triplets.join(df_songData,on=['song_id'],how = 'inner')
df_users = df_users.drop("song_id")
df_users = df_users.groupBy('user_id').agg(F.sum('listen_count').alias('listen_count'))
df_users = df_users.orderBy('listen_count',ascending=False) .filter('listen_count >= 2000')
#f_users.show()



user_data = df_users.rdd.collect()
user_weight_list_counterlen = len(user_data)*len(songid)
print("Number of searches:",user_weight_list_counterlen)

user_weight_list = []
user_weight_list_counter = 0
for i in user_data:
    user_weight = 0

    for j in range(len(songid)):
        print("search #"+str(user_weight_list_counter)+"/"+str(user_weight_list_counterlen))
        user_weight_list_counter += 1
        songListens = num_listens(i.user_id,songid[j],df_triplets)
        try:
            user_weight += songListens*weights[j]
        except:
            pass
    user_weight_list.append([i.user_id, user_weight])
user_weight_list.sort(key=lambda x: x[1],reverse=True)


df_userweight_schema = ["user_id","user_weight"]
df_userweight = sc.createDataFrame(data=user_weight_list, schema = df_userweight_schema)
#df_userweight.show()

df_usersFinal = df_userweight.join(df_users,on=['user_id'],how = 'inner')
df_usersFinal = df_usersFinal.orderBy('user_weight',ascending=False).filter('user_weight >= 1')
#df_usersFinal.show()
userListFinal = df_usersFinal.rdd.collect()

recommended_songsList = []
df_temp = df_triplets.drop("listen_count")
df = df_usersFinal.join(df_temp,on=['user_id'],how = 'inner')
#cols = ["user_weight","listen_count"]
df = df.join(df_songData,on=['song_id'],how = 'inner')
df = df.orderBy("user_weight","listen_count",ascending=False)

df_final = df.drop("song_id")
df_final = df_final.drop("user_id")
df_final = df_final.drop("release")
df_final = df_final.drop("user_weight")
df_final = df_final.drop("listen_count")
df_final = df_final.drop("new_userID")
df_final = df_final.drop("new_songID")
df_final = df_final.distinct()
df_final.show(threshold_songlist)

#user_data = df_usersFinal.rdd.collect()

