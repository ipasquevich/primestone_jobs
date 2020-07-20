
import boto3
import io
import time
import os
import json
import random
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType ,ArrayType
from pyspark.sql.functions import *
from pyspark import SparkContext ,SparkConf
from functools import reduce 
from collections import OrderedDict
from datetime import datetime, timedelta
from urllib import request, parse

appName = "PySpark - AMRDEF"

spark = SparkSession.builder \
    .appName(appName).config("localhost") \
    .getOrCreate()

sc = SparkContext.getOrCreate()

# este es el df con el que estamos trabajando, se levanta de un excel en este caso
df_union = spark.read.csv(
    "salida_cleaning_sinpaso2.csv", header=True, mode="DROPMALFORMED"
)

df_union = df_union.withColumn("intervalSize",df_union["intervalSize"].cast(IntegerType()))


df_LoadProfile = df_union.filter(df_union.readingType == "LOAD PROFILE READING")

variables_id = df_LoadProfile.select("variableId").distinct().collect()
variables_id = [row.variableId for row in variables_id]



# aca empieza el ciclo for por cada elemento de la variable variable_Id
for iteracion,variable in enumerate(variables_id):

        df_LP = df_LoadProfile.filter(df_LoadProfile.variableId == variable)

        # obtengo los limites de los tiempos y los paso a timestamp
        max_ts = datetime.strptime((df_LP.agg(max('readingUtcLocalTime')).collect()[0][0]),"%Y-%m-%d %H:%M:%S")
        min_ts = datetime.strptime((df_LP.agg(min('readingUtcLocalTime')).collect()[0][0]),"%Y-%m-%d %H:%M:%S")
        delta = max_ts - min_ts

        # interval indica el intervalo en minutos
        interval = df_LP.select("intervalSize").first()[0]
        mins = delta.seconds//60
        cant_lecturas = (mins//interval) + 1

        if df_LP.filter(df_LP.validacion_intervalos.isNotNull()).count() == cant_lecturas:
            df_LP = df_LP.withColumn("usageValid",lit(True)).withColumn("ValidationDetail",lit("")).withColumn("IsGrouped",lit(False))
        else:
            df_LP_success = df_LP.filter(df_LP.validacion_intervalos.isNotNull())
            df_LP_success = df_LP_success.withColumn("usageValid",lit(True)).withColumn("ValidationDetail",lit("")).withColumn("IsGrouped",lit(False))

            df_LP_failure = df_LP.filter(df_LP.validacion_intervalos.isNull())
            df_LP_failure = df_LP_failure.withColumn("usageValid",lit(False))\
                                .withColumn("ValidationDetail",lit('{"Usage":{"IntervalsError":"Interval not exist"}}'))\
                                .withColumn("IsGrouped",lit(False))

            df_LP = df_LP_success.union(df_LP_failure).coalesce(1)
            df_LP = df_LP.orderBy("readingUtcLocalTime",ascending=True)


        if iteracion == 0:
                df_LoadProfile_final = df_LP
        else:
                df_LoadProfile_final = df_LoadProfile.union(df_LP).coalesce(1)


df_LoadProfile = df_LoadProfile_final

# selecciona las columnas en el orden que van y ademas hacer el union con el df original filtrado por
# los otros tipos de lecturas

df_Registers_Events = df_union.filter((df_union.readingType == "REGISTERS") | (df_union.readingType == "EVENTS"))
df_Registers_Events = df_Registers_Events.withColumn("usageValid",lit(None))\
                                        .withColumn("ValidationDetail",lit(""))\
                                        .withColumn("IsGrouped",lit(None))

lista_columnas = ["servicePointId","readingType","variableId","deviceId","meteringType","readingUtcLocalTime","readingDateSource","readingLocalTime","dstStatus"
,"channel","unitOfMeasure","qualityCodesSystemId","qualityCodesCategorization","qualityCodesIndex","intervalSize","logNumber"
,"version","readingsValue","primarySource","readingsSource","owner","guidFile","estatus"
,"registersNumber","eventsCode","agentId","agentDescription","multiplier","deviceMaster","deviceDescription","deviceStatus"
,"serial","accountNumber","servicePointTimeZone","connectionType","relationStartDate","relationEndDate"
,"deviceType","brand","model","usageReading","usageValid","ValidationDetail","IsGrouped","estimationReading","estimationValid","editionReading","editionValid"]




df_LoadProfile = df_LoadProfile.select(*lista_columnas)
df_Registers_Events = df_Registers_Events.select(*lista_columnas)

df_union = df_LoadProfile.union(df_Registers_Events).coalesce(1)

df_union.write.format('csv').mode("overwrite").save("./union", header="true", emptyValue="")
print("\n"*10,df_union.count(),"\n"*10)