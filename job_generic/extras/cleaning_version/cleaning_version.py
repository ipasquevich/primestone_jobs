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
import pyathena
import boto3
import s3fs 



appName = "PySpark - AMRDEF"

spark = SparkSession.builder \
    .appName(appName).config("localhost") \
    .getOrCreate()

sc = SparkContext.getOrCreate()

# este es el df con el que estamos trabajando, se levanta de un excel en este caso
df_union = spark.read.csv(
    "original_versions.csv", header=True, mode="DROPMALFORMED"
)

# creo las columnas de editado y estimado con valores vacios
df_union = df_union.withColumn("estimationReading", lit(""))\
                .withColumn("estimationValid", lit(""))\
                .withColumn("editionReading", lit(""))\
                .withColumn("editionValid", lit(""))



# verificar si el df tiene datos de REGISTERS Y LOAD PROFILE READING



df_nuevo = df_union.filter((df_union.readingType == "REGISTERS") | (df_union.readingType == "LOAD PROFILE READING"))

max_ts = df_nuevo.agg(max('readingUtcLocalTime')).collect()[0][0]
min_ts = df_nuevo.agg(min('readingUtcLocalTime')).collect()[0][0]

# pongo todos los valores de version en 1, como si fueran nuevos, despues se van modificando
df_nuevo = df_nuevo.withColumn("version",lit(1))



# este es el df que esta presente en trustdata (base de datos), hacer una consulta que traiga datos con readinType REGISTERS O
# LOAD PROFILE READING y ademas que tenga una fecha comprendida entre max_ts y min_ts
df_orig = spark.read.csv(
    "original_modificado_glue.csv", header=True, mode="DROPMALFORMED"
)




# checkeo que el df en la base no este vacio para poder hacer los cruces correctamente, caso que sea vacio, automaticamente
# se rellena el campo version presentes el el df procesado con 1
if len(df_orig.head(1)) > 0:
    
    df_orig = df_orig.filter((df_orig.readingType == "REGISTERS") | (df_orig.readingType == "LOAD PROFILE READING"))


    # obtener la info de la ultima version de cada lectura unica
    df_orig = df_orig.withColumn("version", df_orig["version"].cast(IntegerType()))
    df_maxs = df_orig.groupby(['servicePointId',"variableId","logNumber","readingUtcLocalTime","meteringType","readingsSource","owner"]).agg(max("version")).drop('version').withColumnRenamed('max(version)', 'version').coalesce(1)
    df_orig = df_orig.withColumnRenamed('servicePointId', 'servicePointId_drop')\
                .withColumnRenamed('variableId', 'variableId_drop')\
                .withColumnRenamed('logNumber', 'logNumber_drop')\
                .withColumnRenamed('readingUtcLocalTime', 'readingUtcLocalTime_drop')\
                .withColumnRenamed('meteringType', 'meteringType_drop')\
                .withColumnRenamed('version', 'version_drop')\
                .withColumnRenamed('readingsSource', 'readingsSource_drop')\
                .withColumnRenamed('owner', 'owner_drop')\

    # traer los datos de la ultima version de cada lectura unica
    df_orig =  df_maxs.join(df_orig, [(df_maxs.servicePointId == df_orig.servicePointId_drop)
                                        , (df_maxs.variableId == df_orig.variableId_drop)
                                        , (df_maxs.readingsSource == df_orig.readingsSource_drop)
                                        , (df_maxs.owner == df_orig.owner_drop)
                                        , (df_maxs.meteringType == df_orig.meteringType_drop)
                                        , (df_maxs.readingUtcLocalTime == df_orig.readingUtcLocalTime_drop)
                                        , (df_maxs.logNumber.eqNullSafe(df_orig.logNumber_drop))
                                        , (df_maxs.version == df_orig.version_drop)]
                                        ,how = 'inner').coalesce(1)

    columns_to_drop = [item for item in df_orig.columns if "_drop" in item]
    df_orig = df_orig.drop(*columns_to_drop)



    # seleccionar los valores de interes en la base de trustdata 
    df_orig = df_orig.select(col("servicePointId").alias("servicePointId_orig")
        ,col("variableId").alias("variableId_orig")
        ,col("owner").alias("owner_orig")
        ,col("logNumber").alias("logNumber_orig")
        ,col("meteringType").alias("meteringType_orig")
        ,col("readingUtcLocalTime").alias("readingUtcLocalTime_orig")
        ,col("readingsSource").alias("readingsSource_orig")
        ,col("readingsValue").alias("readingsValue_orig")
        ,col("version").alias("version_orig")
        ,col("estimationReading").alias("estimationReading_orig")
        ,col("estimationValid").alias("estimationValid_orig")
        ,col("editionReading").alias("editionReading_orig")
        ,col("editionValid").alias("editionValid_orig")
        )

    
    # left join entre el df en proceso y el df en la base de datos
    df_final = df_nuevo.join(df_orig, [(df_nuevo.servicePointId == df_orig.servicePointId_orig)
                                                            , (df_nuevo.variableId == df_orig.variableId_orig)
                                                            , (df_nuevo.readingsSource == df_orig.readingsSource_orig)
                                                            , (df_nuevo.owner == df_orig.owner_orig)
                                                            , (df_nuevo.meteringType == df_orig.meteringType_orig)
                                                            , (df_nuevo.readingUtcLocalTime == df_orig.readingUtcLocalTime_orig)
                                                            , (df_nuevo.logNumber.eqNullSafe(df_orig.logNumber_orig))]
                                                            ,how = 'left_outer').coalesce(1)


    def versionamiento(row):
        if row.readingsValue_orig is None:
            return 1
        else:
            if row.readingsValue_orig == row.readingsValue:
                return None
            else:
                return row.version_orig + 1

    udf_object = udf(versionamiento, IntegerType())
    df_final = df_final.withColumn("version_ref", udf_object(struct([df_final[x] for x in df_final.columns])))

    # logs y df resultante en base a los valores devueltos por la funcion de versionamiento
    df_logs = df_final.filter(df_final.version_ref.isNull()).withColumn("Descripcion_log",lit("Versionamiento duplicados"))

    df_final = df_final.filter(df_final.version_ref.isNotNull())
    df_final = df_final.drop("version").withColumnRenamed("version_ref", 'version')
    df_final = df_final.drop("estimationReading").withColumnRenamed("estimationReading_orig", 'estimationReading')
    df_final = df_final.drop("estimationValid").withColumnRenamed("estimationValid_orig", 'estimationValid')
    df_final = df_final.drop("editionReading").withColumnRenamed("editionReading_orig", 'editionReading')
    df_final = df_final.drop("editionValid").withColumnRenamed("editionValid_orig", 'editionValid')

    # mantengo los valores de estimado y editado de la version anterior

else:
    df_final = df_nuevo


def usage_reading(row):
    string = ":".join(row.multiplier.split(":")[1:])
    string = string.replace("'", "\"")
    dicti = json.loads(string)
    val = float(row.readingsValue)
    for value in dicti.values():
        if value:
            val = val * float(value)
    return val

udf_object = udf(usage_reading, StringType())
df_final = df_final.withColumn("usageReading", udf_object(struct([df_final[x] for x in df_final.columns])))


# agregar columnas de valor estimado y editado ??  columnas que tenga el df final ?
lista_columnas = ["servicePointId","readingType","variableId","deviceId","meteringType","readingUtcLocalTime","readingDateSource","readingLocalTime","dstStatus"
                ,"channel","unitOfMeasure","qualityCodesSystemId","qualityCodesCategorization","qualityCodesIndex","intervalSize","logNumber"
                ,"version","readingsValue","primarySource","readingsSource","owner","guidFile","estatus"
                ,"registersNumber","eventsCode","agentId","agentDescription","multiplier","deviceMaster","deviceDescription","deviceStatus"
                ,"serial","accountNumber","servicePointTimeZone","connectionType","relationStartDate","relationEndDate"
                ,"deviceType","brand","model","usageReading","estimationReading","estimationValid","editionReading","editionValid"]

df_final = df_final.select(*lista_columnas)


# union con los EVENTS
df_union = df_union.filter((df_union.readingType == "EVENTS"))
df_union = df_union.withColumn("usageReading",lit('')).select(*lista_columnas)
df_union = df_union.union(df_final).coalesce(1)

# logs
# ver agragar las columnas en los logs
#lista_columnas_logs = lista_columnas[:-1]
#lista_columnas_logs.append("Descripcion_log")
#df_logs = df_logs.select(*lista_columnas_logs)



df_union.write.format('csv').mode("overwrite").save("./union", header="true", emptyValue="")
print("\n"*10,df_union.count(),"\n"*10)

df_final.write.format('csv').mode("overwrite").save("./resultado", header="true", emptyValue="")
print("\n"*10,df_final.count(),"\n"*10)
