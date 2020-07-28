import boto3
import io
import time
import os
import json
import random
from pyspark.sql import SparkSession, DataFrame, Row,Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType ,ArrayType
from pyspark.sql.functions import lit, udf, struct, col, concat, explode, max, min,row_number, monotonically_increasing_id
from pyspark import SparkContext ,SparkConf
from functools import reduce 
from collections import OrderedDict
from datetime import datetime, timedelta, date
from urllib import request, parse


def enricher(df_union,spark):


    # Cambio tipo de datos.
    df_union = df_union.withColumn("channel",df_union["channel"].cast(IntegerType()))
    df_union = df_union.withColumn("unitOfMeasure",df_union["unitOfMeasure"].cast(IntegerType()))
    df_union = df_union.withColumn("intervalSize",df_union["intervalSize"].cast(IntegerType()))
    df_union = df_union.withColumn("logNumber",df_union["logNumber"].cast(IntegerType()))


    # Campos calculados
    df_union = df_union.withColumn("idVdi",lit('')).withColumn("idenEvent",lit(''))\
                .withColumn("identReading",row_number().over(Window.orderBy(monotonically_increasing_id()))-1)\
                .withColumn("idDateYmd",lit('')).withColumn("idDateYw",lit(''))

    fecha_hoy = date.today()
    df_union = df_union.withColumn('dateCreation',lit(fecha_hoy))



    # Creacion del diccionario para el request

    hes = df_union.head().readingsSource
    owner = df_union.head().owner
    guid = df_union.head().guidFile
    
    dicc = {"owner": {
                            "hes": hes,
                            "owner": owner,
                            "guidFile": guid
                    }
                    }


    # creo el dataframe con valores unicos tanto de servicePointId como de deviceId

    df_relation_device = df_union.dropDuplicates((["servicePointId","deviceId"]))
    df_service_point = df_relation_device.dropDuplicates((["servicePointId"]))
    df_device = df_relation_device.dropDuplicates((["deviceId"]))

    df_service_point_variable_prof = df_union.filter(df_union.readingType == "LOAD PROFILE READING")
    df_service_point_variable_prof = df_service_point_variable_prof.dropDuplicates((["readingType","variableId","servicePointId"
                                    ,"meteringType","unitOfMeasure","logNumber","channel","intervalSize"]))

    df_service_point_variable_reg_event = df_union.filter((df_union.readingType == "REGISTERS") | (df_union.readingType == "EVENTS"))
    df_service_point_variable_reg_event = df_service_point_variable_reg_event.dropDuplicates((["readingType","variableId","servicePointId",
                                    "meteringType","unitOfMeasure"]))


    # genero el diccionario del request

    lista_aux = []
    for row in df_service_point.rdd.collect():
            dicc_aux = { "servicePointId": row.servicePointId }
            lista_aux.append(dicc_aux)
    dicc["servicePoint"] = lista_aux



    lista_aux = []
    for row in df_device.rdd.collect():
            dicc_aux = {
                            "brand": row.brand,
                            "model": row.model,
                            "deviceCategory": "METER",
                            "deviceId": row.deviceId,
                            "deviceDescription": row.deviceDescription,
                            "deviceStatus":row.deviceStatus
                            }
            lista_aux.append(dicc_aux)
    dicc["device"] = lista_aux


    lista_aux = []
    for row in df_relation_device.rdd.collect():
            dicc_aux = {
                                    "servicePointId": row.servicePointId,
                                    "deviceId": row.deviceId,
                                    "meteringType": row.meteringType,
                                    "relationStartDate": row.relationStartDate,
                                    "relationEndDate": row.relationEndDate
                            }
            lista_aux.append(dicc_aux)
    dicc["relationDevice"] = lista_aux


    lista_aux = []
    for row in df_service_point_variable_prof.rdd.collect():
            dicc_aux = {
                                    "readingType": row.readingType,
                                    "variableId": row.variableId,
                                    "servicePointId": row.servicePointId,
                                    "meteringType": row.meteringType,
                                    "unitOfMeasure": row.unitOfMeasure,
                                    "logNumber": row.logNumber,
                                    "channel": row.channel,
                                    "intervalSize": row.intervalSize
                            }
            lista_aux.append(dicc_aux)

    lista_aux_2 = []
    for row in df_service_point_variable_reg_event.rdd.collect():
            dicc_aux = {
                                    "readingType": row.readingType,
                                    "variableId": row.variableId,
                                    "servicePointId": row.servicePointId,
                                    "meteringType": row.meteringType,
                                    "unitOfMeasure": row.unitOfMeasure
                            }
            lista_aux_2.append(dicc_aux)

    if lista_aux:
            lista_aux.extend(lista_aux_2)
            dicc["servicePointVariable"] = lista_aux
    else:
            dicc["servicePointVariable"] = lista_aux_2




    # BLOQUE DE ENRICH

    # bloque para el caso en que el json sea una respuesta directa de la api

    base_url = "http://80f56133-default-orchestra-c412-1531608832.us-east-1.elb.amazonaws.com/orchestrator/topology/set_up"
    
    headers = {"Content-Type":"application/json", "Transaction-Id":"test"
              , "User-Id":"test"}
    
    body = json.dumps(dicc)
    
    req =  request.Request(base_url, data=bytes(body.encode("utf-8"))
                            ,headers=headers,method="POST") # this will make the method "POST"
    resp = request.urlopen(req).read()
    response = json.loads(resp)
    req = response["data"]

    # Con el json levantado necesito extraer los datos para cruzar con el dataframe que tenemos
    # Extraigo los valores del diccionario en dos listas

    lista_relation_device = [[item["servicePointId"],item["deviceId"],item["result"]]
                            for item in req["relationDevice"]]

    lista_service_point_variable = [[item["readingType"],item["variableId"],item["servicePointId"],item["result"],
                                item["toStock"],item["lastReadDate"]]
                                for item in req["servicePointVariable"] ]

    # Creo un dataframe por lista

    schema_relation_device = StructType([StructField("servicePointId", StringType())\
                        ,StructField("deviceId", StringType())\
                        ,StructField("result_rD", BooleanType())
                        ])

    df_relation_device = spark.createDataFrame(lista_relation_device,schema=schema_relation_device).coalesce(1)



    schema_service_point_variable = StructType([StructField("readingType", StringType())\
                            ,StructField("variableId", StringType())\
                            ,StructField("servicePointId", StringType())\
                            ,StructField("result_sPV", BooleanType())\
                            ,StructField("toStock", BooleanType())\
                            ,StructField("lastReadDate", StringType())])
                            
    df_service_point_variable = spark.createDataFrame(lista_service_point_variable,schema=schema_service_point_variable).coalesce(1)

    
    # hago un inner join de los dataframes creados con el dataframe original (enrichment)
    df_union = df_union.join(df_relation_device, on=['servicePointId',"deviceId"], how='inner').coalesce(1)
    df_union = df_union.join(df_service_point_variable, on=['variableId',"servicePointId","readingType"], how='inner').coalesce(1)
    

    # bloque calculo de readingUtcLocalTime
    # hay que tomar la hora readingLocalTime y pasarla a la hora correspondiente en Utc = 0 para cada registro del df.
    # para la conversion necesito tomar el valor de servicePointTimeZone

    # Si hay registros con valores nulos en servicePointTimeZone entonces no van a servir para el analisis y hay que eliminarlos
    df_union = df_union.filter(df_union.servicePointTimeZone.isNotNull())

    # defino el diccionario con las transformaciones para cada codigo
    utc_dicc = {
            '0': -12,
            '1': -11,
            '2': -10,
            '3': -9,
            '4': -8,
            '5': -7,
            '6': -7,
            '7': -6,
            '8': -6,
            '9': -6,
            '10': -5,
            '11': -5,
            '12': -5,
            '13': -4,
            '14': -4,
            '15': -3.5,
            '16': -3,
            '17': -3,
            '18': -2,
            '19': -1,
            '20': 0,
            '21': 0,
            '22': 1,
            '23': 1,
            '24': 1,
            '25': 2,
            '26': 2,
            '27': 2,
            '28': 2,
            '29': 2,
            '30': 3,
            '31': 3,
            '32': 3.5,
            '33': 4,
            '34': 4.5,
            '35': 5,
            '36': 5.5,
            '37': 6,
            '38': 7,
            '39': 8,
            '40': 8,
            '41': 9,
            '42': 9.5,
            '43': 9.5,
            '44': 10,
            '45': 10,
            '46': 10,
            '47': 11,
            '48': 12,
            '49': 12,
            '50': -8,
            '51': -7,
            '52': -6,
            '53': -6,
            '54': -6,
            '55': -6,
            '56': 10}

    def conversor_utc(row):
            if row.readingLocalTime:
                    fecha = datetime.strptime(row.readingLocalTime,'%Y-%m-%d %H:%M:%S')
                    fecha = fecha - timedelta(hours = utc_dicc[row.servicePointTimeZone])
                    return fecha.strftime('%Y-%m-%d %H:%M:%S')

    # Aplico la funcion al dataframe
    udf_object = udf(conversor_utc, StringType())
    df_union = df_union.withColumn("readingUtcLocalTime", udf_object(struct([df_union[x] for x in df_union.columns])))



    # Campos calculados parte 2
    def fecha_numeric(valor):
            try:
                    return "".join(valor[:10].split("-"))
            except TypeError:
                    return ''

    udf1 = udf(fecha_numeric,StringType())
    df_union = df_union.withColumn('idDateYmd',udf1('readingUtcLocalTime'))


    def semana(valor):
            try:
                    date_format = '%Y-%m-%d'
                    fecha = datetime.strptime(valor[:10],date_format)
                    an,sem,_ = fecha.isocalendar()
                    return str(an)+str(sem)
            except (ValueError,TypeError) :
                    return ''

    udf1 = udf(semana,StringType())
    df_union = df_union.withColumn('idDateYw',udf1('readingUtcLocalTime'))


        
    # escribo los csv
    df_union.write.format('csv').mode("overwrite").save("./output/enriched", header="true", emptyValue="")


    return df_union
