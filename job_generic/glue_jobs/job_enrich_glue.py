import boto3
import io
import time
import os
import json
import random
import sys
from pyspark.sql import SparkSession, DataFrame, Row,Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType ,ArrayType
from pyspark.sql.functions import udf, struct, lit,row_number, monotonically_increasing_id
from pyspark import SparkContext ,SparkConf
from functools import reduce 
from collections import OrderedDict
from datetime import datetime, timedelta, date
from urllib import request, parse
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame



# defino la funcion para los logs a nivel job
def generacion_logs_jobs(spark,df,category_status,category_process="Enrich",source="ENRICHMENT",
                        target="sqs-logs",event_type="pap-log.business",data_message = " ",
                        category_name="ReadingsProcessingLifecycle",user_id="System",log_level="INFO"):
    
    # Get the service resource
    client = boto3.client('sqs', region_name='us-east-1')

    json_category = {}
    
    json_data = {}
    
    json_log = {}
    
    date_process = datetime.utcnow()
    date_process_log = date_process.strftime("%Y/%m/%d %H:%M:%S,%f")[:-3]
    
    # bloque category
    json_category["category_name"] = category_name
    json_category["guid_file"] = df.head().guidFile
    json_category["process"] = category_process
    json_category["status"] = category_status
    json_category["register_count"] = str(df.count())
    json_category["event_timestamp"] = date_process_log
    
    # bloque data
    json_data["message"] = data_message
    json_data["logLevel"] = log_level
    json_data["category"] = json_category
    json_data["hes"] = df.head().readingsSource
    
    # bloque log
    json_log["target"] = target
    json_log["eventType"] = event_type
    json_log["data"] = [json_data]
    json_log["source"] = source
    json_log["transactionId"] = df.head().guidFile
    json_log["owner"] = df.head().owner
    json_log["userId"] = user_id
    json_log["datestamp"] = date_process_log
    json_log["process"] = source
    
    # envio del msj a la cola
    client.send_message(QueueUrl='https://queue.amazonaws.com/583767213990/sqs-logs',MessageBody=json.dumps(json_log))






def enricher(df_union,s3_path_result,spark):

    # Cambio tipo de datos.
    df_union = df_union.withColumn("channel",df_union["channel"].cast(IntegerType()))
    df_union = df_union.withColumn("unitOfMeasure",df_union["unitOfMeasure"].cast(IntegerType()))
    df_union = df_union.withColumn("intervalSize",df_union["intervalSize"].cast(IntegerType()))
    df_union = df_union.withColumn("logNumber",df_union["logNumber"].cast(IntegerType()))
    df_union = df_union.withColumn("version",lit(1))

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

    s3 = boto3.resource('s3')
    object = s3.Object('primestone-raw-dev', 'enrich_request.json')
    object.put(Body=json.dumps(dicc).encode('utf-8'))



    # BLOQUE DE ENRICH



    # levanto el archivo json de referencia para configuracion, sirve para probar en caso de que no funcione el endpoint
    #with open('respuesta_AMRDEF_sample_20200713.json') as json_file:
    #    req = json.load(json_file)
    #req = req["data"]

    # bloque para el caso en que el json sea una respuesta directa de la api (no generado manualmente)

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
                        ,StructField("result_rD", BooleanType())])

    df_relation_device = spark.createDataFrame(lista_relation_device,schema=schema_relation_device)



    schema_service_point_variable = StructType([StructField("readingType", StringType())\
                            ,StructField("variableId", StringType())\
                            ,StructField("servicePointId", StringType())\
                            ,StructField("result_sPV", BooleanType())\
                            ,StructField("toStock", BooleanType())\
                            ,StructField("lastReadDate", StringType())])
                            
    df_service_point_variable = spark.createDataFrame(lista_service_point_variable,schema=schema_service_point_variable)


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
    df_union.write.format('csv').mode("overwrite").save(s3_path_result, header="true", emptyValue="")
    
    return df_union
    
if __name__ == '__main__':
    args = getResolvedOptions(sys.argv,
                        ['JOB_NAME',
                        'file_name',
                        'bucket_name'])
                        
    bucket = "s3://" + args['bucket_name'] + '/'
    filename = args['file_name']
    s3_path = bucket + filename
    
    output_bucket = 'primestone-trust-dev'
    output_file = filename.split('|')[0] + "|enrich"
    s3_path_result = "s3://" + output_bucket + '/'+ output_file 
    #glue_client = boto3.client("glue")
    #args = getResolvedOptions(sys.argv, ['JOB_NAME','WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
    #workflow_name = args['WORKFLOW_NAME']
    #workflow_run_id = args['WORKFLOW_RUN_ID']
    #workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name,
    #                                    RunId=workflow_run_id)["RunProperties"]

    #s3_filename = workflow_params['translated_file']
    appName = "PySpark - enrichment"
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    df = spark.read.format('csv').options(header='true').load(s3_path)
    generacion_logs_jobs(spark,df,"processing_started")
    df = enricher(df,s3_path_result,spark)
    generacion_logs_jobs(spark,df,"processing_ended")
    client = boto3.client("glue",region_name='us-east-1')
    client.start_job_run(
        JobName= "job-clean",
        Arguments={
         '--file_name': output_file,
         '--bucket_name': output_bucket
        }
    )
    

