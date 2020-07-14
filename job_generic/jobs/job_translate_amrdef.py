
import json
import random
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType 
from pyspark.sql.functions import *
from pyspark import SparkContext ,SparkConf
from functools import reduce 
from collections import OrderedDict
from datetime import datetime, timedelta

def translator(xml_file_path,spark):

    # Cada <MeterReadings> ... <MeterReadings/> va ser tomado como un row en el dataframe df.
    rootTag = "AMRDEF"
    rowTag = "MeterReadings"

    strSch = spark.read.text("../configs/schemaAMRDEF_sim_allstring.json").first()[0]
    schema = StructType.fromJson(json.loads(strSch))

    bucket = "s3://primestone-raw-dev/"
    #bucket = "s3://" + args['bucket_name'] + '/'

    filename = "amrdef-test.xml"
    #filename = args['file_name']

    guid = filename.split(".xml")[0]

    s3_path = bucket + filename

    filename = "PRIMEREAD/AMRDEF/2020/19/Actualizacion+de+datos.xlsx"
    hes = filename.split("/")[1]
    owner = filename.split("/")[0]

    df = spark.read.format("com.databricks.spark.xml").options(rootTag=rootTag).options(rowTag=rowTag).options(nullValue="").options(valueTag="_valueTag").option("columnNameOfCorruptRecord", "algunNombre") \
        .schema(schema)\
        .load(xml_file_path)


    '''
    Se crea un dataframe por cada tipo de lectura (MaxDemandData, DemandResetCount, etc.) porque es la forma más facil de tratarlos para la traducción.
    Cada dataframe se crea primero seleccionando las columnas que sean necesarias para ese tipo de lectura ( df.withColumn(..las col que hagan falta..).select(..) )
    Al tomar estas columnas, se les asigna un nombre distinto dependiento de su procedencia para identificarlos más facil.
    - Los atributos que vienen de el MeterReadings "padre" van a tener nombres que comiencen con MeterReadings_ (por ejemplo: MeterReadings_Source)
    - Los atributos que vienen de Meter, van a comenzar con Meter_ (por ejemplo: Meter_MeterIrn)
    - Los atributos que lleven un valor fijo/hardcodeado comienzan con FixedAttribute_ (por ejemplo, "FixedAttribute_estatus" que siempre debe tener como valor: "Activo")
    - Los atributos propios de la lectura, simplemente llevan el nombre del atributo (por ejemplo: UOM, Direction, TouBucket, etc)

    Despues de seleccionar las columnas que necesitamos, sobre ese dataframe reducido se hace la traducción correspondiente a ese tipo de lectura.
    En el XML siempre va a haber muchos MeterReadings que dentro tendrán un Meter y muchas leecturas de distinto tipo (pueden ser MaxDemandData, ConsumptionData, y todas las definidas en el xls)
    por lo tanto, el dataframe df (el que contiene toda la data de el xml) va a tener una estructura complicada, los atributos de MeterReadings y Meter no van a traer problemas,
    pero los atributos propios de cada lectura al ser un array (ya que un MeterReadings puede tener muchos MaxDemandData por ejemplo), van a estar almacenados como array 
    en una sola fila del df. Por ejemplo, en UOM podemos tener en una sola fila algo como:

    [Kw, Kw, Voltage, Current, Kwh] y esto nosotros necesitamos pasarlo a distintas filas. Por eso, en la parte de traducción, se hace primero que nada:
    
    .withColumn("tmp", arrays_zip("TouBucket", "Direction","UOM", "Value", "Multiplier","TimeStamp")) 
    .withColumn("tmp", explode("tmp"))

    (Esto se hace SOLO sobre los atributos que son pertenecientes a la leectura en si, es decir, los que son arrays. No hay que hacerlo sobre los atributos que provienen de 
    MeterReadings o Meter ya que esos no son arrays.)
    Una vez que se hace esto, se puede acceder a cada fila con tmp.NombreDelAtributo. Por ejemplo: col("tmp.UOM")

    Para los casos en los que hay que checkear el valor del atributo en el xml para asignar el valor de esa columna en nuestro df, se puede usar la funcion .when(condicion, valor).otherwise(valor)
    Por ejemplo, para definir el valor de MeterReadings_Source si vemos en el excel, cuando su valor sea "LocalRF" lo debemos traducir a "LAN" y asi hay varias condiciones:
    .withColumn("MeterReadings_Source", 
                when(col("MeterReadings_Source") == "Visual", lit("Visual")) 
                .when(col("MeterReadings_Source") == "Remote", lit("Remoto")) 
                .when(col("MeterReadings_Source") == "LocalRF", lit("LAN")) 
                .when(col("MeterReadings_Source") == "Optical", lit("Optical")) 
                .when(col("MeterReadings_Source") == "Manually Estimated", lit("Visual")) 
                .when(col("MeterReadings_Source") == "LegacySystem", lit("HES")) 
                .otherwise(col("MeterReadings_Source"))) 
    '''


    ######################################################################################################################################################

    ######################################################################################################################################################
    maxDemandDataReadings = df.withColumn("TouBucket", col("MaxDemandData.MaxDemandSpec._TouBucket")) \
                                .withColumn("Direction", col("MaxDemandData.MaxDemandSpec._Direction")) \
                                .withColumn("UOM", col("MaxDemandData.MaxDemandSpec._UOM")) \
                                .withColumn("Multiplier", col("MaxDemandData.MaxDemandSpec._Multiplier")) \
                                .withColumn("Value", col("MaxDemandData.Reading._Value")) \
                                .withColumn("MeterReadings_CollectionTime", col("_CollectionTime")) \
                                .withColumn("TimeStamp", col("MaxDemandData.Reading._TimeStamp")) \
                                .withColumn("MeterReadings_Source", col("_Source")) \
                                .withColumn("Meter_SdpIdent", col("Meter._SdpIdent")) \
                                .withColumn("FixedAttribute_readingType", lit("REGISTERS")) \
                                .withColumn("Meter_MeterIrn", col("Meter._MeterIrn")) \
                                .withColumn("FixedAttribute_meteringType", lit("MAIN")) \
                                .withColumn("FixedAttribute_readingUtcLocalTime", lit("")) \
                                .withColumn("FixedAttribute_readingDateSource", lit("")) \
                                .withColumn("FixedAttribute_dstStatus", lit("")) \
                                .withColumn("FixedAttribute_channel", lit("")) \
                                .withColumn("FixedAttribute_qualityCodesSystemId", lit("")) \
                                .withColumn("FixedAttribute_qualityCodesCategorization", lit("")) \
                                .withColumn("FixedAttribute_qualityCodesIndex", lit("")) \
                                .withColumn("FixedAttribute_intervalSize", lit("")) \
                                .withColumn("FixedAttribute_logNumber", lit("")) \
                                .withColumn("FixedAttribute_ct", lit("")) \
                                .withColumn("FixedAttribute_pt", lit("")) \
                                .withColumn("FixedAttribute_sf", lit("")) \
                                .withColumn("FixedAttribute_version", lit("")) \
                                .withColumn("FixedAttribute_readingsSource", lit("")) \
                                .withColumn("FixedAttribute_owner", lit(owner)) \
                                .withColumn("FixedAttribute_guidFile", lit(guid)) \
                                .withColumn("FixedAttribute_estatus", lit("Activo")) \
                                .withColumn("FixedAttribute_registersNumber", lit("")) \
                                .withColumn("FixedAttribute_eventsCode", lit("")) \
                                .withColumn("FixedAttribute_agentId", lit("")) \
                                .withColumn("FixedAttribute_agentDescription", lit("")) \
                                .select(
                                    "TouBucket", 
                                    "Direction", 
                                    "UOM", 
                                    "TimeStamp",
                                    "MeterReadings_CollectionTime",
                                    "Meter_SdpIdent",
                                    "FixedAttribute_readingType",
                                    "Meter_MeterIrn",
                                    "FixedAttribute_meteringType",
                                    "FixedAttribute_readingUtcLocalTime",
                                    "FixedAttribute_readingDateSource",
                                    "FixedAttribute_dstStatus",
                                    "FixedAttribute_channel",
                                    "FixedAttribute_qualityCodesSystemId",
                                    "FixedAttribute_qualityCodesCategorization",
                                    "FixedAttribute_qualityCodesIndex",
                                    "FixedAttribute_intervalSize",
                                    "FixedAttribute_logNumber",
                                    "FixedAttribute_ct",
                                    "FixedAttribute_pt",
                                    "Multiplier",
                                    "FixedAttribute_sf",
                                    "FixedAttribute_version",
                                    "Value",
                                    "MeterReadings_Source", 
                                    "FixedAttribute_readingsSource",
                                    "FixedAttribute_owner",
                                    "FixedAttribute_guidFile",
                                    "FixedAttribute_estatus",
                                    "FixedAttribute_registersNumber",
                                    "FixedAttribute_eventsCode",
                                    "FixedAttribute_agentId",
                                    "FixedAttribute_agentDescription",
                                    "_SourceIrn",
                                    "Meter._Description",
                                    "Meter._IsActive",
                                    "Meter._SerialNumber",
                                    "Meter._MeterType",
                                    "Meter._AccountIdent",
                                    "Meter._TimeZoneIndex",
                                    "Meter._MediaType",
                                    "Meter._InstallDate",
                                    "Meter._RemovalDate")
    maxDemandDataReadings = maxDemandDataReadings.withColumn("tmp", arrays_zip("TouBucket", "Direction","UOM", "Value", "Multiplier","TimeStamp")) \
                                                .withColumn("tmp", explode("tmp")) \
                                                .withColumn("TimeStamp", 
                                                            when(col("tmp.TimeStamp").isNull(), col("MeterReadings_CollectionTime")) \
                                                            .otherwise(col("tmp.TimeStamp"))) \
                                                .withColumn("MeterReadings_Source", 
                                                            when(col("MeterReadings_Source") == "Visual", lit("Visual")) \
                                                            .when(col("MeterReadings_Source") == "Remote", lit("Remoto")) \
                                                            .when(col("MeterReadings_Source") == "LocalRF", lit("LAN")) \
                                                            .when(col("MeterReadings_Source") == "Optical", lit("Optical")) \
                                                            .when(col("MeterReadings_Source") == "Manually Estimated", lit("Visual")) \
                                                            .when(col("MeterReadings_Source") == "LegacySystem", lit("HES")) \
                                                            .otherwise(col("MeterReadings_Source"))) \
                                                .withColumn("servicePointId", 
                                                            when(col("Meter_SdpIdent").isNull(), col("Meter_MeterIrn")) \
                                                            .otherwise(col("Meter_SdpIdent"))) \
                                                .select(
                                                    col("servicePointId"),
                                                    col("FixedAttribute_readingType").alias("readingType"),
                                                    concat(col("tmp.UOM"),lit(" "),col("tmp.Direction"),lit(" "),col("tmp.TouBucket")).alias("variableId"),
                                                    col("Meter_MeterIrn").alias("deviceId"),
                                                    col("FixedAttribute_meteringType").alias("meteringType"),
                                                    col("FixedAttribute_readingUtcLocalTime").alias("readingUtcLocalTime"),
                                                    col("TimeStamp").alias("readingDateSource"),
                                                    col("TimeStamp").alias("readingLocalTime"),
                                                    col("FixedAttribute_dstStatus").alias("dstStatus"),
                                                    col("FixedAttribute_channel").alias("channel"),
                                                    col("tmp.UOM").alias("unitOfMeasure"),
                                                    col("FixedAttribute_qualityCodesSystemId").alias("qualityCodesSystemId"),
                                                    col("FixedAttribute_qualityCodesCategorization").alias("qualityCodesCategorization"),
                                                    col("FixedAttribute_qualityCodesIndex").alias("qualityCodesIndex"),
                                                    col("FixedAttribute_intervalSize").alias("intervalSize"),
                                                    col("FixedAttribute_logNumber").alias("logNumber"),
                                                    col("FixedAttribute_ct").alias("ct"),
                                                    col("FixedAttribute_pt").alias("pt"),
                                                    col("tmp.Multiplier").alias("ke"),
                                                    col("FixedAttribute_sf").alias("sf"),
                                                    col("FixedAttribute_version").alias("version"),
                                                    col("tmp.Value").alias("readingsValue"),
                                                    col("MeterReadings_Source").alias("primarySource"),
                                                    col("FixedAttribute_owner").alias("owner"),
                                                    col("FixedAttribute_guidFile").alias("guidFile"),
                                                    col("FixedAttribute_estatus").alias("estatus"),
                                                    col("FixedAttribute_registersNumber").alias("registersNumber"),
                                                    col("FixedAttribute_eventsCode").alias("eventsCode"),
                                                    col("FixedAttribute_agentId").alias("agentId"),
                                                    col("FixedAttribute_agentDescription").alias("agentDescription"),
                                                    col("_SourceIrn"),
                                                    col("_Description"),
                                                    col("_IsActive"),
                                                    col("_SerialNumber"),
                                                    col("_MeterType"),
                                                    col("_AccountIdent"),
                                                    col("_TimeZoneIndex"),
                                                    col("_MediaType"),
                                                    col("_InstallDate"),
                                                    col("_RemovalDate")
                                                    )

    ######################################################################################################################################################

    ######################################################################################################################################################
    demandResetCountReadings = df.withColumn("Count", col("DemandResetCount._Count")) \
                                .withColumn("TimeStamp", col("DemandResetCount._TimeStamp")) \
                                .withColumn("UOM", col("DemandResetCount._UOM")) \
                                .withColumn("FixedAttribute_variableId", lit("Demand Reset Count")) \
                                .withColumn("MeterReadings_Source", col("_Source")) \
                                .withColumn("Meter_SdpIdent", col("Meter._SdpIdent")) \
                                .withColumn("FixedAttribute_readingType", lit("REGISTERS")) \
                                .withColumn("Meter_MeterIrn", col("Meter._MeterIrn")) \
                                .withColumn("FixedAttribute_meteringType", lit("MAIN")) \
                                .withColumn("FixedAttribute_readingUtcLocalTime", lit("")) \
                                .withColumn("FixedAttribute_readingDateSource", lit("")) \
                                .withColumn("FixedAttribute_dstStatus", lit("")) \
                                .withColumn("FixedAttribute_channel", lit("")) \
                                .withColumn("FixedAttribute_qualityCodesSystemId", lit("")) \
                                .withColumn("FixedAttribute_qualityCodesCategorization", lit("")) \
                                .withColumn("FixedAttribute_qualityCodesIndex", lit("")) \
                                .withColumn("FixedAttribute_intervalSize", lit("")) \
                                .withColumn("FixedAttribute_logNumber", lit("")) \
                                .withColumn("FixedAttribute_ct", lit("")) \
                                .withColumn("FixedAttribute_pt", lit("")) \
                                .withColumn("FixedAttribute_ke", lit("")) \
                                .withColumn("FixedAttribute_sf", lit("")) \
                                .withColumn("FixedAttribute_version", lit("")) \
                                .withColumn("FixedAttribute_readingsSource", lit("")) \
                                .withColumn("FixedAttribute_owner", lit(owner)) \
                                .withColumn("FixedAttribute_guidFile", lit(guid)) \
                                .withColumn("FixedAttribute_estatus", lit("Activo")) \
                                .withColumn("FixedAttribute_registersNumber", lit("")) \
                                .withColumn("FixedAttribute_eventsCode", lit("")) \
                                .withColumn("FixedAttribute_agentId", lit("")) \
                                .withColumn("FixedAttribute_agentDescription", lit("")) \
                                .select(
                                    "Count", 
                                    "TimeStamp", 
                                    "UOM",
                                    "FixedAttribute_variableId",
                                    "MeterReadings_Source",
                                    "Meter_SdpIdent",
                                    "FixedAttribute_readingType",
                                    "Meter_MeterIrn",
                                    "FixedAttribute_meteringType",
                                    "FixedAttribute_readingUtcLocalTime",
                                    "FixedAttribute_readingDateSource",
                                    "FixedAttribute_dstStatus",
                                    "FixedAttribute_channel",
                                    "FixedAttribute_qualityCodesSystemId",
                                    "FixedAttribute_qualityCodesCategorization",
                                    "FixedAttribute_qualityCodesIndex",
                                    "FixedAttribute_intervalSize",
                                    "FixedAttribute_logNumber",
                                    "FixedAttribute_ct",
                                    "FixedAttribute_pt",
                                    "FixedAttribute_ke",
                                    "FixedAttribute_sf",
                                    "FixedAttribute_version",
                                    "FixedAttribute_readingsSource",
                                    "FixedAttribute_owner",
                                    "FixedAttribute_guidFile",
                                    "FixedAttribute_estatus",
                                    "FixedAttribute_registersNumber",
                                    "FixedAttribute_eventsCode",
                                    "FixedAttribute_agentId",
                                    "FixedAttribute_agentDescription",
                                    "_SourceIrn",
                                    "Meter._Description",
                                    "Meter._IsActive",
                                    "Meter._SerialNumber",
                                    "Meter._MeterType",
                                    "Meter._AccountIdent",
                                    "Meter._TimeZoneIndex",
                                    "Meter._MediaType",
                                    "Meter._InstallDate",
                                    "Meter._RemovalDate")
    demandResetCountReadings = demandResetCountReadings.withColumn("tmp", arrays_zip("Count", "TimeStamp","UOM")) \
                                                .withColumn("tmp", explode("tmp")) \
                                                .withColumn("MeterReadings_Source", 
                                                            when(col("MeterReadings_Source") == "Visual", lit("Visual")) \
                                                            .when(col("MeterReadings_Source") == "Remote", lit("Remoto")) \
                                                            .when(col("MeterReadings_Source") == "LocalRF", lit("LAN")) \
                                                            .when(col("MeterReadings_Source") == "Optical", lit("Optical")) \
                                                            .when(col("MeterReadings_Source") == "Manually Estimated", lit("Visual")) \
                                                            .when(col("MeterReadings_Source") == "LegacySystem", lit("HES")) \
                                                            .otherwise(col("MeterReadings_Source"))) \
                                                .withColumn("unitOfMeasure", 
                                                            when(col("tmp.UOM") == 'Times', lit("Count")) \
                                                            .otherwise(col("tmp.UOM"))) \
                                                .withColumn("servicePointId", 
                                                            when(col("Meter_SdpIdent").isNull(), col("Meter_MeterIrn")) \
                                                            .otherwise(col("Meter_SdpIdent"))) \
                                                .select(
                                                    col("servicePointId"),
                                                    col("FixedAttribute_readingType").alias("readingType"),
                                                    col("FixedAttribute_variableId").alias("variableId"),
                                                    col("Meter_MeterIrn").alias("deviceId"),
                                                    col("FixedAttribute_meteringType").alias("meteringType"),
                                                    col("FixedAttribute_readingUtcLocalTime").alias("readingUtcLocalTime"),
                                                    col("tmp.TimeStamp").alias("readingDateSource"),
                                                    col("tmp.TimeStamp").alias("readingLocalTime"),
                                                    col("FixedAttribute_dstStatus").alias("dstStatus"),
                                                    col("FixedAttribute_channel").alias("channel"),
                                                    col("unitOfMeasure"),
                                                    col("FixedAttribute_qualityCodesSystemId").alias("qualityCodesSystemId"),
                                                    col("FixedAttribute_qualityCodesCategorization").alias("qualityCodesCategorization"),
                                                    col("FixedAttribute_qualityCodesIndex").alias("qualityCodesIndex"),
                                                    col("FixedAttribute_intervalSize").alias("intervalSize"),
                                                    col("FixedAttribute_logNumber").alias("logNumber"),
                                                    col("FixedAttribute_ct").alias("ct"),
                                                    col("FixedAttribute_pt").alias("pt"),
                                                    col("FixedAttribute_ke").alias("ke"),
                                                    col("FixedAttribute_sf").alias("sf"),
                                                    col("FixedAttribute_version").alias("version"),
                                                    col("tmp.Count").alias("readingsValue"),
                                                    col("MeterReadings_Source").alias("primarySource"),
                                                    col("FixedAttribute_owner").alias("owner"),
                                                    col("FixedAttribute_guidFile").alias("guidFile"),
                                                    col("FixedAttribute_estatus").alias("estatus"),
                                                    col("FixedAttribute_registersNumber").alias("registersNumber"),
                                                    col("FixedAttribute_eventsCode").alias("eventsCode"),
                                                    col("FixedAttribute_agentId").alias("agentId"),
                                                    col("FixedAttribute_agentDescription").alias("agentDescription"),
                                                    col("_SourceIrn"),
                                                    col("_Description"),
                                                    col("_IsActive"),
                                                    col("_SerialNumber"),
                                                    col("_MeterType"),
                                                    col("_AccountIdent"),
                                                    col("_TimeZoneIndex"),
                                                    col("_MediaType"),
                                                    col("_InstallDate"),
                                                    col("_RemovalDate")
                                                    )
    ######################################################################################################################################################

    ######################################################################################################################################################
    consumptionDataReadings = df.withColumn("TouBucket", col("ConsumptionData.ConsumptionSpec._TouBucket")) \
                                .withColumn("Direction", col("ConsumptionData.ConsumptionSpec._Direction")) \
                                .withColumn("UOM", col("ConsumptionData.ConsumptionSpec._UOM")) \
                                .withColumn("Multiplier", col("ConsumptionData.ConsumptionSpec._Multiplier")) \
                                .withColumn("Value", col("ConsumptionData.Reading._Value")) \
                                .withColumn("TimeStamp", col("ConsumptionData.Reading._TimeStamp")) \
                                .withColumn("MeterReadings_Source", col("_Source")) \
                                .withColumn("Meter_SdpIdent", col("Meter._SdpIdent")) \
                                .withColumn("FixedAttribute_readingType", lit("REGISTERS")) \
                                .withColumn("Meter_MeterIrn", col("Meter._MeterIrn")) \
                                .withColumn("FixedAttribute_meteringType", lit("MAIN")) \
                                .withColumn("FixedAttribute_readingUtcLocalTime", lit("")) \
                                .withColumn("FixedAttribute_readingDateSource", lit("")) \
                                .withColumn("FixedAttribute_dstStatus", lit("")) \
                                .withColumn("FixedAttribute_channel", lit("")) \
                                .withColumn("FixedAttribute_qualityCodesSystemId", lit("")) \
                                .withColumn("FixedAttribute_qualityCodesCategorization", lit("")) \
                                .withColumn("FixedAttribute_qualityCodesIndex", lit("")) \
                                .withColumn("FixedAttribute_intervalSize", lit("")) \
                                .withColumn("FixedAttribute_logNumber", lit("")) \
                                .withColumn("FixedAttribute_ct", lit("")) \
                                .withColumn("FixedAttribute_pt", lit("")) \
                                .withColumn("FixedAttribute_sf", lit("")) \
                                .withColumn("FixedAttribute_version", lit("")) \
                                .withColumn("FixedAttribute_readingsSource", lit("")) \
                                .withColumn("FixedAttribute_owner", lit(owner)) \
                                .withColumn("FixedAttribute_guidFile", lit(guid)) \
                                .withColumn("FixedAttribute_estatus", lit("Activo")) \
                                .withColumn("FixedAttribute_registersNumber", lit("")) \
                                .withColumn("FixedAttribute_eventsCode", lit("")) \
                                .withColumn("FixedAttribute_agentId", lit("")) \
                                .withColumn("FixedAttribute_agentDescription", lit("")) \
                                .select(
                                    "TouBucket", 
                                    "Direction", 
                                    "UOM", 
                                    "TimeStamp",
                                    "Meter_SdpIdent",
                                    "FixedAttribute_readingType",
                                    "Meter_MeterIrn",
                                    "FixedAttribute_meteringType",
                                    "FixedAttribute_readingUtcLocalTime",
                                    "FixedAttribute_readingDateSource",
                                    "FixedAttribute_dstStatus",
                                    "FixedAttribute_channel",
                                    "FixedAttribute_qualityCodesSystemId",
                                    "FixedAttribute_qualityCodesCategorization",
                                    "FixedAttribute_qualityCodesIndex",
                                    "FixedAttribute_intervalSize",
                                    "FixedAttribute_logNumber",
                                    "FixedAttribute_ct",
                                    "FixedAttribute_pt",
                                    "Multiplier",
                                    "FixedAttribute_sf",
                                    "FixedAttribute_version",
                                    "Value",
                                    "MeterReadings_Source", 
                                    "FixedAttribute_readingsSource",
                                    "FixedAttribute_owner",
                                    "FixedAttribute_guidFile",
                                    "FixedAttribute_estatus",
                                    "FixedAttribute_registersNumber",
                                    "FixedAttribute_eventsCode",
                                    "FixedAttribute_agentId",
                                    "FixedAttribute_agentDescription",
                                    "_SourceIrn",
                                    "Meter._Description",
                                    "Meter._IsActive",
                                    "Meter._SerialNumber",
                                    "Meter._MeterType",
                                    "Meter._AccountIdent",
                                    "Meter._TimeZoneIndex",
                                    "Meter._MediaType",
                                    "Meter._InstallDate",
                                    "Meter._RemovalDate")
    consumptionDataReadings = consumptionDataReadings.withColumn("tmp", arrays_zip("TouBucket", "Direction","UOM", "Value", "Multiplier","TimeStamp")) \
                                                    .withColumn("tmp", explode("tmp")) \
                                                    .withColumn("MeterReadings_Source", 
                                                            when(col("MeterReadings_Source") == "Visual", lit("Visual")) \
                                                            .when(col("MeterReadings_Source") == "Remote", lit("Remoto")) \
                                                            .when(col("MeterReadings_Source") == "LocalRF", lit("LAN")) \
                                                            .when(col("MeterReadings_Source") == "Optical", lit("Optical")) \
                                                            .when(col("MeterReadings_Source") == "Manually Estimated", lit("Visual")) \
                                                            .when(col("MeterReadings_Source") == "LegacySystem", lit("HES")) \
                                                            .otherwise(col("MeterReadings_Source"))) \
                                                    .withColumn("servicePointId", 
                                                            when(col("Meter_SdpIdent").isNull(), col("Meter_MeterIrn")) \
                                                            .otherwise(col("Meter_SdpIdent"))) \
                                                    .select(
                                                            col("servicePointId"),
                                                        col("FixedAttribute_readingType").alias("readingType"),
                                                        concat(col("tmp.UOM"),lit(" "),col("tmp.Direction"),lit(" "),col("tmp.TouBucket")).alias("variableId"),
                                                        col("Meter_MeterIrn").alias("deviceId"),
                                                        col("FixedAttribute_meteringType").alias("meteringType"),
                                                        col("FixedAttribute_readingUtcLocalTime").alias("readingUtcLocalTime"),
                                                        col("tmp.TimeStamp").alias("readingDateSource"),
                                                        col("tmp.TimeStamp").alias("readingLocalTime"),
                                                        col("FixedAttribute_dstStatus").alias("dstStatus"),
                                                        col("FixedAttribute_channel").alias("channel"),
                                                        col("tmp.UOM").alias("unitOfMeasure"),
                                                        col("FixedAttribute_qualityCodesSystemId").alias("qualityCodesSystemId"),
                                                        col("FixedAttribute_qualityCodesCategorization").alias("qualityCodesCategorization"),
                                                        col("FixedAttribute_qualityCodesIndex").alias("qualityCodesIndex"),
                                                        col("FixedAttribute_intervalSize").alias("intervalSize"),
                                                        col("FixedAttribute_logNumber").alias("logNumber"),
                                                        col("FixedAttribute_ct").alias("ct"),
                                                        col("FixedAttribute_pt").alias("pt"),
                                                        col("tmp.Multiplier").alias("ke"),
                                                        col("FixedAttribute_sf").alias("sf"),
                                                        col("FixedAttribute_version").alias("version"),
                                                        col("tmp.Value").alias("readingsValue"),
                                                        col("MeterReadings_Source").alias("primarySource"),
                                                        col("FixedAttribute_owner").alias("owner"),
                                                        col("FixedAttribute_guidFile").alias("guidFile"),
                                                        col("FixedAttribute_estatus").alias("estatus"),
                                                        col("FixedAttribute_registersNumber").alias("registersNumber"),
                                                        col("FixedAttribute_eventsCode").alias("eventsCode"),
                                                        col("FixedAttribute_agentId").alias("agentId"),
                                                        col("FixedAttribute_agentDescription").alias("agentDescription"),
                                                        col("_SourceIrn"),
                                                        col("_Description"),
                                                        col("_IsActive"),
                                                        col("_SerialNumber"),
                                                        col("_MeterType"),
                                                        col("_AccountIdent"),
                                                        col("_TimeZoneIndex"),
                                                        col("_MediaType"),
                                                        col("_InstallDate"),
                                                        col("_RemovalDate")) \
                                                    .withColumn("unitOfMeasure", 
                                                            when(col("unitOfMeasure") == 'Degree', lit("°")) \
                                                            .otherwise(col("unitOfMeasure")))
    ######################################################################################################################################################

    ######################################################################################################################################################
    coincidentDemandDataReadings = df.withColumn("TouBucket", col("CoincidentDemandData.CoincidentDemandSpec._TouBucket")) \
                                .withColumn("Direction", col("CoincidentDemandData.CoincidentDemandSpec._Direction")) \
                                .withColumn("UOM", col("CoincidentDemandData.CoincidentDemandSpec._UOM")) \
                                .withColumn("Multiplier", col("CoincidentDemandData.CoincidentDemandSpec._Multiplier")) \
                                .withColumn("Value", col("CoincidentDemandData.Reading._Value")) \
                                .withColumn("TimeStamp", col("CoincidentDemandData.Reading._TimeStamp")) \
                                .withColumn("MeterReadings_CollectionTime", col("_CollectionTime")) \
                                .withColumn("MeterReadings_Source", col("_Source")) \
                                .withColumn("Meter_SdpIdent", col("Meter._SdpIdent")) \
                                .withColumn("FixedAttribute_readingType", lit("REGISTERS")) \
                                .withColumn("Meter_MeterIrn", col("Meter._MeterIrn")) \
                                .withColumn("FixedAttribute_meteringType", lit("MAIN")) \
                                .withColumn("FixedAttribute_readingUtcLocalTime", lit("")) \
                                .withColumn("FixedAttribute_readingDateSource", lit("")) \
                                .withColumn("FixedAttribute_dstStatus", lit("")) \
                                .withColumn("FixedAttribute_channel", lit("")) \
                                .withColumn("FixedAttribute_qualityCodesSystemId", lit("")) \
                                .withColumn("FixedAttribute_qualityCodesCategorization", lit("")) \
                                .withColumn("FixedAttribute_qualityCodesIndex", lit("")) \
                                .withColumn("FixedAttribute_intervalSize", lit("")) \
                                .withColumn("FixedAttribute_logNumber", lit("")) \
                                .withColumn("FixedAttribute_ct", lit("")) \
                                .withColumn("FixedAttribute_pt", lit("")) \
                                .withColumn("FixedAttribute_sf", lit("")) \
                                .withColumn("FixedAttribute_version", lit("")) \
                                .withColumn("FixedAttribute_readingsSource", lit("")) \
                                .withColumn("FixedAttribute_owner", lit(owner)) \
                                .withColumn("FixedAttribute_guidFile", lit(guid)) \
                                .withColumn("FixedAttribute_estatus", lit("Activo")) \
                                .withColumn("FixedAttribute_registersNumber", lit("")) \
                                .withColumn("FixedAttribute_eventsCode", lit("")) \
                                .withColumn("FixedAttribute_agentId", lit("")) \
                                .withColumn("FixedAttribute_agentDescription", lit("")) \
                                .select(
                                    "TouBucket", 
                                    "Direction", 
                                    "UOM", 
                                    "TimeStamp",
                                    "MeterReadings_CollectionTime",
                                    "Meter_SdpIdent",
                                    "FixedAttribute_readingType",
                                    "Meter_MeterIrn",
                                    "FixedAttribute_meteringType",
                                    "FixedAttribute_readingUtcLocalTime",
                                    "FixedAttribute_readingDateSource",
                                    "FixedAttribute_dstStatus",
                                    "FixedAttribute_channel",
                                    "FixedAttribute_qualityCodesSystemId",
                                    "FixedAttribute_qualityCodesCategorization",
                                    "FixedAttribute_qualityCodesIndex",
                                    "FixedAttribute_intervalSize",
                                    "FixedAttribute_logNumber",
                                    "FixedAttribute_ct",
                                    "FixedAttribute_pt",
                                    "Multiplier",
                                    "FixedAttribute_sf",
                                    "FixedAttribute_version",
                                    "Value",
                                    "MeterReadings_Source", 
                                    "FixedAttribute_readingsSource",
                                    "FixedAttribute_owner",
                                    "FixedAttribute_guidFile",
                                    "FixedAttribute_estatus",
                                    "FixedAttribute_registersNumber",
                                    "FixedAttribute_eventsCode",
                                    "FixedAttribute_agentId",
                                    "FixedAttribute_agentDescription",
                                    "_SourceIrn",
                                    "Meter._Description",
                                    "Meter._IsActive",
                                    "Meter._SerialNumber",
                                    "Meter._MeterType",
                                    "Meter._AccountIdent",
                                    "Meter._TimeZoneIndex",
                                    "Meter._MediaType",
                                    "Meter._InstallDate",
                                    "Meter._RemovalDate") 
    coincidentDemandDataReadings = coincidentDemandDataReadings.withColumn("tmp", arrays_zip("TouBucket", "Direction","UOM", "Value", "Multiplier", "TimeStamp")) \
                                                                .withColumn("tmp", explode("tmp")) \
                                                                .withColumn("MeterReadings_Source", 
                                                                when(col("MeterReadings_Source") == "Visual", lit("Visual")) \
                                                                .when(col("MeterReadings_Source") == "Remote", lit("Remoto")) \
                                                                .when(col("MeterReadings_Source") == "LocalRF", lit("LAN")) \
                                                                .when(col("MeterReadings_Source") == "Optical", lit("Optical")) \
                                                                .when(col("MeterReadings_Source") == "Manually Estimated", lit("Visual")) \
                                                                .when(col("MeterReadings_Source") == "LegacySystem", lit("HES")) \
                                                                .otherwise(col("MeterReadings_Source"))) \
                                                                .withColumn("TimeStamp", 
                                                                            when(col("tmp.TimeStamp").isNull(), col("MeterReadings_CollectionTime")) \
                                                                            .otherwise(col("tmp.TimeStamp"))) \
                                                                .withColumn("servicePointId", 
                                                                            when(col("Meter_SdpIdent").isNull(), col("Meter_MeterIrn")) \
                                                                            .otherwise(col("Meter_SdpIdent"))) \
                                                                .select(
                                                                    col("servicePointId"),
                                                                    col("FixedAttribute_readingType").alias("readingType"),
                                                                    concat(col("tmp.UOM"),lit(" "),col("tmp.Direction"),lit(" "),col("tmp.TouBucket")).alias("variableId"),
                                                                    col("Meter_MeterIrn").alias("deviceId"),
                                                                    col("FixedAttribute_meteringType").alias("meteringType"),
                                                                    col("FixedAttribute_readingUtcLocalTime").alias("readingUtcLocalTime"),
                                                                    col("TimeStamp").alias("readingDateSource"),
                                                                    col("TimeStamp").alias("readingLocalTime"),
                                                                    col("FixedAttribute_dstStatus").alias("dstStatus"),
                                                                    col("FixedAttribute_channel").alias("channel"),
                                                                    col("tmp.UOM").alias("unitOfMeasure"),
                                                                    col("FixedAttribute_qualityCodesSystemId").alias("qualityCodesSystemId"),
                                                                    col("FixedAttribute_qualityCodesCategorization").alias("qualityCodesCategorization"),
                                                                    col("FixedAttribute_qualityCodesIndex").alias("qualityCodesIndex"),
                                                                    col("FixedAttribute_intervalSize").alias("intervalSize"),
                                                                    col("FixedAttribute_logNumber").alias("logNumber"),
                                                                    col("FixedAttribute_ct").alias("ct"),
                                                                    col("FixedAttribute_pt").alias("pt"),
                                                                    col("tmp.Multiplier").alias("ke"),
                                                                    col("FixedAttribute_sf").alias("sf"),
                                                                    col("FixedAttribute_version").alias("version"),
                                                                    col("tmp.Value").alias("readingsValue"),
                                                                    col("MeterReadings_Source").alias("primarySource"),
                                                                    col("FixedAttribute_owner").alias("owner"),
                                                                    col("FixedAttribute_guidFile").alias("guidFile"),
                                                                    col("FixedAttribute_estatus").alias("estatus"),
                                                                    col("FixedAttribute_registersNumber").alias("registersNumber"),
                                                                    col("FixedAttribute_eventsCode").alias("eventsCode"),
                                                                    col("FixedAttribute_agentId").alias("agentId"),
                                                                    col("FixedAttribute_agentDescription").alias("agentDescription"),
                                                                    col("_SourceIrn"),
                                                                    col("_Description"),
                                                                    col("_IsActive"),
                                                                    col("_SerialNumber"),
                                                                    col("_MeterType"),
                                                                    col("_AccountIdent"),
                                                                    col("_TimeZoneIndex"),
                                                                    col("_MediaType"),
                                                                    col("_InstallDate"),
                                                                    col("_RemovalDate"))

    ######################################################################################################################################################

    ######################################################################################################################################################
    cumulativeDemandDataReadings = df.withColumn("TouBucket", col("CumulativeDemandData.CumulativeDemandSpec._TouBucket")) \
                                .withColumn("Direction", col("CumulativeDemandData.CumulativeDemandSpec._Direction")) \
                                .withColumn("UOM", col("CumulativeDemandData.CumulativeDemandSpec._UOM")) \
                                .withColumn("Multiplier", col("CumulativeDemandData.CumulativeDemandSpec._Multiplier")) \
                                .withColumn("Value", col("CumulativeDemandData.Reading._Value")) \
                                .withColumn("TimeStamp", col("CumulativeDemandData.Reading._TimeStamp")) \
                                .withColumn("MeterReadings_CollectionTime", col("_CollectionTime")) \
                                .withColumn("MeterReadings_Source", col("_Source")) \
                                .withColumn("Meter_SdpIdent", col("Meter._SdpIdent")) \
                                .withColumn("FixedAttribute_readingType", lit("REGISTERS")) \
                                .withColumn("Meter_MeterIrn", col("Meter._MeterIrn")) \
                                .withColumn("FixedAttribute_meteringType", lit("MAIN")) \
                                .withColumn("FixedAttribute_readingUtcLocalTime", lit("")) \
                                .withColumn("FixedAttribute_readingDateSource", lit("")) \
                                .withColumn("FixedAttribute_dstStatus", lit("")) \
                                .withColumn("FixedAttribute_channel", lit("")) \
                                .withColumn("FixedAttribute_qualityCodesSystemId", lit("")) \
                                .withColumn("FixedAttribute_qualityCodesCategorization", lit("")) \
                                .withColumn("FixedAttribute_qualityCodesIndex", lit("")) \
                                .withColumn("FixedAttribute_intervalSize", lit("")) \
                                .withColumn("FixedAttribute_logNumber", lit("")) \
                                .withColumn("FixedAttribute_ct", lit("")) \
                                .withColumn("FixedAttribute_pt", lit("")) \
                                .withColumn("FixedAttribute_sf", lit("")) \
                                .withColumn("FixedAttribute_version", lit("")) \
                                .withColumn("FixedAttribute_readingsSource", lit("")) \
                                .withColumn("FixedAttribute_owner", lit(owner)) \
                                .withColumn("FixedAttribute_guidFile", lit(guid)) \
                                .withColumn("FixedAttribute_estatus", lit("Activo")) \
                                .withColumn("FixedAttribute_registersNumber", lit("")) \
                                .withColumn("FixedAttribute_eventsCode", lit("")) \
                                .withColumn("FixedAttribute_agentId", lit("")) \
                                .withColumn("FixedAttribute_agentDescription", lit("")) \
                                .select(
                                    "TouBucket", 
                                    "Direction", 
                                    "UOM", 
                                    "TimeStamp",
                                    "MeterReadings_CollectionTime",
                                    "Meter_SdpIdent",
                                    "FixedAttribute_readingType",
                                    "Meter_MeterIrn",
                                    "FixedAttribute_meteringType",
                                    "FixedAttribute_readingUtcLocalTime",
                                    "FixedAttribute_readingDateSource",
                                    "FixedAttribute_dstStatus",
                                    "FixedAttribute_channel",
                                    "FixedAttribute_qualityCodesSystemId",
                                    "FixedAttribute_qualityCodesCategorization",
                                    "FixedAttribute_qualityCodesIndex",
                                    "FixedAttribute_intervalSize",
                                    "FixedAttribute_logNumber",
                                    "FixedAttribute_ct",
                                    "FixedAttribute_pt",
                                    "Multiplier",
                                    "FixedAttribute_sf",
                                    "FixedAttribute_version",
                                    "Value",
                                    "MeterReadings_Source", 
                                    "FixedAttribute_readingsSource",
                                    "FixedAttribute_owner",
                                    "FixedAttribute_guidFile",
                                    "FixedAttribute_estatus",
                                    "FixedAttribute_registersNumber",
                                    "FixedAttribute_eventsCode",
                                    "FixedAttribute_agentId",
                                    "FixedAttribute_agentDescription",
                                    "_SourceIrn",
                                    "Meter._Description",
                                    "Meter._IsActive",
                                    "Meter._SerialNumber",
                                    "Meter._MeterType",
                                    "Meter._AccountIdent",
                                    "Meter._TimeZoneIndex",
                                    "Meter._MediaType",
                                    "Meter._InstallDate",
                                    "Meter._RemovalDate") 
    cumulativeDemandDataReadings = cumulativeDemandDataReadings.withColumn("tmp", arrays_zip("TouBucket", "Direction","UOM", "Value", "Multiplier","TimeStamp")) \
                                                                .withColumn("tmp", explode("tmp")) \
                                                                .withColumn("MeterReadings_Source", 
                                                                when(col("MeterReadings_Source") == "Visual", lit("Visual")) \
                                                                .when(col("MeterReadings_Source") == "Remote", lit("Remoto")) \
                                                                .when(col("MeterReadings_Source") == "LocalRF", lit("LAN")) \
                                                                .when(col("MeterReadings_Source") == "Optical", lit("Optical")) \
                                                                .when(col("MeterReadings_Source") == "Manually Estimated", lit("Visual")) \
                                                                .when(col("MeterReadings_Source") == "LegacySystem", lit("HES")) \
                                                                .otherwise(col("MeterReadings_Source"))) \
                                                                .withColumn("TimeStamp", 
                                                                            when(col("tmp.TimeStamp").isNull(), col("MeterReadings_CollectionTime")) \
                                                                            .otherwise(col("tmp.TimeStamp"))) \
                                                                .withColumn("servicePointId", 
                                                                            when(col("Meter_SdpIdent").isNull(), col("Meter_MeterIrn")) \
                                                                            .otherwise(col("Meter_SdpIdent"))) \
                                                                .select(
                                                                    col("servicePointId"),
                                                                    col("FixedAttribute_readingType").alias("readingType"),
                                                                    concat(col("tmp.UOM"),lit(" "),col("tmp.Direction"),lit(" "),col("tmp.TouBucket")).alias("variableId"),
                                                                    col("Meter_MeterIrn").alias("deviceId"),
                                                                    col("FixedAttribute_meteringType").alias("meteringType"),
                                                                    col("FixedAttribute_readingUtcLocalTime").alias("readingUtcLocalTime"),
                                                                    col("TimeStamp").alias("readingDateSource"),
                                                                    col("TimeStamp").alias("readingLocalTime"),
                                                                    col("FixedAttribute_dstStatus").alias("dstStatus"),
                                                                    col("FixedAttribute_channel").alias("channel"),
                                                                    col("tmp.UOM").alias("unitOfMeasure"),
                                                                    col("FixedAttribute_qualityCodesSystemId").alias("qualityCodesSystemId"),
                                                                    col("FixedAttribute_qualityCodesCategorization").alias("qualityCodesCategorization"),
                                                                    col("FixedAttribute_qualityCodesIndex").alias("qualityCodesIndex"),
                                                                    col("FixedAttribute_intervalSize").alias("intervalSize"),
                                                                    col("FixedAttribute_logNumber").alias("logNumber"),
                                                                    col("FixedAttribute_ct").alias("ct"),
                                                                    col("FixedAttribute_pt").alias("pt"),
                                                                    col("tmp.Multiplier").alias("ke"),
                                                                    col("FixedAttribute_sf").alias("sf"),
                                                                    col("FixedAttribute_version").alias("version"),
                                                                    col("tmp.Value").alias("readingsValue"),
                                                                    col("MeterReadings_Source").alias("primarySource"),
                                                                    col("FixedAttribute_owner").alias("owner"),
                                                                    col("FixedAttribute_guidFile").alias("guidFile"),
                                                                    col("FixedAttribute_estatus").alias("estatus"),
                                                                    col("FixedAttribute_registersNumber").alias("registersNumber"),
                                                                    col("FixedAttribute_eventsCode").alias("eventsCode"),
                                                                    col("FixedAttribute_agentId").alias("agentId"),
                                                                    col("FixedAttribute_agentDescription").alias("agentDescription"),
                                                                    col("_SourceIrn"),
                                                                    col("_Description"),
                                                                    col("_IsActive"),
                                                                    col("_SerialNumber"),
                                                                    col("_MeterType"),
                                                                    col("_AccountIdent"),
                                                                    col("_TimeZoneIndex"),
                                                                    col("_MediaType"),
                                                                    col("_InstallDate"),
                                                                    col("_RemovalDate"))

    ######################################################################################################################################################

    ######################################################################################################################################################
    demandResetReadings = df.withColumn("TimeStamp", col("DemandReset._TimeStamp")) \
                                .withColumn("FixedAttribute_readingsValue", lit("0")) \
                                .withColumn("FixedAttribute_unitOfMeasure", lit("-")) \
                                .withColumn("FixedAttribute_variableId", lit("Demand Reset")) \
                                .withColumn("MeterReadings_Source", col("_Source")) \
                                .withColumn("Meter_SdpIdent", col("Meter._SdpIdent")) \
                                .withColumn("FixedAttribute_readingType", lit("EVENTS")) \
                                .withColumn("Meter_MeterIrn", col("Meter._MeterIrn")) \
                                .withColumn("FixedAttribute_meteringType", lit("MAIN")) \
                                .withColumn("FixedAttribute_readingUtcLocalTime", lit("")) \
                                .withColumn("FixedAttribute_readingDateSource", lit("")) \
                                .withColumn("FixedAttribute_dstStatus", lit("")) \
                                .withColumn("FixedAttribute_channel", lit("")) \
                                .withColumn("FixedAttribute_qualityCodesSystemId", lit("")) \
                                .withColumn("FixedAttribute_qualityCodesCategorization", lit("")) \
                                .withColumn("FixedAttribute_qualityCodesIndex", lit("")) \
                                .withColumn("FixedAttribute_intervalSize", lit("")) \
                                .withColumn("FixedAttribute_logNumber", lit("")) \
                                .withColumn("FixedAttribute_ct", lit("")) \
                                .withColumn("FixedAttribute_pt", lit("")) \
                                .withColumn("FixedAttribute_ke", lit("")) \
                                .withColumn("FixedAttribute_sf", lit("")) \
                                .withColumn("FixedAttribute_version", lit("")) \
                                .withColumn("FixedAttribute_readingsSource", lit("")) \
                                .withColumn("FixedAttribute_owner", lit(owner)) \
                                .withColumn("FixedAttribute_guidFile", lit(guid)) \
                                .withColumn("FixedAttribute_estatus", lit("Activo")) \
                                .withColumn("FixedAttribute_registersNumber", lit("")) \
                                .withColumn("FixedAttribute_eventsCode", lit("")) \
                                .withColumn("FixedAttribute_agentId", lit("")) \
                                .withColumn("FixedAttribute_agentDescription", lit("")) \
                                .select(
                                        "TimeStamp", 
                                        "FixedAttribute_readingsValue",
                                        "FixedAttribute_unitOfMeasure",
                                        "FixedAttribute_variableId",
                                        "MeterReadings_Source",
                                        "Meter_SdpIdent",
                                        "FixedAttribute_readingType",
                                        "Meter_MeterIrn",
                                        "FixedAttribute_meteringType",
                                        "FixedAttribute_readingUtcLocalTime",
                                        "FixedAttribute_readingDateSource",
                                        "FixedAttribute_dstStatus",
                                        "FixedAttribute_channel",
                                        "FixedAttribute_qualityCodesSystemId",
                                        "FixedAttribute_qualityCodesCategorization",
                                        "FixedAttribute_qualityCodesIndex",
                                        "FixedAttribute_intervalSize",
                                        "FixedAttribute_logNumber",
                                        "FixedAttribute_ct",
                                        "FixedAttribute_pt",
                                        "FixedAttribute_ke",
                                        "FixedAttribute_sf",
                                        "FixedAttribute_version",
                                        "FixedAttribute_readingsSource",
                                        "FixedAttribute_owner",
                                        "FixedAttribute_guidFile",
                                        "FixedAttribute_estatus",
                                        "FixedAttribute_registersNumber",
                                        "FixedAttribute_eventsCode",
                                        "FixedAttribute_agentId",
                                        "FixedAttribute_agentDescription",
                                        "_SourceIrn",
                                        "Meter._Description",
                                        "Meter._IsActive",
                                        "Meter._SerialNumber",
                                        "Meter._MeterType",
                                        "Meter._AccountIdent",
                                        "Meter._TimeZoneIndex",
                                        "Meter._MediaType",
                                        "Meter._InstallDate",
                                        "Meter._RemovalDate")
    demandResetReadings = demandResetReadings.withColumn("tmp", arrays_zip("TimeStamp")) \
                                                .withColumn("tmp", explode("tmp")) \
                                                .withColumn("MeterReadings_Source", 
                                                when(col("MeterReadings_Source") == "Visual", lit("Visual")) \
                                                .when(col("MeterReadings_Source") == "Remote", lit("Remoto")) \
                                                .when(col("MeterReadings_Source") == "LocalRF", lit("LAN")) \
                                                .when(col("MeterReadings_Source") == "Optical", lit("Optical")) \
                                                .when(col("MeterReadings_Source") == "Manually Estimated", lit("Visual")) \
                                                .when(col("MeterReadings_Source") == "LegacySystem", lit("HES")) \
                                                .otherwise(col("MeterReadings_Source"))) \
                                                .withColumn("servicePointId", 
                                                            when(col("Meter_SdpIdent").isNull(), col("Meter_MeterIrn")) \
                                                            .otherwise(col("Meter_SdpIdent"))) \
                                                .select(
                                                        col("servicePointId"),
                                                        col("FixedAttribute_readingType").alias("readingType"),
                                                        col("FixedAttribute_variableId").alias("variableId"),
                                                        col("Meter_MeterIrn").alias("deviceId"),
                                                        col("FixedAttribute_meteringType").alias("meteringType"),
                                                        col("FixedAttribute_readingUtcLocalTime").alias("readingUtcLocalTime"),
                                                        col("tmp.TimeStamp").alias("readingDateSource"),
                                                        col("tmp.TimeStamp").alias("readingLocalTime"),
                                                        col("FixedAttribute_dstStatus").alias("dstStatus"),
                                                        col("FixedAttribute_channel").alias("channel"),
                                                        col("FixedAttribute_unitOfMeasure").alias("unitOfMeasure"),
                                                        col("FixedAttribute_qualityCodesSystemId").alias("qualityCodesSystemId"),
                                                        col("FixedAttribute_qualityCodesCategorization").alias("qualityCodesCategorization"),
                                                        col("FixedAttribute_qualityCodesIndex").alias("qualityCodesIndex"),
                                                        col("FixedAttribute_intervalSize").alias("intervalSize"),
                                                        col("FixedAttribute_logNumber").alias("logNumber"),
                                                        col("FixedAttribute_ct").alias("ct"),
                                                        col("FixedAttribute_pt").alias("pt"),
                                                        col("FixedAttribute_ke").alias("ke"),
                                                        col("FixedAttribute_sf").alias("sf"),
                                                        col("FixedAttribute_version").alias("version"),
                                                        col("FixedAttribute_readingsValue").alias("readingsValue"),
                                                        col("MeterReadings_Source").alias("primarySource"),
                                                        col("FixedAttribute_owner").alias("owner"),
                                                        col("FixedAttribute_guidFile").alias("guidFile"),
                                                        col("FixedAttribute_estatus").alias("estatus"),
                                                        col("FixedAttribute_registersNumber").alias("registersNumber"),
                                                        col("FixedAttribute_eventsCode").alias("eventsCode"),
                                                        col("FixedAttribute_agentId").alias("agentId"),
                                                        col("FixedAttribute_agentDescription").alias("agentDescription"),
                                                        col("_SourceIrn"),
                                                        col("_Description"),
                                                        col("_IsActive"),
                                                        col("_SerialNumber"),
                                                        col("_MeterType"),
                                                        col("_AccountIdent"),
                                                        col("_TimeZoneIndex"),
                                                        col("_MediaType"),
                                                        col("_InstallDate"),
                                                        col("_RemovalDate"))
    ######################################################################################################################################################

    ######################################################################################################################################################
    instrumentationValueReadings = df.withColumn("TimeStamp", col("InstrumentationValue._TimeStamp")) \
                                    .withColumn("Name", col("InstrumentationValue._Name")) \
                                    .withColumn("Phase", col("InstrumentationValue._Phase")) \
                                    .withColumn("Value", col("InstrumentationValue._Value")) \
                                    .withColumn("MeterReadings_Source", col("_Source")) \
                                    .withColumn("Meter_SdpIdent", col("Meter._SdpIdent")) \
                                    .withColumn("FixedAttribute_readingType", lit("REGISTERS")) \
                                    .withColumn("Meter_MeterIrn", col("Meter._MeterIrn")) \
                                    .withColumn("FixedAttribute_meteringType", lit("MAIN")) \
                                    .withColumn("FixedAttribute_readingUtcLocalTime", lit("")) \
                                    .withColumn("FixedAttribute_readingDateSource", lit("")) \
                                    .withColumn("FixedAttribute_dstStatus", lit("")) \
                                    .withColumn("FixedAttribute_channel", lit("")) \
                                    .withColumn("FixedAttribute_qualityCodesSystemId", lit("")) \
                                    .withColumn("FixedAttribute_qualityCodesCategorization", lit("")) \
                                    .withColumn("FixedAttribute_qualityCodesIndex", lit("")) \
                                    .withColumn("FixedAttribute_intervalSize", lit("")) \
                                    .withColumn("FixedAttribute_logNumber", lit("")) \
                                    .withColumn("FixedAttribute_ct", lit("")) \
                                    .withColumn("FixedAttribute_pt", lit("")) \
                                    .withColumn("FixedAttribute_ke", lit("")) \
                                    .withColumn("FixedAttribute_sf", lit("")) \
                                    .withColumn("FixedAttribute_version", lit("")) \
                                    .withColumn("FixedAttribute_readingsSource", lit("")) \
                                    .withColumn("FixedAttribute_owner", lit(owner)) \
                                    .withColumn("FixedAttribute_guidFile", lit(guid)) \
                                    .withColumn("FixedAttribute_estatus", lit("Activo")) \
                                    .withColumn("FixedAttribute_registersNumber", lit("")) \
                                    .withColumn("FixedAttribute_eventsCode", lit("")) \
                                    .withColumn("FixedAttribute_agentId", lit("")) \
                                    .withColumn("FixedAttribute_agentDescription", lit("")) \
                                    .select(
                                            "TimeStamp", 
                                            "Phase",
                                            "Name",
                                            "Value",
                                            "MeterReadings_Source",
                                            "Meter_SdpIdent",
                                            "FixedAttribute_readingType",
                                            "Meter_MeterIrn",
                                            "FixedAttribute_meteringType",
                                            "FixedAttribute_readingUtcLocalTime",
                                            "FixedAttribute_readingDateSource",
                                            "FixedAttribute_dstStatus",
                                            "FixedAttribute_channel",
                                            "FixedAttribute_qualityCodesSystemId",
                                            "FixedAttribute_qualityCodesCategorization",
                                            "FixedAttribute_qualityCodesIndex",
                                            "FixedAttribute_intervalSize",
                                            "FixedAttribute_logNumber",
                                            "FixedAttribute_ct",
                                            "FixedAttribute_pt",
                                            "FixedAttribute_ke",
                                            "FixedAttribute_sf",
                                            "FixedAttribute_version",
                                            "FixedAttribute_readingsSource",
                                            "FixedAttribute_owner",
                                            "FixedAttribute_guidFile",
                                            "FixedAttribute_estatus",
                                            "FixedAttribute_registersNumber",
                                            "FixedAttribute_eventsCode",
                                            "FixedAttribute_agentId",
                                            "FixedAttribute_agentDescription",
                                            "_SourceIrn",
                                            "Meter._Description",
                                            "Meter._IsActive",
                                            "Meter._SerialNumber",
                                            "Meter._MeterType",
                                            "Meter._AccountIdent",
                                            "Meter._TimeZoneIndex",
                                            "Meter._MediaType",
                                            "Meter._InstallDate",
                                            "Meter._RemovalDate")
    instrumentationValueReadings = instrumentationValueReadings.withColumn("tmp", arrays_zip("TimeStamp","Phase","Name","Value")) \
                                                                .withColumn("tmp", explode("tmp")) \
                                                                .withColumn("MeterReadings_Source", 
                                                                when(col("MeterReadings_Source") == "Visual", lit("Visual")) \
                                                                .when(col("MeterReadings_Source") == "Remote", lit("Remoto")) \
                                                                .when(col("MeterReadings_Source") == "LocalRF", lit("LAN")) \
                                                                .when(col("MeterReadings_Source") == "Optical", lit("Optical")) \
                                                                .when(col("MeterReadings_Source") == "Manually Estimated", lit("Visual")) \
                                                                .when(col("MeterReadings_Source") == "LegacySystem", lit("HES")) \
                                                                .otherwise(col("MeterReadings_Source"))) \
                                                                .withColumn("Translated_Name", 
                                                                            when(col("tmp.Name") == "Current", "A") \
                                                                            .when(col("tmp.Name") == "Voltage", "V") \
                                                                            .when(col("tmp.Name") == "Power Factor Angle", "°") \
                                                                            .when(col("tmp.Name") == "Frequency", "Hz") \
                                                                            .otherwise(lit("-")) \
                                                                ) \
                                                                .withColumn("servicePointId", 
                                                                            when(col("Meter_SdpIdent").isNull(), col("Meter_MeterIrn")) \
                                                                            .otherwise(col("Meter_SdpIdent"))) \
                                                                .select(
                                                                        col("servicePointId"),
                                                                        col("FixedAttribute_readingType").alias("readingType"),
                                                                        concat(col("tmp.Name"),lit(" "),col("tmp.Phase")).alias("variableId"),
                                                                        col("Meter_MeterIrn").alias("deviceId"),
                                                                        col("FixedAttribute_meteringType").alias("meteringType"),
                                                                        col("FixedAttribute_readingUtcLocalTime").alias("readingUtcLocalTime"),
                                                                        col("tmp.TimeStamp").alias("readingDateSource"),
                                                                        col("tmp.TimeStamp").alias("readingLocalTime"),
                                                                        col("FixedAttribute_dstStatus").alias("dstStatus"),
                                                                        col("FixedAttribute_channel").alias("channel"),
                                                                        col("Translated_Name").alias("unitOfMeasure"),
                                                                        col("FixedAttribute_qualityCodesSystemId").alias("qualityCodesSystemId"),
                                                                        col("FixedAttribute_qualityCodesCategorization").alias("qualityCodesCategorization"),
                                                                        col("FixedAttribute_qualityCodesIndex").alias("qualityCodesIndex"),
                                                                        col("FixedAttribute_intervalSize").alias("intervalSize"),
                                                                        col("FixedAttribute_logNumber").alias("logNumber"),
                                                                        col("FixedAttribute_ct").alias("ct"),
                                                                        col("FixedAttribute_pt").alias("pt"),
                                                                        col("FixedAttribute_ke").alias("ke"),
                                                                        col("FixedAttribute_sf").alias("sf"),
                                                                        col("FixedAttribute_version").alias("version"),
                                                                        col("tmp.Value").alias("readingsValue"),
                                                                        col("MeterReadings_Source").alias("primarySource"),
                                                                        col("FixedAttribute_owner").alias("owner"),
                                                                        col("FixedAttribute_guidFile").alias("guidFile"),
                                                                        col("FixedAttribute_estatus").alias("estatus"),
                                                                        col("FixedAttribute_registersNumber").alias("registersNumber"),
                                                                        col("FixedAttribute_eventsCode").alias("eventsCode"),
                                                                        col("FixedAttribute_agentId").alias("agentId"),
                                                                        col("FixedAttribute_agentDescription").alias("agentDescription"),
                                                                        col("_SourceIrn"),
                                                                        col("_Description"),
                                                                        col("_IsActive"),
                                                                        col("_SerialNumber"),
                                                                        col("_MeterType"),
                                                                        col("_AccountIdent"),
                                                                        col("_TimeZoneIndex"),
                                                                        col("_MediaType"),
                                                                        col("_InstallDate"),
                                                                        col("_RemovalDate"))
    ######################################################################################################################################################

    ######################################################################################################################################################

    StatusData = []
    for row in df.rdd.collect():
            if row.Meter != None:
                    MeterReadings_Source = row._Source
                    MeterReadings_SourceIrn = row._SourceIrn
                    MeterReadings_CollectionTime = row._CollectionTime
                    Meter_Irn = row.Meter._MeterIrn
                    Meter_SdpIdent = row.Meter._SdpIdent
                    Meter_Description = row.Meter._Description
                    Meter_IsActive = row.Meter._IsActive
                    Meter_SerialNumber = row.Meter._SerialNumber
                    Meter_MeterType = row.Meter._MeterType
                    Meter_AccountIdent = row.Meter._AccountIdent
                    Meter_TimeZoneIndex = row.Meter._TimeZoneIndex
                    Meter_MediaType = row.Meter._MediaType
                    Meter_InstallDate = row.Meter._InstallDate
                    Meter_RemovalDate = row.Meter._RemovalDate
                    if row.Statuses != None:
                            for statuses in row.Statuses:
                                    for status in statuses.Status:
                                            Status_Id = status._Id
                                            Status_Category = status._Category
                                            Status_Name = status._Name
                                            Status_Value = status._Value

                                            servicePointId = Meter_SdpIdent
                                            if servicePointId == None:
                                                    servicePointId = Meter_Irn

                                            if Status_Value == "true": #Esta lectura solo se toma si Value=0 o magnitud
                                                    Status_Value = 0
                                            if Status_Value != "false":
                                                    StatusData.append ({
                                                            "servicePointId":servicePointId,
                                                            "readingType":"EVENTS",
                                                            "variableId":Status_Category + ' ' + Status_Name,
                                                            "deviceId": Meter_Irn,
                                                            "meteringType": "MAIN",
                                                            "readingUtcLocalTime": "",
                                                            "readingDateSource": MeterReadings_CollectionTime,
                                                            "readingLocalTime": MeterReadings_CollectionTime,
                                                            "dstStatus":"",
                                                            "channel": "",
                                                            "unitOfMeasure":"-",
                                                            "qualityCodesSystemId":"",
                                                            "qualityCodesCategorization":"",
                                                            "qualityCodesIndex":"",
                                                            "intervalSize":"",
                                                            "logNumber": "1",
                                                            "ct":"",
                                                            "pt":"",
                                                            "ke":"",
                                                            "sf":"",
                                                            "version":"Original",
                                                            "readingsValue": Status_Value,
                                                            "primarySource":MeterReadings_Source,
                                                            "owner":owner,
                                                            "guidFile":guid,
                                                            "estatus": "Activo",
                                                            "registersNumber":"",
                                                            "eventsCode":Status_Id,
                                                            "agentId":"",
                                                            "agentDescription":"",
                                                            "_SourceIrn":str(MeterReadings_SourceIrn),
                                                            "_Description":str(Meter_Description),
                                                            "_IsActive":Meter_IsActive,
                                                            "_SerialNumber":str(Meter_SerialNumber),
                                                            "_MeterType":str(Meter_MeterType),
                                                            "_AccountIdent":str(Meter_AccountIdent),
                                                            "_TimeZoneIndex":str(Meter_TimeZoneIndex),
                                                            "_MediaType":str(Meter_MediaType),
                                                            "_InstallDate":str(Meter_InstallDate),
                                                            "_RemovalDate":str(Meter_RemovalDate)
                                                    })

    StatusData = spark.sparkContext.parallelize(StatusData) \
                            .map(lambda x: Row(**OrderedDict(x.items())))

    if StatusData.isEmpty() == False:
            statusReadings = spark.createDataFrame(StatusData.coalesce(1)) \
                            .withColumn("primarySource", 
                                    when(col("primarySource") == "Visual", lit("Visual")) \
                                    .when(col("primarySource") == "Remote", lit("Remoto")) \
                                    .when(col("primarySource") == "LocalRF", lit("LAN")) \
                                    .when(col("primarySource") == "Optical", lit("Optical")) \
                                    .when(col("primarySource") == "Manually Estimated", lit("Visual")) \
                                    .when(col("primarySource") == "LegacySystem", lit("HES")) \
                                    .otherwise(col("primarySource"))) \
                            .withColumn("_SourceIrne",
                                    when (col("_SourceIrn")=="None", lit(""))
                                    .otherwise(col("_SourceIrn")))\
                            .withColumn("_Description",
                                    when (col("_Description")=="None", lit(""))
                                    .otherwise(col("_Description")))\
                            .withColumn("_SerialNumber",
                                    when (col("_SerialNumber")=="None", lit(""))
                                    .otherwise(col("_SerialNumber")))\
                            .withColumn("_MeterType",
                                    when (col("_MeterType")=="None", lit(""))
                                    .otherwise(col("_MeterType")))\
                            .withColumn("_AccountIdent",
                                    when (col("_AccountIdent")=="None", lit(""))
                                    .otherwise(col("_AccountIdent")))\
                            .withColumn("_TimeZoneIndex",
                                    when (col("_TimeZoneIndex")=="None", lit(""))
                                    .otherwise(col("_TimeZoneIndex")))\
                            .withColumn("_MediaType",
                                    when (col("_MediaType")=="None", lit(""))
                                    .otherwise(col("_MediaType")))\
                            .withColumn("_InstallDate",
                                    when (col("_InstallDate")=="None", lit(""))
                                    .otherwise(col("_InstallDate")))\
                            .withColumn("_RemovalDate",
                                    when (col("_RemovalDate")=="None", lit(""))
                                    .otherwise(col("_RemovalDate")))\
                            .select(
                            "servicePointId",
                            "readingType",
                            "variableId",
                            "deviceId",
                            "meteringType",
                            "readingUtcLocalTime",
                            "readingDateSource",
                            "readingLocalTime",
                            "dstStatus",
                            "channel",
                            "unitOfMeasure",
                            "qualityCodesSystemId",
                            "qualityCodesCategorization",
                            "qualityCodesIndex",
                            "intervalSize",
                            "logNumber",
                            "ct",
                            "pt",
                            "ke",
                            "sf",
                            "version",
                            "readingsValue",
                            "primarySource",
                            "owner",
                            "guidFile",
                            "estatus",
                            "registersNumber",
                            "eventsCode",
                            "agentId",
                            "agentDescription",
                            "_SourceIrn",
                            "_Description",
                            "_IsActive",
                            "_SerialNumber",
                            "_MeterType",
                            "_AccountIdent",
                            "_TimeZoneIndex",
                            "_MediaType",
                            "_InstallDate",
                            "_RemovalDate"
                            )

    ######################################################################################################################################################

    ######################################################################################################################################################
    loadProfileSummaryReadings = df.withColumn("UOM", col("LoadProfileSummary.Channel._UOM")) \
                                    .withColumn("Direction", col("LoadProfileSummary.Channel._Direction")) \
                                    .withColumn("SumOfIntervalValues", col("LoadProfileSummary.Channel._SumOfIntervalValues")) \
                                    .withColumn("Multiplier", col("LoadProfileSummary.Channel._Multiplier")) \
                                    .withColumn("FixedAttribute_readingLocalTime", lit("")) \
                                    .withColumn("MeterReadings_Source", col("_Source")) \
                                    .withColumn("Meter_SdpIdent", col("Meter._SdpIdent")) \
                                    .withColumn("FixedAttribute_readingType", lit("REGISTERS")) \
                                    .withColumn("Meter_MeterIrn", col("Meter._MeterIrn")) \
                                    .withColumn("FixedAttribute_meteringType", lit("MAIN")) \
                                    .withColumn("FixedAttribute_readingUtcLocalTime", lit("")) \
                                    .withColumn("FixedAttribute_readingDateSource", lit("")) \
                                    .withColumn("FixedAttribute_dstStatus", lit("")) \
                                    .withColumn("FixedAttribute_channel", lit("")) \
                                    .withColumn("FixedAttribute_qualityCodesSystemId", lit("")) \
                                    .withColumn("FixedAttribute_qualityCodesCategorization", lit("")) \
                                    .withColumn("FixedAttribute_qualityCodesIndex", lit("")) \
                                    .withColumn("FixedAttribute_intervalSize", lit("")) \
                                    .withColumn("FixedAttribute_logNumber", lit("")) \
                                    .withColumn("FixedAttribute_ct", lit("")) \
                                    .withColumn("FixedAttribute_pt", lit("")) \
                                    .withColumn("FixedAttribute_sf", lit("")) \
                                    .withColumn("FixedAttribute_version", lit("")) \
                                    .withColumn("FixedAttribute_readingsSource", lit("")) \
                                    .withColumn("FixedAttribute_owner", lit(owner)) \
                                    .withColumn("FixedAttribute_guidFile", lit(guid)) \
                                    .withColumn("FixedAttribute_estatus", lit("Activo")) \
                                    .withColumn("FixedAttribute_registersNumber", lit("")) \
                                    .withColumn("FixedAttribute_eventsCode", lit("")) \
                                    .withColumn("FixedAttribute_agentId", lit("")) \
                                    .withColumn("FixedAttribute_agentDescription", lit("")) \
                                    .select(
                                            "UOM", 
                                            "Direction",
                                            "SumOfIntervalValues",
                                            "Multiplier",
                                            "FixedAttribute_readingLocalTime",
                                            "MeterReadings_Source",
                                            "Meter_SdpIdent",
                                            "FixedAttribute_readingType",
                                            "Meter_MeterIrn",
                                            "FixedAttribute_meteringType",
                                            "FixedAttribute_readingUtcLocalTime",
                                            "FixedAttribute_readingDateSource",
                                            "FixedAttribute_dstStatus",
                                            "FixedAttribute_channel",
                                            "FixedAttribute_qualityCodesSystemId",
                                            "FixedAttribute_qualityCodesCategorization",
                                            "FixedAttribute_qualityCodesIndex",
                                            "FixedAttribute_intervalSize",
                                            "FixedAttribute_logNumber",
                                            "FixedAttribute_ct",
                                            "FixedAttribute_pt",
                                            "FixedAttribute_sf",
                                            "FixedAttribute_version",
                                            "FixedAttribute_readingsSource",
                                            "FixedAttribute_owner",
                                            "FixedAttribute_guidFile",
                                            "FixedAttribute_estatus",
                                            "FixedAttribute_registersNumber",
                                            "FixedAttribute_eventsCode",
                                            "FixedAttribute_agentId",
                                            "FixedAttribute_agentDescription",
                                            "_SourceIrn",
                                            "Meter._Description",
                                            "Meter._IsActive",
                                            "Meter._SerialNumber",
                                            "Meter._MeterType",
                                            "Meter._AccountIdent",
                                            "Meter._TimeZoneIndex",
                                            "Meter._MediaType",
                                            "Meter._InstallDate",
                                            "Meter._RemovalDate")
    loadProfileSummaryReadings = loadProfileSummaryReadings.withColumn("tmp", arrays_zip("UOM","Direction","SumOfIntervalValues","Multiplier")) \
                                                            .withColumn("tmp", explode("tmp")) \
                                                            .withColumn("MeterReadings_Source", 
                                                            when(col("MeterReadings_Source") == "Visual", lit("Visual")) \
                                                            .when(col("MeterReadings_Source") == "Remote", lit("Remoto")) \
                                                            .when(col("MeterReadings_Source") == "LocalRF", lit("LAN")) \
                                                            .when(col("MeterReadings_Source") == "Optical", lit("Optical")) \
                                                            .when(col("MeterReadings_Source") == "Manually Estimated", lit("Visual")) \
                                                            .when(col("MeterReadings_Source") == "LegacySystem", lit("HES")) \
                                                            .otherwise(col("MeterReadings_Source"))) \
                                                            .withColumn("servicePointId", 
                                                                    when(col("Meter_SdpIdent").isNull(), col("Meter_MeterIrn")) \
                                                                    .otherwise(col("Meter_SdpIdent"))) \
                                                            .select(
                                                                    col("servicePointId"),
                                                                    col("FixedAttribute_readingType").alias("readingType"),
                                                                    concat(col("tmp.UOM"),lit(" "),col("tmp.Direction")).alias("variableId"),
                                                                    col("Meter_MeterIrn").alias("deviceId"),
                                                                    col("FixedAttribute_meteringType").alias("meteringType"),
                                                                    col("FixedAttribute_readingUtcLocalTime").alias("readingUtcLocalTime"),
                                                                    col("FixedAttribute_readingDateSource").alias("readingDateSource"),
                                                                    col("FixedAttribute_readingLocalTime").alias("readingLocalTime"),
                                                                    col("FixedAttribute_dstStatus").alias("dstStatus"),
                                                                    col("FixedAttribute_channel").alias("channel"),
                                                                    col("tmp.UOM").alias("unitOfMeasure"),
                                                                    col("FixedAttribute_qualityCodesSystemId").alias("qualityCodesSystemId"),
                                                                    col("FixedAttribute_qualityCodesCategorization").alias("qualityCodesCategorization"),
                                                                    col("FixedAttribute_qualityCodesIndex").alias("qualityCodesIndex"),
                                                                    col("FixedAttribute_intervalSize").alias("intervalSize"),
                                                                    col("FixedAttribute_logNumber").alias("logNumber"),
                                                                    col("FixedAttribute_ct").alias("ct"),
                                                                    col("FixedAttribute_pt").alias("pt"),
                                                                    col("tmp.Multiplier").alias("ke"),
                                                                    col("FixedAttribute_sf").alias("sf"),
                                                                    col("FixedAttribute_version").alias("version"),
                                                                    col("tmp.SumOfIntervalValues").alias("readingsValue"),
                                                                    col("MeterReadings_Source").alias("primarySource"),
                                                                    col("FixedAttribute_owner").alias("owner"),
                                                                    col("FixedAttribute_guidFile").alias("guidFile"),
                                                                    col("FixedAttribute_estatus").alias("estatus"),
                                                                    col("FixedAttribute_registersNumber").alias("registersNumber"),
                                                                    col("FixedAttribute_eventsCode").alias("eventsCode"),
                                                                    col("FixedAttribute_agentId").alias("agentId"),
                                                                    col("FixedAttribute_agentDescription").alias("agentDescription"),
                                                                    col("_SourceIrn"),
                                                                    col("_Description"),
                                                                    col("_IsActive"),
                                                                    col("_SerialNumber"),
                                                                    col("_MeterType"),
                                                                    col("_AccountIdent"),
                                                                    col("_TimeZoneIndex"),
                                                                    col("_MediaType"),
                                                                    col("_InstallDate"),
                                                                    col("_RemovalDate"))
    ######################################################################################################################################################

    ######################################################################################################################################################
    outageCountReadings = df.withColumn("ReadingTime", col("OutageCountSummary.OutageCount._ReadingTime")) \
                            .withColumn("Value", col("OutageCountSummary.OutageCount._Value")) \
                            .withColumn("FixedAttribute_unitOfMeasure", lit("Count")) \
                            .withColumn("FixedAttribute_variableId", lit("Outage count")) \
                            .withColumn("MeterReadings_Source", col("_Source")) \
                            .withColumn("Meter_SdpIdent", col("Meter._SdpIdent")) \
                            .withColumn("FixedAttribute_readingType", lit("EVENTS")) \
                            .withColumn("Meter_MeterIrn", col("Meter._MeterIrn")) \
                            .withColumn("FixedAttribute_meteringType", lit("MAIN")) \
                            .withColumn("FixedAttribute_readingUtcLocalTime", lit("")) \
                            .withColumn("FixedAttribute_readingDateSource", lit("")) \
                            .withColumn("FixedAttribute_dstStatus", lit("")) \
                            .withColumn("FixedAttribute_channel", lit("")) \
                            .withColumn("FixedAttribute_qualityCodesSystemId", lit("")) \
                            .withColumn("FixedAttribute_qualityCodesCategorization", lit("")) \
                            .withColumn("FixedAttribute_qualityCodesIndex", lit("")) \
                            .withColumn("FixedAttribute_intervalSize", lit("")) \
                            .withColumn("FixedAttribute_logNumber", lit("")) \
                            .withColumn("FixedAttribute_ct", lit("")) \
                            .withColumn("FixedAttribute_pt", lit("")) \
                            .withColumn("FixedAttribute_ke", lit("")) \
                            .withColumn("FixedAttribute_sf", lit("")) \
                            .withColumn("FixedAttribute_version", lit("")) \
                            .withColumn("FixedAttribute_readingsSource", lit("")) \
                            .withColumn("FixedAttribute_owner", lit(owner)) \
                            .withColumn("FixedAttribute_guidFile", lit(guid)) \
                            .withColumn("FixedAttribute_estatus", lit("Activo")) \
                            .withColumn("FixedAttribute_registersNumber", lit("")) \
                            .withColumn("FixedAttribute_eventsCode", lit("")) \
                            .withColumn("FixedAttribute_agentId", lit("")) \
                            .withColumn("FixedAttribute_agentDescription", lit("")) \
                            .select(
                                    "ReadingTime", 
                                    "Value",
                                    "FixedAttribute_unitOfMeasure",
                                    "FixedAttribute_variableId",
                                    "MeterReadings_Source",
                                    "Meter_SdpIdent",
                                    "FixedAttribute_readingType",
                                    "Meter_MeterIrn",
                                    "FixedAttribute_meteringType",
                                    "FixedAttribute_readingUtcLocalTime",
                                    "FixedAttribute_readingDateSource",
                                    "FixedAttribute_dstStatus",
                                    "FixedAttribute_channel",
                                    "FixedAttribute_qualityCodesSystemId",
                                    "FixedAttribute_qualityCodesCategorization",
                                    "FixedAttribute_qualityCodesIndex",
                                    "FixedAttribute_intervalSize",
                                    "FixedAttribute_logNumber",
                                    "FixedAttribute_ct",
                                    "FixedAttribute_pt",
                                    "FixedAttribute_ke",
                                    "FixedAttribute_sf",
                                    "FixedAttribute_version",
                                    "FixedAttribute_readingsSource",
                                    "FixedAttribute_owner",
                                    "FixedAttribute_guidFile",
                                    "FixedAttribute_estatus",
                                    "FixedAttribute_registersNumber",
                                    "FixedAttribute_eventsCode",
                                    "FixedAttribute_agentId",
                                    "FixedAttribute_agentDescription",
                                    "_SourceIrn",
                                    "Meter._Description",
                                    "Meter._IsActive",
                                    "Meter._SerialNumber",
                                    "Meter._MeterType",
                                    "Meter._AccountIdent",
                                    "Meter._TimeZoneIndex",
                                    "Meter._MediaType",
                                    "Meter._InstallDate",
                                    "Meter._RemovalDate") 
    outageCountReadings = outageCountReadings.withColumn("tmp", arrays_zip("ReadingTime","Value")) \
                                            .withColumn("tmp", explode("tmp")) \
                                            .withColumn("MeterReadings_Source", 
                                            when(col("MeterReadings_Source") == "Visual", lit("Visual")) \
                                            .when(col("MeterReadings_Source") == "Remote", lit("Remoto")) \
                                            .when(col("MeterReadings_Source") == "LocalRF", lit("LAN")) \
                                            .when(col("MeterReadings_Source") == "Optical", lit("Optical")) \
                                            .when(col("MeterReadings_Source") == "Manually Estimated", lit("Visual")) \
                                            .when(col("MeterReadings_Source") == "LegacySystem", lit("HES")) \
                                            .otherwise(col("MeterReadings_Source"))) \
                                            .withColumn("servicePointId", 
                                                    when(col("Meter_SdpIdent").isNull(), col("Meter_MeterIrn")) \
                                                    .otherwise(col("Meter_SdpIdent"))) \
                                            .select(
                                                    col("servicePointId"),
                                                    col("FixedAttribute_readingType").alias("readingType"),
                                                    col("FixedAttribute_variableId").alias("variableId"),
                                                    col("Meter_MeterIrn").alias("deviceId"),
                                                    col("FixedAttribute_meteringType").alias("meteringType"),
                                                    col("FixedAttribute_readingUtcLocalTime").alias("readingUtcLocalTime"),
                                                    col("tmp.ReadingTime").alias("readingDateSource"),
                                                    col("tmp.ReadingTime").alias("readingLocalTime"),
                                                    col("FixedAttribute_dstStatus").alias("dstStatus"),
                                                    col("FixedAttribute_channel").alias("channel"),
                                                    col("FixedAttribute_unitOfMeasure").alias("unitOfMeasure"),
                                                    col("FixedAttribute_qualityCodesSystemId").alias("qualityCodesSystemId"),
                                                    col("FixedAttribute_qualityCodesCategorization").alias("qualityCodesCategorization"),
                                                    col("FixedAttribute_qualityCodesIndex").alias("qualityCodesIndex"),
                                                    col("FixedAttribute_intervalSize").alias("intervalSize"),
                                                    col("FixedAttribute_logNumber").alias("logNumber"),
                                                    col("FixedAttribute_ct").alias("ct"),
                                                    col("FixedAttribute_pt").alias("pt"),
                                                    col("FixedAttribute_ke").alias("ke"),
                                                    col("FixedAttribute_sf").alias("sf"),
                                                    col("FixedAttribute_version").alias("version"),
                                                    col("tmp.Value").alias("readingsValue"),
                                                    col("MeterReadings_Source").alias("primarySource"),
                                                    col("FixedAttribute_owner").alias("owner"),
                                                    col("FixedAttribute_guidFile").alias("guidFile"),
                                                    col("FixedAttribute_estatus").alias("estatus"),
                                                    col("FixedAttribute_registersNumber").alias("registersNumber"),
                                                    col("FixedAttribute_eventsCode").alias("eventsCode"),
                                                    col("FixedAttribute_agentId").alias("agentId"),
                                                    col("FixedAttribute_agentDescription").alias("agentDescription"),
                                                    col("_SourceIrn"),
                                                    col("_Description"),
                                                    col("_IsActive"),
                                                    col("_SerialNumber"),
                                                    col("_MeterType"),
                                                    col("_AccountIdent"),
                                                    col("_TimeZoneIndex"),
                                                    col("_MediaType"),
                                                    col("_InstallDate"),
                                                    col("_RemovalDate"))
    ######################################################################################################################################################
    #OutageCountSummary tiene un previous reading time y un reading time, cada uno tiene que generar una lectura.
    ######################################################################################################################################################
    previousOutageCountReadings = df.withColumn("Value", col("OutageCountSummary.OutageCount._Value")) \
                                    .withColumn("PreviousReadingTime", col("OutageCountSummary.OutageCount._PreviousReadingTime")) \
                                    .withColumn("FixedAttribute_unitOfMeasure", lit("Count")) \
                                    .withColumn("FixedAttribute_variableId", lit("Outage count")) \
                                    .withColumn("MeterReadings_Source", col("_Source")) \
                                    .withColumn("Meter_SdpIdent", col("Meter._SdpIdent")) \
                                    .withColumn("FixedAttribute_readingType", lit("EVENTS")) \
                                    .withColumn("Meter_MeterIrn", col("Meter._MeterIrn")) \
                                    .withColumn("FixedAttribute_meteringType", lit("MAIN")) \
                                    .withColumn("FixedAttribute_readingUtcLocalTime", lit("")) \
                                    .withColumn("FixedAttribute_readingDateSource", lit("")) \
                                    .withColumn("FixedAttribute_dstStatus", lit("")) \
                                    .withColumn("FixedAttribute_channel", lit("")) \
                                    .withColumn("FixedAttribute_qualityCodesSystemId", lit("")) \
                                    .withColumn("FixedAttribute_qualityCodesCategorization", lit("")) \
                                    .withColumn("FixedAttribute_qualityCodesIndex", lit("")) \
                                    .withColumn("FixedAttribute_intervalSize", lit("")) \
                                    .withColumn("FixedAttribute_logNumber", lit("")) \
                                    .withColumn("FixedAttribute_ct", lit("")) \
                                    .withColumn("FixedAttribute_pt", lit("")) \
                                    .withColumn("FixedAttribute_ke", lit("")) \
                                    .withColumn("FixedAttribute_sf", lit("")) \
                                    .withColumn("FixedAttribute_version", lit("")) \
                                    .withColumn("FixedAttribute_readingsSource", lit("")) \
                                    .withColumn("FixedAttribute_owner", lit(owner)) \
                                    .withColumn("FixedAttribute_guidFile", lit(guid)) \
                                    .withColumn("FixedAttribute_estatus", lit("Activo")) \
                                    .withColumn("FixedAttribute_registersNumber", lit("")) \
                                    .withColumn("FixedAttribute_eventsCode", lit("")) \
                                    .withColumn("FixedAttribute_agentId", lit("")) \
                                    .withColumn("FixedAttribute_agentDescription", lit("")) \
                                    .select(
                                            "Value",
                                            "PreviousReadingTime",
                                            "FixedAttribute_unitOfMeasure",
                                            "FixedAttribute_variableId",
                                            "MeterReadings_Source",
                                            "Meter_SdpIdent",
                                            "FixedAttribute_readingType",
                                            "Meter_MeterIrn",
                                            "FixedAttribute_meteringType",
                                            "FixedAttribute_readingUtcLocalTime",
                                            "FixedAttribute_readingDateSource",
                                            "FixedAttribute_dstStatus",
                                            "FixedAttribute_channel",
                                            "FixedAttribute_qualityCodesSystemId",
                                            "FixedAttribute_qualityCodesCategorization",
                                            "FixedAttribute_qualityCodesIndex",
                                            "FixedAttribute_intervalSize",
                                            "FixedAttribute_logNumber",
                                            "FixedAttribute_ct",
                                            "FixedAttribute_pt",
                                            "FixedAttribute_ke",
                                            "FixedAttribute_sf",
                                            "FixedAttribute_version",
                                            "FixedAttribute_readingsSource",
                                            "FixedAttribute_owner",
                                            "FixedAttribute_guidFile",
                                            "FixedAttribute_estatus",
                                            "FixedAttribute_registersNumber",
                                            "FixedAttribute_eventsCode",
                                            "FixedAttribute_agentId",
                                            "FixedAttribute_agentDescription",
                                            "_SourceIrn",
                                            "Meter._Description",
                                            "Meter._IsActive",
                                            "Meter._SerialNumber",
                                            "Meter._MeterType",
                                            "Meter._AccountIdent",
                                            "Meter._TimeZoneIndex",
                                            "Meter._MediaType",
                                            "Meter._InstallDate",
                                            "Meter._RemovalDate") 
    previousOutageCountReadings = previousOutageCountReadings.withColumn("tmp", arrays_zip("PreviousReadingTime","Value")) \
                                                            .withColumn("tmp", explode("tmp")) \
                                                            .withColumn("MeterReadings_Source", 
                                                            when(col("MeterReadings_Source") == "Visual", lit("Visual")) \
                                                            .when(col("MeterReadings_Source") == "Remote", lit("Remoto")) \
                                                            .when(col("MeterReadings_Source") == "LocalRF", lit("LAN")) \
                                                            .when(col("MeterReadings_Source") == "Optical", lit("Optical")) \
                                                            .when(col("MeterReadings_Source") == "Manually Estimated", lit("Visual")) \
                                                            .when(col("MeterReadings_Source") == "LegacySystem", lit("HES")) \
                                                            .otherwise(col("MeterReadings_Source"))) \
                                                            .withColumn("servicePointId", 
                                                                    when(col("Meter_SdpIdent").isNull(), col("Meter_MeterIrn")) \
                                                                    .otherwise(col("Meter_SdpIdent"))) \
                                                            .select(
                                                                    col("servicePointId"),
                                                                    col("FixedAttribute_readingType").alias("readingType"),
                                                                    col("FixedAttribute_variableId").alias("variableId"),
                                                                    col("Meter_MeterIrn").alias("deviceId"),
                                                                    col("FixedAttribute_meteringType").alias("meteringType"),
                                                                    col("FixedAttribute_readingUtcLocalTime").alias("readingUtcLocalTime"),
                                                                    col("tmp.PreviousReadingTime").alias("readingDateSource"),
                                                                    col("tmp.PreviousReadingTime").alias("readingLocalTime"),
                                                                    col("FixedAttribute_dstStatus").alias("dstStatus"),
                                                                    col("FixedAttribute_channel").alias("channel"),
                                                                    col("FixedAttribute_unitOfMeasure").alias("unitOfMeasure"),
                                                                    col("FixedAttribute_qualityCodesSystemId").alias("qualityCodesSystemId"),
                                                                    col("FixedAttribute_qualityCodesCategorization").alias("qualityCodesCategorization"),
                                                                    col("FixedAttribute_qualityCodesIndex").alias("qualityCodesIndex"),
                                                                    col("FixedAttribute_intervalSize").alias("intervalSize"),
                                                                    col("FixedAttribute_logNumber").alias("logNumber"),
                                                                    col("FixedAttribute_ct").alias("ct"),
                                                                    col("FixedAttribute_pt").alias("pt"),
                                                                    col("FixedAttribute_ke").alias("ke"),
                                                                    col("FixedAttribute_sf").alias("sf"),
                                                                    col("FixedAttribute_version").alias("version"),
                                                                    col("tmp.Value").alias("readingsValue"),
                                                                    col("MeterReadings_Source").alias("primarySource"),
                                                                    col("FixedAttribute_owner").alias("owner"),
                                                                    col("FixedAttribute_guidFile").alias("guidFile"),
                                                                    col("FixedAttribute_estatus").alias("estatus"),
                                                                    col("FixedAttribute_registersNumber").alias("registersNumber"),
                                                                    col("FixedAttribute_eventsCode").alias("eventsCode"),
                                                                    col("FixedAttribute_agentId").alias("agentId"),
                                                                    col("FixedAttribute_agentDescription").alias("agentDescription"),
                                                                    col("_SourceIrn"),
                                                                    col("_Description"),
                                                                    col("_IsActive"),
                                                                    col("_SerialNumber"),
                                                                    col("_MeterType"),
                                                                    col("_AccountIdent"),
                                                                    col("_TimeZoneIndex"),
                                                                    col("_MediaType"),
                                                                    col("_InstallDate"),
                                                                    col("_RemovalDate"))
    ######################################################################################################################################################

    ######################################################################################################################################################
    reverseEnergySummaryReadings = df.withColumn("CurrentValue", col("ReverseEnergySummary.ReverseEnergy._CurrentValue")) \
                                    .withColumn("FixedAttribute_readingLocalTime", lit("")) \
                                    .withColumn("FixedAttribute_unitOfMeasure", lit("-")) \
                                    .withColumn("FixedAttribute_variableId", lit("Reverse energy summary")) \
                                    .withColumn("MeterReadings_Source", col("_Source")) \
                                    .withColumn("Meter_SdpIdent", col("Meter._SdpIdent")) \
                                    .withColumn("FixedAttribute_readingType", lit("REGISTERS")) \
                                    .withColumn("Meter_MeterIrn", col("Meter._MeterIrn")) \
                                    .withColumn("FixedAttribute_meteringType", lit("MAIN")) \
                                    .withColumn("FixedAttribute_readingUtcLocalTime", lit("")) \
                                    .withColumn("FixedAttribute_readingDateSource", lit("")) \
                                    .withColumn("FixedAttribute_dstStatus", lit("")) \
                                    .withColumn("FixedAttribute_channel", lit("")) \
                                    .withColumn("FixedAttribute_qualityCodesSystemId", lit("")) \
                                    .withColumn("FixedAttribute_qualityCodesCategorization", lit("")) \
                                    .withColumn("FixedAttribute_qualityCodesIndex", lit("")) \
                                    .withColumn("FixedAttribute_intervalSize", lit("")) \
                                    .withColumn("FixedAttribute_logNumber", lit("")) \
                                    .withColumn("FixedAttribute_ct", lit("")) \
                                    .withColumn("FixedAttribute_pt", lit("")) \
                                    .withColumn("FixedAttribute_ke", lit("")) \
                                    .withColumn("FixedAttribute_sf", lit("")) \
                                    .withColumn("FixedAttribute_version", lit("")) \
                                    .withColumn("FixedAttribute_readingsSource", lit("")) \
                                    .withColumn("FixedAttribute_owner", lit(owner)) \
                                    .withColumn("FixedAttribute_guidFile", lit(guid)) \
                                    .withColumn("FixedAttribute_estatus", lit("Activo")) \
                                    .withColumn("FixedAttribute_registersNumber", lit("")) \
                                    .withColumn("FixedAttribute_eventsCode", lit("")) \
                                    .withColumn("FixedAttribute_agentId", lit("")) \
                                    .withColumn("FixedAttribute_agentDescription", lit("")) \
                                    .select(
                                            "CurrentValue", 
                                            "FixedAttribute_readingLocalTime",
                                            "FixedAttribute_unitOfMeasure",
                                            "FixedAttribute_variableId",
                                            "MeterReadings_Source",
                                            "Meter_SdpIdent",
                                            "FixedAttribute_readingType",
                                            "Meter_MeterIrn",
                                            "FixedAttribute_meteringType",
                                            "FixedAttribute_readingUtcLocalTime",
                                            "FixedAttribute_readingDateSource",
                                            "FixedAttribute_dstStatus",
                                            "FixedAttribute_channel",
                                            "FixedAttribute_qualityCodesSystemId",
                                            "FixedAttribute_qualityCodesCategorization",
                                            "FixedAttribute_qualityCodesIndex",
                                            "FixedAttribute_intervalSize",
                                            "FixedAttribute_logNumber",
                                            "FixedAttribute_ct",
                                            "FixedAttribute_pt",
                                            "FixedAttribute_ke",
                                            "FixedAttribute_sf",
                                            "FixedAttribute_version",
                                            "FixedAttribute_readingsSource",
                                            "FixedAttribute_owner",
                                            "FixedAttribute_guidFile",
                                            "FixedAttribute_estatus",
                                            "FixedAttribute_registersNumber",
                                            "FixedAttribute_eventsCode",
                                            "FixedAttribute_agentId",
                                            "FixedAttribute_agentDescription",
                                            "_SourceIrn",
                                            "Meter._Description",
                                            "Meter._IsActive",
                                            "Meter._SerialNumber",
                                            "Meter._MeterType",
                                            "Meter._AccountIdent",
                                            "Meter._TimeZoneIndex",
                                            "Meter._MediaType",
                                            "Meter._InstallDate",
                                            "Meter._RemovalDate") 
    reverseEnergySummaryReadings=reverseEnergySummaryReadings.withColumn("tmp", arrays_zip("CurrentValue")) \
                                                            .withColumn("tmp", explode("tmp")) \
                                                            .withColumn("MeterReadings_Source", 
                                                            when(col("MeterReadings_Source") == "Visual", lit("Visual")) \
                                                            .when(col("MeterReadings_Source") == "Remote", lit("Remoto")) \
                                                            .when(col("MeterReadings_Source") == "LocalRF", lit("LAN")) \
                                                            .when(col("MeterReadings_Source") == "Optical", lit("Optical")) \
                                                            .when(col("MeterReadings_Source") == "Manually Estimated", lit("Visual")) \
                                                            .when(col("MeterReadings_Source") == "LegacySystem", lit("HES")) \
                                                            .otherwise(col("MeterReadings_Source"))) \
                                                            .withColumn("servicePointId", 
                                                                    when(col("Meter_SdpIdent").isNull(), col("Meter_MeterIrn")) \
                                                                    .otherwise(col("Meter_SdpIdent"))) \
                                                            .select(
                                                                    col("servicePointId"),
                                                                    col("FixedAttribute_readingType").alias("readingType"),
                                                                    col("FixedAttribute_variableId").alias("variableId"),
                                                                    col("Meter_MeterIrn").alias("deviceId"),
                                                                    col("FixedAttribute_meteringType").alias("meteringType"),
                                                                    col("FixedAttribute_readingUtcLocalTime").alias("readingUtcLocalTime"),
                                                                    col("FixedAttribute_readingLocalTime").alias("readingDateSource"),
                                                                    col("FixedAttribute_readingLocalTime").alias("readingLocalTime"),
                                                                    col("FixedAttribute_dstStatus").alias("dstStatus"),
                                                                    col("FixedAttribute_channel").alias("channel"),
                                                                    col("FixedAttribute_unitOfMeasure").alias("unitOfMeasure"),
                                                                    col("FixedAttribute_qualityCodesSystemId").alias("qualityCodesSystemId"),
                                                                    col("FixedAttribute_qualityCodesCategorization").alias("qualityCodesCategorization"),
                                                                    col("FixedAttribute_qualityCodesIndex").alias("qualityCodesIndex"),
                                                                    col("FixedAttribute_intervalSize").alias("intervalSize"),
                                                                    col("FixedAttribute_logNumber").alias("logNumber"),
                                                                    col("FixedAttribute_ct").alias("ct"),
                                                                    col("FixedAttribute_pt").alias("pt"),
                                                                    col("FixedAttribute_ke").alias("ke"),
                                                                    col("FixedAttribute_sf").alias("sf"),
                                                                    col("FixedAttribute_version").alias("version"),
                                                                    col("tmp.CurrentValue").alias("readingsValue"),
                                                                    col("MeterReadings_Source").alias("primarySource"),
                                                                    col("FixedAttribute_owner").alias("owner"),
                                                                    col("FixedAttribute_guidFile").alias("guidFile"),
                                                                    col("FixedAttribute_estatus").alias("estatus"),
                                                                    col("FixedAttribute_registersNumber").alias("registersNumber"),
                                                                    col("FixedAttribute_eventsCode").alias("eventsCode"),
                                                                    col("FixedAttribute_agentId").alias("agentId"),
                                                                    col("FixedAttribute_agentDescription").alias("agentDescription"),
                                                                    col("_SourceIrn"),
                                                                    col("_Description"),
                                                                    col("_IsActive"),
                                                                    col("_SerialNumber"),
                                                                    col("_MeterType"),
                                                                    col("_AccountIdent"),
                                                                    col("_TimeZoneIndex"),
                                                                    col("_MediaType"),
                                                                    col("_InstallDate"),
                                                                    col("_RemovalDate"))
    ######################################################################################################################################################

    ######################################################################################################################################################
    eventsDataReadings = df.select("EventData",
                                    "_Source",
                                    "Meter._SdpIdent",
                                    "Meter._MeterIrn",
                                    "_SourceIrn",
                                    "Meter._Description",
                                    "Meter._IsActive",
                                    "Meter._SerialNumber",
                                    "Meter._MeterType",
                                    "Meter._AccountIdent",
                                    "Meter._TimeZoneIndex",
                                    "Meter._MediaType",
                                    "Meter._InstallDate",
                                    "Meter._RemovalDate") \
                            .withColumn("_Source", 
                                when(col("_Source") == "Visual", lit("Visual")) \
                                .when(col("_Source") == "Remote", lit("Remoto")) \
                                .when(col("_Source") == "LocalRF", lit("LAN")) \
                                .when(col("_Source") == "Optical", lit("Optical")) \
                                .when(col("_Source") == "Manually Estimated", lit("Visual")) \
                                .when(col("_Source") == "LegacySystem", lit("HES")) \
                                .otherwise(col("_Source"))) \
                            .withColumn("servicePointId", 
                                when(col("_SdpIdent").isNull(), col("_MeterIrn")) \
                                .otherwise(col("_SdpIdent"))) \
                            .withColumn("Event",explode("EventData.Event")) \
                            .withColumn("Event_exploded", explode("Event"))\
                            .withColumn("TimeStamp", 
                                    when(col("Event_exploded._TimeStamp").isNull(), col("Event_exploded._DiscoveredAt")) \
                                    .otherwise(col("Event_exploded._TimeStamp"))) \
                            .withColumn("EventAttribute_exploded", col("Event_exploded.EventAttribute")) \
                            .withColumn("Name", col("EventAttribute_exploded._Name")) \
                            .withColumn("Value", col("EventAttribute_exploded._Value")) \
                            .withColumn("tmp", arrays_zip("Name","Value")) \
                            .withColumn("tmp", explode("tmp")) \
                            .withColumn("variableId", col("tmp.Name")) \
                            .withColumn("readingsValue", col("tmp.Value")) \
                            .withColumn("readingType",lit("EVENTS")) \
                            .withColumn("FixedAttribute_readingType", lit("REGISTERS")) \
                            .withColumn("FixedAttribute_meteringType", lit("MAIN")) \
                            .withColumn("FixedAttribute_readingUtcLocalTime", lit("")) \
                            .withColumn("FixedAttribute_dstStatus", lit("")) \
                            .withColumn("FixedAttribute_channel", lit("")) \
                            .withColumn("FixedAttribute_qualityCodesSystemId", lit("")) \
                            .withColumn("FixedAttribute_qualityCodesCategorization", lit("")) \
                            .withColumn("FixedAttribute_qualityCodesIndex", lit("")) \
                            .withColumn("FixedAttribute_intervalSize", lit("")) \
                            .withColumn("FixedAttribute_logNumber", lit("")) \
                            .withColumn("FixedAttribute_ct", lit("")) \
                            .withColumn("FixedAttribute_pt", lit("")) \
                            .withColumn("FixedAttribute_sf", lit("")) \
                            .withColumn("FixedAttribute_ke", lit("")) \
                            .withColumn("FixedAttribute_version", lit("")) \
                            .withColumn("FixedAttribute_readingsSource", lit("")) \
                            .withColumn("FixedAttribute_owner", lit(owner)) \
                            .withColumn("FixedAttribute_guidFile", lit(guid)) \
                            .withColumn("FixedAttribute_estatus", lit("Activo")) \
                            .withColumn("FixedAttribute_registersNumber", lit("")) \
                            .withColumn("FixedAttribute_eventsCode", lit("")) \
                            .withColumn("FixedAttribute_agentId", lit("")) \
                            .withColumn("FixedAttribute_agentDescription", lit("")) \
                            .withColumn("FixedAttribute_UOM", lit("-")) \
                            .select(
                                "servicePointId",
                                "readingType",
                                "variableId",
                                col("_MeterIrn").alias("deviceId"),
                                col("FixedAttribute_meteringType").alias("meteringType"),
                                col("FixedAttribute_readingUtcLocalTime").alias("readingUtcLocalTime"),
                                col("TimeStamp").alias("readingDateSource"),
                                col("TimeStamp").alias("readingLocalTime"),
                                col("FixedAttribute_dstStatus").alias("dstStatus"),
                                col("FixedAttribute_channel").alias("channel"),
                                col("FixedAttribute_UOM").alias("unitOfMeasure"),
                                col("FixedAttribute_qualityCodesSystemId").alias("qualityCodesSystemId"),
                                col("FixedAttribute_qualityCodesCategorization").alias("qualityCodesCategorization"),
                                col("FixedAttribute_qualityCodesIndex").alias("qualityCodesIndex"),
                                col("FixedAttribute_intervalSize").alias("intervalSize"),
                                col("FixedAttribute_logNumber").alias("logNumber"),
                                col("FixedAttribute_ct").alias("ct"),
                                col("FixedAttribute_pt").alias("pt"),
                                col("FixedAttribute_ke").alias("ke"),
                                col("FixedAttribute_sf").alias("sf"),
                                col("FixedAttribute_version").alias("version"),
                                "readingsValue",
                                col("_Source").alias("primarySource"),
                                col("FixedAttribute_owner").alias("owner"),
                                col("FixedAttribute_guidFile").alias("guidFile"),
                                col("FixedAttribute_estatus").alias("estatus"),
                                col("FixedAttribute_registersNumber").alias("registersNumber"),
                                col("FixedAttribute_eventsCode").alias("eventsCode"),
                                col("FixedAttribute_agentId").alias("agentId"),
                                col("FixedAttribute_agentDescription").alias("agentDescription"),
                                col("_SourceIrn"),
                                col("_Description"),
                                col("_IsActive"),
                                col("_SerialNumber"),
                                col("_MeterType"),
                                col("_AccountIdent"),
                                col("_TimeZoneIndex"),
                                col("_MediaType"),
                                col("_InstallDate"),
                                col("_RemovalDate"))


    ######################################################################################################################################################

    ######################################################################################################################################################
    IntervalData = []
    for row in df.rdd.collect():
            if row.Meter != None:
                    MeterReadings_Source = row._Source
                    MeterReadings_SourceIrn = row._SourceIrn
                    Meter_Irn = row.Meter._MeterIrn
                    Meter_SdpIdent = row.Meter._SdpIdent
                    Meter_Description = row.Meter._Description
                    Meter_IsActive = row.Meter._IsActive
                    Meter_SerialNumber = row.Meter._SerialNumber
                    Meter_MeterType = row.Meter._MeterType
                    Meter_AccountIdent = row.Meter._AccountIdent
                    Meter_TimeZoneIndex = row.Meter._TimeZoneIndex
                    Meter_MediaType = row.Meter._MediaType
                    Meter_InstallDate = row.Meter._InstallDate
                    Meter_RemovalDate = row.Meter._RemovalDate
                    if row.IntervalData != None:
                            for intervalData in row.IntervalData:
                                    IntervalSpec_Channel = intervalData.IntervalSpec._Channel
                                    IntervalSpec_Direction = intervalData.IntervalSpec._Direction
                                    IntervalSpec_Interval = intervalData.IntervalSpec._Interval
                                    IntervalSpec_Multiplier = intervalData.IntervalSpec._Multiplier
                                    IntervalSpec_Channel = intervalData.IntervalSpec._Channel
                                    IntervalSpec_UOM = intervalData.IntervalSpec._UOM
                                    for reading in intervalData.Reading:
                                            Reading_RawReading = reading._RawReading
                                            Reading_TimeStamp = reading._TimeStamp

                                            servicePointId = Meter_SdpIdent
                                            if servicePointId == None:
                                                    servicePointId = Meter_Irn

                                            dst = ""
                                            QualityCode_SystemId = ""
                                            QualityCode_Categorization = ""
                                            QualityCode_Index = ""
                                            if reading.QualityFlags != None:
                                                    qualityFlag = reading.QualityFlags
                                                    if qualityFlag._TimeChanged != None:
                                                            QualityCode_SystemId = "2"
                                                            QualityCode_Categorization = "4"
                                                            QualityCode_Index = "9"
                                                    elif qualityFlag._ClockSetBackward != None:
                                                            QualityCode_SystemId = "1"
                                                            QualityCode_Categorization = "4"
                                                            QualityCode_Index = "128"
                                                    elif qualityFlag._LongInterval != None:
                                                            QualityCode_SystemId = "1"
                                                            QualityCode_Categorization = "4"
                                                            QualityCode_Index = "3"
                                                    elif qualityFlag._ClockSetForward != None:
                                                            QualityCode_SystemId = "1"
                                                            QualityCode_Categorization = "4"
                                                            QualityCode_Index = "64"
                                                    elif qualityFlag._PartialInterval != None:
                                                            QualityCode_SystemId = "1"
                                                            QualityCode_Categorization = "4"
                                                            QualityCode_Index = "2"
                                                    elif qualityFlag._InvalidTime != None:
                                                            QualityCode_SystemId = "1"
                                                            QualityCode_Categorization = "1"
                                                            QualityCode_Index = "9"
                                                    elif qualityFlag._SkippedInterval != None:
                                                            QualityCode_SystemId = "1"
                                                            QualityCode_Categorization = "4"
                                                            QualityCode_Index = "4"
                                                    elif qualityFlag._CompleteOutage != None or qualityFlag._PartialOutage != None:
                                                            QualityCode_SystemId = "1"
                                                            QualityCode_Categorization = "2"
                                                            QualityCode_Index = "32"
                                                    elif qualityFlag._PulseOverflow != None:
                                                            QualityCode_SystemId = "1"
                                                            QualityCode_Categorization = "4"
                                                            QualityCode_Index = "1"
                                                    elif qualityFlag._TestMode != None:
                                                            QualityCode_SystemId = "1"
                                                            QualityCode_Categorization = "4"
                                                            QualityCode_Index = "5"
                                                    elif qualityFlag._Tamper != None:
                                                            QualityCode_SystemId = "1"
                                                            QualityCode_Categorization = "3"
                                                            QualityCode_Index = "0"
                                                    elif qualityFlag._SuspectedOutage != None or qualityFlag._Restoration != None:
                                                            QualityCode_SystemId = "1"
                                                            QualityCode_Categorization = "2"
                                                            QualityCode_Index = "0"
                                                    elif qualityFlag._DST != None:
                                                            QualityCode_SystemId = "1"
                                                            QualityCode_Categorization = "4"
                                                            QualityCode_Index = "16"
                                                            dst = "1"
                                                    elif qualityFlag._InvalidValue != None:
                                                            QualityCode_SystemId = "2"
                                                            QualityCode_Categorization = "5"
                                                            QualityCode_Index = "256"

                                            IntervalData.append ({
                                                    "servicePointId":servicePointId,
                                                    "readingType":"LOAD PROFILE READING",
                                                    "variableId":IntervalSpec_UOM + ' ' + IntervalSpec_Direction,
                                                    "deviceId": Meter_Irn,
                                                    "meteringType": "MAIN",
                                                    "readingUtcLocalTime": "",
                                                    "readingDateSource": Reading_TimeStamp,
                                                    "readingLocalTime": Reading_TimeStamp,
                                                    "dstStatus":dst,
                                                    "channel":IntervalSpec_Channel,
                                                    "unitOfMeasure":IntervalSpec_UOM,
                                                    "qualityCodesSystemId":QualityCode_SystemId,
                                                    "qualityCodesCategorization":QualityCode_Categorization,
                                                    "qualityCodesIndex":QualityCode_Index,
                                                    "intervalSize":IntervalSpec_Interval,
                                                    "logNumber": "1",
                                                    "ct":"",
                                                    "pt":"",
                                                    "ke":IntervalSpec_Multiplier,
                                                    "sf":"",
                                                    "version":"Original",
                                                    "readingsValue":Reading_RawReading,
                                                    "primarySource":MeterReadings_Source,
                                                    "owner":owner,
                                                    "guidFile":guid,
                                                    "estatus": "Activo",
                                                    "registersNumber":"",
                                                    "eventsCode":"",
                                                    "agentId":"",
                                                    "agentDescription":"",
                                                    "_SourceIrn":str(MeterReadings_SourceIrn),
                                                    "_Description":str(Meter_Description),
                                                    "_IsActive":Meter_IsActive,
                                                    "_SerialNumber":str(Meter_SerialNumber),
                                                    "_MeterType":str(Meter_MeterType),
                                                    "_AccountIdent":str(Meter_AccountIdent),
                                                    "_TimeZoneIndex":str(Meter_TimeZoneIndex),
                                                    "_MediaType":str(Meter_MediaType),
                                                    "_InstallDate":str(Meter_InstallDate),
                                                    "_RemovalDate":str(Meter_RemovalDate)
                                                    })
    IntervalData = spark.sparkContext.parallelize(IntervalData) \
                            .map(lambda x: Row(**OrderedDict(x.items())))
    if IntervalData.isEmpty() == False:
            intervalDataReadings = spark.createDataFrame(IntervalData.coalesce(1)) \
                                    .withColumn("primarySource", 
                                            when(col("primarySource") == "Visual", lit("Visual")) \
                                            .when(col("primarySource") == "Remote", lit("Remoto")) \
                                            .when(col("primarySource") == "LocalRF", lit("LAN")) \
                                            .when(col("primarySource") == "Optical", lit("Optical")) \
                                            .when(col("primarySource") == "Manually Estimated", lit("Visual")) \
                                            .when(col("primarySource") == "LegacySystem", lit("HES")) \
                                            .otherwise(col("primarySource"))) \
                                    .withColumn("_SourceIrne",
                                            when (col("_SourceIrn")=="None", lit(""))
                                            .otherwise(col("_SourceIrn")))\
                                    .withColumn("_Description",
                                            when (col("_Description")=="None", lit(""))
                                            .otherwise(col("_Description")))\
                                    .withColumn("_SerialNumber",
                                            when (col("_SerialNumber")=="None", lit(""))
                                            .otherwise(col("_SerialNumber")))\
                                    .withColumn("_MeterType",
                                            when (col("_MeterType")=="None", lit(""))
                                            .otherwise(col("_MeterType")))\
                                    .withColumn("_AccountIdent",
                                            when (col("_AccountIdent")=="None", lit(""))
                                            .otherwise(col("_AccountIdent")))\
                                    .withColumn("_TimeZoneIndex",
                                            when (col("_TimeZoneIndex")=="None", lit(""))
                                            .otherwise(col("_TimeZoneIndex")))\
                                    .withColumn("_MediaType",
                                            when (col("_MediaType")=="None", lit(""))
                                            .otherwise(col("_MediaType")))\
                                    .withColumn("_InstallDate",
                                            when (col("_InstallDate")=="None", lit(""))
                                            .otherwise(col("_InstallDate")))\
                                    .withColumn("_RemovalDate",
                                            when (col("_RemovalDate")=="None", lit(""))
                                            .otherwise(col("_RemovalDate")))\
                                    .select(
                                    "servicePointId",
                                    "readingType",
                                    "variableId",
                                    "deviceId",
                                    "meteringType",
                                    "readingUtcLocalTime",
                                    "readingDateSource",
                                    "readingLocalTime",
                                    "dstStatus",
                                    "channel",
                                    "unitOfMeasure",
                                    "qualityCodesSystemId",
                                    "qualityCodesCategorization",
                                    "qualityCodesIndex",
                                    "intervalSize",
                                    "logNumber",
                                    "ct",
                                    "pt",
                                    "ke",
                                    "sf",
                                    "version",
                                    "readingsValue",
                                    "primarySource",
                                    "owner",
                                    "guidFile",
                                    "estatus",
                                    "registersNumber",
                                    "eventsCode",
                                    "agentId",
                                    "agentDescription",
                                    "_SourceIrn",
                                    "_Description",
                                    "_IsActive",
                                    "_SerialNumber",
                                    "_MeterType",
                                    "_AccountIdent",
                                    "_TimeZoneIndex",
                                    "_MediaType",
                                    "_InstallDate",
                                    "_RemovalDate"
                                    )                       
    ######################################################################################################################################################

    def union_all(*dfs):
        return reduce(DataFrame.unionAll, dfs)


    readings_list = [maxDemandDataReadings, demandResetCountReadings, \
            consumptionDataReadings, coincidentDemandDataReadings, \
            cumulativeDemandDataReadings, demandResetReadings, \
            instrumentationValueReadings, \
            loadProfileSummaryReadings, outageCountReadings, \
            reverseEnergySummaryReadings, eventsDataReadings ]

    if IntervalData.isEmpty() == False:
            readings_list.append(intervalDataReadings)

    if StatusData.isEmpty() == False:
            readings_list.append(statusReadings)

    union = union_all(*readings_list).coalesce(1) #Aca los estoy uniendo en una sola partición, si se saca el .coalesce(1) se van a crear distintas particiones para c/u

    # Se unen los 4 campos: ct, pt, ke y sf en un solo campo con el nombre Multipliers
    union = union.withColumn("multiplier",
                            concat(
                                    lit("'MultiplierValues':{'ke':'"), \
                                    col("ke"), \
                                    lit("'}")
                                    )
                            )
    # Se dropean las columnas que se unieron en el paso anterior
    columns_to_drop = ['ct', 'pt', 'ke', 'sf']
    union = union.drop(*columns_to_drop)


    union = union.withColumn("_IsActive", 
                    when(col("_IsActive") == True, "ENABLE") \
                    .when(col("_IsActive") == False, "DISABLE") \
                    .otherwise(None)) \
                .withColumn("deviceType", 
                    when(col("_MeterType") == "A3", lit("Electric meter"))\
                    .when(col("_MeterType") == "Alpha", lit("Electric meter")) \
                    .when(col("_MeterType") == "A3_ILN", lit("Electric meter")) \
                    .when(col("_MeterType") == "A3_Collector", lit("Electric meter")) \
                    .when(col("_MeterType") == "REX", lit("Electric meter")) \
                    .when(col("_MeterType") == "EAWater", lit("Water meter")) \
                    .when(col("_MeterType") == "EAGas", lit("Gas meter")) \
                    .otherwise(col("_MeterType"))) \
                .withColumn("brand", 
                    when(col("_MeterType") == "A3", lit("Elster"))\
                    .when(col("_MeterType") == "Alpha", lit("Elster")) \
                    .when(col("_MeterType") == "A3_ILN", lit("Elster")) \
                    .when(col("_MeterType") == "A3_Collector", lit("Elster")) \
                    .when(col("_MeterType") == "REX", lit("Elster")) \
                    .when(col("_MeterType") == "EAWater", lit("Elster")) \
                    .when(col("_MeterType") == "EAGas", lit("Elster")) \
                    .otherwise(col("_MeterType"))) \
                .withColumn("model", 
                    when(col("_MeterType") == "A3", lit("Alpha A3"))\
                    .when(col("_MeterType") == "Alpha", lit("Alpha")) \
                    .when(col("_MeterType") == "A3_ILN", lit("A3 ILN")) \
                    .when(col("_MeterType") == "A3_Collector", lit("A3 Collector")) \
                    .when(col("_MeterType") == "REX", lit("REX")) \
                    .when(col("_MeterType") == "EAWater", lit("EAWater")) \
                    .when(col("_MeterType") == "EAGas", lit("EAGas")) \
                    .otherwise(col("_MeterType"))) \
                .withColumn("connectionType", 
                    when(col("_MediaType") == "Ethernet WIC", "TCP/Ethernet") \
                    .when(col("_MediaType") == "900Mhz", "RF") \
                    .otherwise("_MediaType")) \
                .withColumn("readingsSource",lit(hes)) \
                .select(
                    col("servicePointId"),
                    col("readingType"),
                    col("variableId"),
                    col("deviceId"),
                    col("meteringType"),
                    col("readingUtcLocalTime"),
                    col("readingDateSource"),
                    col("readingLocalTime"),
                    col("dstStatus"),
                    col("channel"),
                    col("unitOfMeasure"),
                    col("qualityCodesSystemId"),
                    col("qualityCodesCategorization"),
                    col("qualityCodesIndex"),
                    col("intervalSize"),
                    col("logNumber"),
                    col("version"),
                    col("readingsValue"),
                    col("primarySource"),
                    col("readingsSource"),
                    col("owner"),
                    col("guidFile"),
                    col("estatus"),
                    col("registersNumber"),
                    col("eventsCode"),
                    col("agentId"),
                    col("agentDescription"),
                    col("multiplier"),
                    col("_SourceIrn").alias("deviceMaster"),
                    col("_Description").alias("deviceDescription"),
                    col("_IsActive").alias("deviceStatus"),
                    col("_SerialNumber").alias("serial"),
                    col("_AccountIdent").alias("accountNumber"),
                    col("_TimeZoneIndex").alias("servicePointTimeZone"),
                    col("connectionType"),
                    col("_InstallDate").alias("relationStartDate"),
                    col("_RemovalDate").alias("relationEndDate"),
                    col("deviceType"),
                    col("brand"),
                    col("model")
                    )          


    # los campos relationStartDate y relationEndDate no deben ir nulos, sino con el valor string vacio ('')
    union = union.fillna( { 'relationStartDate':'', 'relationEndDate':'' } )

    # Las filas que tengan el valor variableId nulo, no sirven, por lo que se tiran
    # Preguntar que hacemos en este caso a primestone, si las tiramos forever o es cambiarlas de bucket
    union = union.filter(union.variableId.isNotNull())

    # Reemplazo los valores de unitOfMeasure por su codigo
    dic_uom = {"kW":"96",
        "kWh":"74",
        "kVAr":"75",
        "kVArh":"77",
        "kVA":"78",
        "kVAh":"80",
        "kQ":"81",
        "kQh":"82",
        "-":"70",
        "V":"12",
        "A":"1",
        "ccf":"85",
        "gal":"86",
        "cgal":"87",
        "m^3":"88",
        "ft^3":"89",
        "in^3":"90",
        "yd^3":"91",
        "imp gal":"94",
        "L":"92",
        "af":"93",
        "Count":"99",
        "Hz":"97",
        "°":"98"}

    union = union.replace(dic_uom, subset=["unitOfMeasure"])

    # Cambio tipo de datos.
    union = union.withColumn("channel",union["channel"].cast(IntegerType()))
    union = union.withColumn("unitOfMeasure",union["unitOfMeasure"].cast(IntegerType()))
    union = union.withColumn("intervalSize",union["intervalSize"].cast(IntegerType()))
    union = union.withColumn("logNumber",union["logNumber"].cast(IntegerType()))

    union.write.format('csv').mode("overwrite").save("./output/translated", header="true", emptyValue="")
    
    return union