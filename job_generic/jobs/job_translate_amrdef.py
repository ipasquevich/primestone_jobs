
import json
import random
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType 
from pyspark.sql.functions import col, lit, udf, when, explode, arrays_zip, concat
from pyspark import SparkContext ,SparkConf
from functools import reduce 
from collections import OrderedDict
from datetime import datetime, timedelta

def translator(xml_file_path,spark):

    # Cada <MeterReadings> ... <MeterReadings/> va ser tomado como un row en el dataframe df.
    root_tag = "AMRDEF"
    row_tag = "MeterReadings"

    str_sch = spark.read.text("../configs/schemaAMRDEF_sim_allstring.json").first()[0]
    schema = StructType.fromJson(json.loads(str_sch))

    bucket = "s3://primestone-raw-dev/"
    #bucket = "s3://" + args['bucket_name'] + '/'

    filename = "amrdef-test.xml"
    #filename = args['file_name']

    guid = filename.split(".xml")[0]

    s3_path = bucket + filename

    filename = "PRIMEREAD/AMRDEF/2020/19/Actualizacion+de+datos.xlsx"
    hes = filename.split("/")[1]
    owner = filename.split("/")[0]

    df = spark.read.format("com.databricks.spark.xml").options(root_tag=root_tag).options(row_tag=row_tag).options(nullValue="").options(valueTag="_valueTag").option("columnNameOfCorruptRecord", "algunNombre") \
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
    max_demand_data_readings = df.withColumn("TouBucket", col("MaxDemandData.MaxDemandSpec._TouBucket")) \
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
    max_demand_data_readings = max_demand_data_readings.withColumn("tmp", arrays_zip("TouBucket", "Direction","UOM", "Value", "Multiplier","TimeStamp")) \
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
    demand_reset_count_readings = df.withColumn("Count", col("DemandResetCount._Count")) \
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
    demand_reset_count_readings = demand_reset_count_readings.withColumn("tmp", arrays_zip("Count", "TimeStamp","UOM")) \
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
    consumption_data_readings = df.withColumn("TouBucket", col("ConsumptionData.ConsumptionSpec._TouBucket")) \
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
    consumption_data_readings = consumption_data_readings.withColumn("tmp", arrays_zip("TouBucket", "Direction","UOM", "Value", "Multiplier","TimeStamp")) \
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
    coincident_demand_data_readings = df.withColumn("TouBucket", col("CoincidentDemandData.CoincidentDemandSpec._TouBucket")) \
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
    coincident_demand_data_readings = coincident_demand_data_readings.withColumn("tmp", arrays_zip("TouBucket", "Direction","UOM", "Value", "Multiplier", "TimeStamp")) \
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
    cumulative_demand_data_readings = df.withColumn("TouBucket", col("CumulativeDemandData.CumulativeDemandSpec._TouBucket")) \
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
    cumulative_demand_data_readings = cumulative_demand_data_readings.withColumn("tmp", arrays_zip("TouBucket", "Direction","UOM", "Value", "Multiplier","TimeStamp")) \
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
    demand_reset_readings = df.withColumn("TimeStamp", col("DemandReset._TimeStamp")) \
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
    demand_reset_readings = demand_reset_readings.withColumn("tmp", arrays_zip("TimeStamp")) \
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
    instrumentation_value_readings = df.withColumn("TimeStamp", col("InstrumentationValue._TimeStamp")) \
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
    instrumentation_value_readings = instrumentation_value_readings.withColumn("tmp", arrays_zip("TimeStamp","Phase","Name","Value")) \
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

    status_data = []
    for row in df.rdd.collect():
            if row.Meter != None:
                    meter_readings_source = row._Source
                    meter_readings_sourceirn = row._SourceIrn
                    meter_readings_collection_time = row._CollectionTime
                    meter_irn = row.Meter._MeterIrn
                    meter_sdp_ident = row.Meter._SdpIdent
                    meter_description = row.Meter._Description
                    meter_is_active = row.Meter._IsActive
                    meter_serial_number = row.Meter._SerialNumber
                    meter_meter_type = row.Meter._MeterType
                    meter_account_ident = row.Meter._AccountIdent
                    meter_time_zone_index = row.Meter._TimeZoneIndex
                    meter_media_type = row.Meter._MediaType
                    meter_install_date = row.Meter._InstallDate
                    meter_removal_date = row.Meter._RemovalDate
                    if row.Statuses != None:
                            for statuses in row.Statuses:
                                    for status in statuses.Status:
                                            status_id = status._Id
                                            status_category = status._Category
                                            status_name = status._Name
                                            status_value = status._Value

                                            service_point_id = meter_sdp_ident
                                            if service_point_id == None:
                                                    service_point_id = meter_irn

                                            if status_value == "true": #Esta lectura solo se toma si Value=0 o magnitud
                                                    status_value = 0
                                            if status_value != "false":
                                                    status_data.append ({
                                                            "servicePointId":service_point_id,
                                                            "readingType":"EVENTS",
                                                            "variableId":status_category + ' ' + status_name,
                                                            "deviceId": meter_irn,
                                                            "meteringType": "MAIN",
                                                            "readingUtcLocalTime": "",
                                                            "readingDateSource": meter_readings_collection_time,
                                                            "readingLocalTime": meter_readings_collection_time,
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
                                                            "readingsValue": status_value,
                                                            "primarySource":meter_readings_source,
                                                            "owner":owner,
                                                            "guidFile":guid,
                                                            "estatus": "Activo",
                                                            "registersNumber":"",
                                                            "eventsCode":status_id,
                                                            "agentId":"",
                                                            "agentDescription":"",
                                                            "_SourceIrn":str(meter_readings_sourceirn),
                                                            "_Description":str(meter_description),
                                                            "_IsActive":meter_is_active,
                                                            "_SerialNumber":str(meter_serial_number),
                                                            "_MeterType":str(meter_meter_type),
                                                            "_AccountIdent":str(meter_account_ident),
                                                            "_TimeZoneIndex":str(meter_time_zone_index),
                                                            "_MediaType":str(meter_media_type),
                                                            "_InstallDate":str(meter_install_date),
                                                            "_RemovalDate":str(meter_removal_date)
                                                    })

    status_data = spark.sparkContext.parallelize(status_data) \
                            .map(lambda x: Row(**OrderedDict(x.items())))

    if status_data.isEmpty() == False:
            status_data_readings = spark.createDataFrame(status_data.coalesce(1)) \
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
    load_profile_summary_readings = df.withColumn("UOM", col("LoadProfileSummary.Channel._UOM")) \
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
    load_profile_summary_readings = load_profile_summary_readings.withColumn("tmp", arrays_zip("UOM","Direction","SumOfIntervalValues","Multiplier")) \
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
    outage_count_readings = df.withColumn("ReadingTime", col("OutageCountSummary.OutageCount._ReadingTime")) \
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
    outage_count_readings = outage_count_readings.withColumn("tmp", arrays_zip("ReadingTime","Value")) \
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
    previous_outage_count_readings = df.withColumn("Value", col("OutageCountSummary.OutageCount._Value")) \
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
    previous_outage_count_readings = previous_outage_count_readings.withColumn("tmp", arrays_zip("PreviousReadingTime","Value")) \
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
    reverse_energy_summary_readings = df.withColumn("CurrentValue", col("ReverseEnergySummary.ReverseEnergy._CurrentValue")) \
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
    reverse_energy_summary_readings=reverse_energy_summary_readings.withColumn("tmp", arrays_zip("CurrentValue")) \
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
    events_data_readings = df.select("EventData",
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
    interval_data = []
    for row in df.rdd.collect():
            if row.Meter != None:
                    meter_readings_source = row._Source
                    meter_readings_sourceirn = row._SourceIrn
                    meter_irn = row.Meter._MeterIrn
                    meter_sdp_ident = row.Meter._SdpIdent
                    meter_description = row.Meter._Description
                    meter_is_active = row.Meter._IsActive
                    meter_serial_number = row.Meter._SerialNumber
                    meter_meter_type = row.Meter._MeterType
                    meter_account_ident = row.Meter._AccountIdent
                    meter_time_zone_index = row.Meter._TimeZoneIndex
                    meter_media_type = row.Meter._MediaType
                    meter_install_date = row.Meter._InstallDate
                    meter_removal_date = row.Meter._RemovalDate
                    if row.IntervalData != None:
                            for interval_data in row.IntervalData:
                                    interval_spec_channel = interval_data.IntervalSpec._Channel
                                    interval_spec_direction = interval_data.IntervalSpec._Direction
                                    interval_spec_interval = interval_data.IntervalSpec._Interval
                                    interval_spec_multiplier = interval_data.IntervalSpec._Multiplier
                                    interval_spec_uom = interval_data.IntervalSpec._UOM

                                    for reading in interval_data.Reading:
                                            reading_rawreading = reading._RawReading
                                            reading_timestamp = reading._TimeStamp

                                            service_point_id = meter_sdp_ident
                                            if service_point_id == None:
                                                    service_point_id = meter_irn

                                            dst = ""
                                            qualitycode_systemid = ""
                                            qualitycode_categorization = ""
                                            qualitycode_index = ""
                                            if reading.QualityFlags != None:
                                                    quality_flag = reading.QualityFlags
                                                    if quality_flag._TimeChanged != None:
                                                            qualitycode_systemid = "2"
                                                            qualitycode_categorization = "4"
                                                            qualitycode_index = "9"
                                                    elif quality_flag._ClockSetBackward != None:
                                                            qualitycode_systemid = "1"
                                                            qualitycode_categorization = "4"
                                                            qualitycode_index = "128"
                                                    elif quality_flag._LongInterval != None:
                                                            qualitycode_systemid = "1"
                                                            qualitycode_categorization = "4"
                                                            qualitycode_index = "3"
                                                    elif quality_flag._ClockSetForward != None:
                                                            qualitycode_systemid = "1"
                                                            qualitycode_categorization = "4"
                                                            qualitycode_index = "64"
                                                    elif quality_flag._PartialInterval != None:
                                                            qualitycode_systemid = "1"
                                                            qualitycode_categorization = "4"
                                                            qualitycode_index = "2"
                                                    elif quality_flag._InvalidTime != None:
                                                            qualitycode_systemid = "1"
                                                            qualitycode_categorization = "1"
                                                            qualitycode_index = "9"
                                                    elif quality_flag._SkippedInterval != None:
                                                            qualitycode_systemid = "1"
                                                            qualitycode_categorization = "4"
                                                            qualitycode_index = "4"
                                                    elif quality_flag._CompleteOutage != None or quality_flag._PartialOutage != None:
                                                            qualitycode_systemid = "1"
                                                            qualitycode_categorization = "2"
                                                            qualitycode_index = "32"
                                                    elif quality_flag._PulseOverflow != None:
                                                            qualitycode_systemid = "1"
                                                            qualitycode_categorization = "4"
                                                            qualitycode_index = "1"
                                                    elif quality_flag._TestMode != None:
                                                            qualitycode_systemid = "1"
                                                            qualitycode_categorization = "4"
                                                            qualitycode_index = "5"
                                                    elif quality_flag._Tamper != None:
                                                            qualitycode_systemid = "1"
                                                            qualitycode_categorization = "3"
                                                            qualitycode_index = "0"
                                                    elif quality_flag._SuspectedOutage != None or quality_flag._Restoration != None:
                                                            qualitycode_systemid = "1"
                                                            qualitycode_categorization = "2"
                                                            qualitycode_index = "0"
                                                    elif quality_flag._DST != None:
                                                            qualitycode_systemid = "1"
                                                            qualitycode_categorization = "4"
                                                            qualitycode_index = "16"
                                                            dst = "1"
                                                    elif quality_flag._InvalidValue != None:
                                                            qualitycode_systemid = "2"
                                                            qualitycode_categorization = "5"
                                                            qualitycode_index = "256"

                                            interval_data.append ({
                                                    "servicePointId":service_point_id,
                                                    "readingType":"LOAD PROFILE READING",
                                                    "variableId":interval_spec_uom + ' ' + interval_spec_direction,
                                                    "deviceId": meter_irn,
                                                    "meteringType": "MAIN",
                                                    "readingUtcLocalTime": "",
                                                    "readingDateSource": reading_timestamp,
                                                    "readingLocalTime": reading_timestamp,
                                                    "dstStatus":dst,
                                                    "channel":interval_spec_channel,
                                                    "unitOfMeasure":interval_spec_uom,
                                                    "qualityCodesSystemId":qualitycode_systemid,
                                                    "qualityCodesCategorization":qualitycode_categorization,
                                                    "qualityCodesIndex":qualitycode_index,
                                                    "intervalSize":interval_spec_interval,
                                                    "logNumber": "1",
                                                    "ct":"",
                                                    "pt":"",
                                                    "ke":interval_spec_multiplier,
                                                    "sf":"",
                                                    "version":"Original",
                                                    "readingsValue":reading_rawreading,
                                                    "primarySource":meter_readings_source,
                                                    "owner":owner,
                                                    "guidFile":guid,
                                                    "estatus": "Activo",
                                                    "registersNumber":"",
                                                    "eventsCode":"",
                                                    "agentId":"",
                                                    "agentDescription":"",
                                                    "_SourceIrn":str(meter_readings_sourceirn),
                                                    "_Description":str(meter_description),
                                                    "_IsActive":meter_is_active,
                                                    "_SerialNumber":str(meter_serial_number),
                                                    "_MeterType":str(meter_meter_type),
                                                    "_AccountIdent":str(meter_account_ident),
                                                    "_TimeZoneIndex":str(meter_time_zone_index),
                                                    "_MediaType":str(meter_media_type),
                                                    "_InstallDate":str(meter_install_date),
                                                    "_RemovalDate":str(meter_removal_date)
                                                    })
    interval_data = spark.sparkContext.parallelize(interval_data) \
                            .map(lambda x: Row(**OrderedDict(x.items())))
    if interval_data.isEmpty() == False:
            interval_data_readings = spark.createDataFrame(interval_data.coalesce(1)) \
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


    readings_list = [max_demand_data_readings, demand_reset_count_readings, \
            consumption_data_readings, coincident_demand_data_readings, \
            cumulative_demand_data_readings, demand_reset_readings, \
            instrumentation_value_readings, \
            load_profile_summary_readings, outage_count_readings, \
            reverse_energy_summary_readings, events_data_readings ]

    if interval_data.isEmpty() == False:
            readings_list.append(interval_data_readings)

    if status_data.isEmpty() == False:
            readings_list.append(status_data_readings)

    df_union = union_all(*readings_list).coalesce(1) #Aca los estoy uniendo en una sola partición, si se saca el .coalesce(1) se van a crear distintas particiones para c/u

    # Se unen los 4 campos: ct, pt, ke y sf en un solo campo con el nombre Multipliers
    df_union = df_union.withColumn("multiplier",
                            concat(
                                    lit("'MultiplierValues':{'ke':'"), \
                                    col("ke"), \
                                    lit("'}")
                                    )
                            )
    # Se dropean las columnas que se unieron en el paso anterior
    columns_to_drop = ['ct', 'pt', 'ke', 'sf']
    df_union = df_union.drop(*columns_to_drop)


    df_union = df_union.withColumn("_IsActive", 
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
    df_union = df_union.fillna( { 'relationStartDate':'', 'relationEndDate':'' } )

    # Las filas que tengan el valor variableId nulo, no sirven, por lo que se tiran
    # Preguntar que hacemos en este caso a primestone, si las tiramos forever o es cambiarlas de bucket
    df_union = df_union.filter(df_union.variableId.isNotNull())

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

    df_union = df_union.replace(dic_uom, subset=["unitOfMeasure"])

    # Cambio tipo de datos.
    df_union = df_union.withColumn("channel",df_union["channel"].cast(IntegerType()))
    df_union = df_union.withColumn("unitOfMeasure",df_union["unitOfMeasure"].cast(IntegerType()))
    df_union = df_union.withColumn("intervalSize",df_union["intervalSize"].cast(IntegerType()))
    df_union = df_union.withColumn("logNumber",df_union["logNumber"].cast(IntegerType()))

    df_union.write.format('csv').mode("overwrite").save("./output/translated", header="true", emptyValue="")
    
    return df_union
