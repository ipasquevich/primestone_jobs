
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



# BLOQUE CLEANING

def cleaner(union, spark):

        #Hago el request para traer el json de configuracion
        base_url = "http://80f56133-default-platforms-88e5-2108999998.us-east-1.elb.amazonaws.com/platform_settings/get_configuration"

        headers = {"Content-Type":"application/json", "Transaction-Id":"test"
                , "User-Id":"test" , "owner":"test"}

        # Hago el request usando urlib
        req = request.Request(base_url,headers = headers)
        response = request.urlopen(req).read()
        response = json.loads(response)
        req_config = response["data"][0]


        # extraigo los atributos y los guardo en variables
        for item in req_config["configPastFutureDays"]:
                if item['type'] == 'LP':
                        LP_futuro = item['future']
                        LP_pasado = item['past']
                else:
                        RE_futuro = item['future']
                        RE_pasado = item['past']

        ceros = req_config["withNullsOrCero"]["withCeros"]
        nulos = req_config["withNullsOrCero"]['withNull']

        lista_apagon = req_config["QualityCode"]["PowerOutage"]


        def check_empty(df):
                return len(df.head(1)) > 0



        # PARTE 1: FLAGS
        #descarte de banderas, si alguna es false, se descarta el registro (se guarda en otro dataframe que se 
        # va a usar para crear los logs)

        if check_empty(union):
                # primero revisamos por la bandera de estado de asociacion, si esta es falsa todos los registros con dicha condicion
                # van a separarse el dataframe original para ir al dataframe de logs

                df_logs = union.filter(union.result_rD == False).withColumn("Descripcion_log",lit("Estado de asociacion"))
                df_logs.show()
                df_logs.printSchema()

                union = union.filter(union.result_rD == True)


                # Ahora separamos aquellos registros que tienen falso en el estado de la variable o en la bandera de guardado
                # y lo concatenamos al df de logs

                df_logs_aux = union.filter((union.result_sPV == False) | (union.toStock == False))\
                .withColumn("Descripcion_log",lit("result variable - to stock "))

                df_logs = df_logs.union(df_logs_aux).coalesce(1)

                union = union.filter((union.result_sPV == True) & (union.toStock == True))




        # PARTE 2: DIAS PASADOS FUTUROS
        # en base a la respuesta del request (req_config) tenemos que encontrar aquellas lecturas de los dias que correspondan

        if check_empty(union):

                # creamos la funcion para aplicar a cada fila del df
                hoy = datetime.today()
                date_format = '%Y-%m-%d'

                def pasado_futuro(row):
                        last_date = datetime.strptime(row.lastReadDate[:10],date_format)
                        asoc = datetime.strptime(row.relationStartDate[:10],date_format)
                        reading_time = datetime.strptime(row.readingUtcLocalTime[:10],date_format)
                        if (asoc - last_date).days > 0:
                                last_date = asoc

                        if row.readingType == "LOAD PROFILE READING":
                                return (last_date - timedelta(days=LP_pasado)) <= reading_time <= (hoy + timedelta(days=LP_futuro))
                        else:
                                return (last_date - timedelta(days=RE_pasado)) <= reading_time <= (hoy + timedelta(days=RE_futuro))



                # Aplico la funcion al dataframe
                udf_object = udf(pasado_futuro, BooleanType())
                union = union.withColumn("lastReadDate_result", udf_object(struct([union[x] for x in union.columns])))


                # Ahora separamos aquellos registros que tienen falso en el lastReadDate_result
                # y lo concatenamos al df de logs

                df_logs_aux = union.filter(union.lastReadDate_result == False)\
                .withColumn("Descripcion_log",lit("dias pasado-futuro"))\
                .drop("lastReadDate_result")

                df_logs = df_logs.union(df_logs_aux).coalesce(1)

                union = union.filter(union.lastReadDate_result == True)
                union = union.drop("lastReadDate_result")


        # PARTE 3: DUPLICADOS E INCONSISTENCIAS
        # las lecturas totalmente repetidas hay que descartar todas menos una (paso 1), pero si hay alguna lectura con el mismo conjunto
        # de datos (servicePointId, deviceId, meteringType, variableId, readingLocalTime,logNumber) pero con distinto  
        # readingsValue entonces se descartan todas las filas por inconsistencia en los datos (paso 2).

        if check_empty(union):

                # paso 1 (DUPLICADOS)
                #union_unique = union.dropDuplicates(['servicePointId','deviceId','meteringType','variableId','readingLocalTime','logNumber','readingsValue'])
                #df_logs_aux = union.subtract(union_unique).withColumn("Descripcion_log",lit("Duplicados"))

                ### Get Duplicate rows in pyspark

                df_logs_aux = union.groupBy('servicePointId','deviceId','meteringType','variableId','readingLocalTime','logNumber','readingsValue').count()
                df_logs_aux = df_logs_aux.filter(df_logs_aux["count"] > 1)
                df_logs_aux = df_logs_aux.select( col("servicePointId").alias("servicePointId_2")
                                                                ,col("deviceId").alias("deviceId_2")
                                                                ,col("meteringType").alias("meteringType_2")
                                                                ,col("variableId").alias("variableId_2")
                                                                ,col("readingLocalTime").alias("readingLocalTime_2")
                                                                ,col("logNumber").alias("logNumber_2")
                                                                ,col("readingsValue").alias("readingsValue_2") )
                df_logs_aux = df_logs_aux.withColumn("Descripcion_log",lit("Duplicados"))
                df_logs_aux = union.join(df_logs_aux, [(union.servicePointId == df_logs_aux.servicePointId_2)
                                                        , (union.deviceId == df_logs_aux.deviceId_2)
                                                        , (union.meteringType == df_logs_aux.meteringType_2)
                                                        , (union.variableId == df_logs_aux.variableId_2)
                                                        , (union.readingLocalTime == df_logs_aux.readingLocalTime_2)
                                                        , (union.logNumber.eqNullSafe(df_logs_aux.logNumber_2))
                                                        , (union.readingsValue == df_logs_aux.readingsValue_2)]
                                                        ,how = 'inner').coalesce(1)
                df_logs_aux = df_logs_aux.dropDuplicates(['servicePointId','deviceId','meteringType','variableId','readingLocalTime','logNumber','readingsValue'])
                columns_to_drop = ["servicePointId_2","deviceId_2","meteringType_2","variableId_2","readingLocalTime_2","logNumber_2","readingsValue_2"]
                df_logs_aux = df_logs_aux.drop(*columns_to_drop)

                union = union.dropDuplicates(['servicePointId','deviceId','meteringType','variableId','readingLocalTime','logNumber','readingsValue'])

                df_logs = df_logs.union(df_logs_aux).coalesce(1)


                # paso 2 (INCONSISTENCIAS)
                # funciona con el left anti join
                union_unique = union.dropDuplicates(['servicePointId','deviceId','meteringType','variableId','readingLocalTime','logNumber'])
                df_duplicates = union.subtract(union_unique).select( 'servicePointId','deviceId','meteringType'
                                                                ,'variableId','readingLocalTime','logNumber')
                df_final = union_unique.join(df_duplicates, [(union_unique.servicePointId == df_duplicates.servicePointId)
                                                        , (union_unique.deviceId == df_duplicates.deviceId)
                                                        , (union_unique.meteringType == df_duplicates.meteringType)
                                                        , (union_unique.variableId == df_duplicates.variableId)
                                                        , (union_unique.readingLocalTime == df_duplicates.readingLocalTime)
                                                        , (union_unique.logNumber.eqNullSafe(df_duplicates.logNumber))]
                                                        ,how = 'left_anti').coalesce(1)
                # con esta linea salvamos la union entre nulos de pyspark, (union_unique.logNumber.eqNullSafe(df_duplicates.logNumber))

                df_logs_aux = union.subtract(union_unique).withColumn("Descripcion_log",lit("Valores inconsitentes")).coalesce(1)
                df_logs = df_logs.union(df_logs_aux).coalesce(1)

                union = df_final






        # PARTE 4: VERSIONADO

        # WIP (Work in progress)






        # PARTE 5: RELLENO DE CEROS Y NULOS

        if check_empty(union.filter(union.readingType == "LOAD PROFILE READING")):
                # Se trabaja solo para los datos de LOAD PROFILE READING
                df_LoadProfile = union.filter(union.readingType == "LOAD PROFILE READING")
                # Crear columna quality_flag
                df_LoadProfile = df_LoadProfile.withColumn("quality_flag", concat(col("qualityCodesSystemId"),lit("-"),col("qualityCodesCategorization"),lit("-"),col("qualityCodesIndex")))

                variables_id = df_LoadProfile.select("variableId").distinct().collect()
                variables_id = [row.variableId for row in variables_id]


                # definimos algunas funciones

                def elem_ranges(all_elem,elem):
                        """
                        Funcion que devuelve una lista con listas incluyendo los rangos de tiempos faltantes.
                        Input:
                        all_elem (string): todos los timestamps separados por coma en un string
                        elem (list): lista de los timestamp presentes en nuestro df a enriquecer
                        """
                        aux_list = []
                        for i in elem[1:]:
                                include,ommit = all_elem.split(i)
                                aux_list.append(include[:-1])
                                all_elem = i + ommit
                        aux_list.append(elem[-1])
                        return aux_list

                # funcion para rellenar el campo readingsValue con nulos o ceros dependiendo de si la bandera es de apagon.
                def func_apagon(row):
                        if (row.quality_flag in lista_apagon) and (not row.referencia):
                                return relleno
                        else:
                                return row.readingsValue_ref


                # creo funcion que va a rellenar en otra columna (a la que vamos a llamar validacion_intervalos) con nulo cuando el valor del
                # campo quality_flag no sea bandera de apagon, y un 1 cuando la bandera indique apagon
                # esta nueva columna nos va a permitir identificar si hay que contar o no ese registro a la hora de hacer la validacion de 
                # intervalos
                def func_apagon_intervalos(row):
                        if (row.quality_flag in lista_apagon) or (row.referencia):
                                return "1"
                        else:
                                return None


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
                        lista = [min_ts]
                        for rep in range(1,mins//interval):
                                lista.append(min_ts + timedelta(minutes = rep*interval))
                        lista.append(max_ts)

                        all_elem = [fecha.strftime('%Y-%m-%d %H:%M:%S') for fecha in lista]
                        all_elem = ",".join(all_elem)

                        elem = df_LP.select("readingUtcLocalTime").orderBy("readingUtcLocalTime",ascending=True).collect()
                        elem = [row.readingUtcLocalTime for row in elem]


                        # genero lista que va a formar parte del df
                        to_df = elem_ranges(all_elem,elem)
                        to_df = [ls.split(",") for ls in to_df]
                        # genero lista para el join, los valores son el primer elemento de cada lista de to_df
                        to_df_join = [item[0] for item in to_df]
                        # creo el df con las listas generadas
                        data = zip(to_df_join, to_df)
                        schema = StructType([
                        StructField('readingUtcLocalTime_2', StringType(), True),
                        StructField('timestamps_arrays', ArrayType(StringType()), True)
                        ])
                        rdd = spark.sparkContext.parallelize(data)
                        df_timestamps = spark.createDataFrame(rdd,schema).coalesce(1)


                        # hago un join del df timestamp explodeado con una porcion del df original para obtener un nuevo df que tenga los valores de value y las
                        # quality flags como estan originalmente en las fechas presentes en el df original y si no existen tendran nulo.
                        df_portion = df_LP.select(col("readingUtcLocalTime").alias("readingUtcLocalTime_ref")
                                                ,col("readingLocalTime").alias("readingLocalTime_ref")
                                                ,col("readingDateSource").alias("readingDateSource_ref")
                                                ,col("readingsValue").alias("readingsValue_ref")
                                                ,col("qualityCodesSystemId").alias("qualityCodesSystemId_ref")
                                                ,col("qualityCodesCategorization").alias("qualityCodesCategorization_ref")
                                                ,col("qualityCodesIndex").alias("qualityCodesIndex_ref")                       
                                                )\
                                                .withColumn("referencia",lit("Original"))

                        df_timestamps_comp = df_timestamps.withColumn("complete_interval_ref", explode("timestamps_arrays")).select("complete_interval_ref")

                        df_reference = df_timestamps_comp.join(df_portion, [(df_timestamps_comp.complete_interval_ref == df_portion.readingUtcLocalTime_ref)]
                                                                ,how = 'left').coalesce(1)




                        # hago un join del df original con el df timestamp para obtener los datos de los timestamp faltantes en el
                        # df original
                        df_LP = df_LP.join(df_timestamps, [(df_LP.readingUtcLocalTime == df_timestamps.readingUtcLocalTime_2)]
                                                                ,how = 'inner').coalesce(1)
                        df_LP = df_LP.drop("readingUtcLocalTime_2")

                        # explode de los arrays de la columna 
                        df_LP = df_LP.withColumn("complete_interval", explode("timestamps_arrays"))
                        df_LP = df_LP.drop("timestamps_arrays")


                        # hago join del df original con el df de referencia, para asi tener los valores consistentes de los campos valor y quality flags
                        df_LP = df_LP.join(df_reference, [(df_LP.complete_interval == df_reference.complete_interval_ref)]
                                                                ,how = 'inner').coalesce(1)


                        # hago el rellenado con nulos y ceros
                        # veo si rellanar con ceros o con nulos
                        
                        if ceros:
                                relleno = "0"
                        else:
                                relleno = None


                        # Create your UDF object (which accepts func_apagon  python function)
                        udf_object = udf(func_apagon, StringType())
                        df_LP = df_LP.withColumn("readingsValue_ref", udf_object(struct([df_LP[x] for x in df_LP.columns])))


                        # Create your UDF object (which accepts func_apagon_intervalos python function)
                        udf_object = udf(func_apagon_intervalos, StringType())
                        df_LP = df_LP.withColumn("validacion_intervalos", udf_object(struct([df_LP[x] for x in df_LP.columns])))


                        # Elimino algunas columnas y renombro otras para mantener el formato de salida esperado

                        columns_to_drop = ['readingUtcLocalTime_ref', 'complete_interval_ref', 'readingUtcLocalTime', 'readingDateSource',
                                                'readingLocalTime', "qualityCodesSystemId" ,"qualityCodesCategorization", 'qualityCodesIndex',
                                                "readingsValue", "referencia" , "quality_flag"]
                        df_LP = df_LP.drop(*columns_to_drop)


                        if iteracion == 0:
                                df_LoadProfile_final = df_LP
                        else:
                                df_LoadProfile_final = df_LoadProfile.union(df_LP).coalesce(1)


                df_LoadProfile = df_LoadProfile_final.withColumnRenamed("complete_interval","readingUtcLocalTime").withColumnRenamed("readingLocalTime_ref","readingLocalTime") .withColumnRenamed("readingDateSource_ref","readingDateSource").withColumnRenamed("qualityCodesSystemId_ref","qualityCodesSystemId").withColumnRenamed("qualityCodesCategorization_ref","qualityCodesCategorization").withColumnRenamed("qualityCodesIndex_ref","qualityCodesIndex").withColumnRenamed("readingsValue_ref","readingsValue")

                # selecciona las columnas en el orden que van y ademas hacer el union con el df original filtrado por
                # los otros tipos de lecturas

                df_Registers_Events = union.filter((union.readingType == "REGISTERS") | (union.readingType == "EVENTS"))
                df_Registers_Events = df_Registers_Events.withColumn("validacion_intervalos",lit(""))

                lista_columnas = ["servicePointId","readingType","variableId","deviceId","meteringType","readingUtcLocalTime","readingDateSource","readingLocalTime","dstStatus"
                ,"channel","unitOfMeasure","qualityCodesSystemId","qualityCodesCategorization","qualityCodesIndex","intervalSize","logNumber"
                ,"version","readingsValue","primarySource","readingsSource","owner","guidFile","estatus"
                ,"registersNumber","eventsCode","agentId","agentDescription","multiplier","deviceMaster","deviceDescription","deviceStatus"
                ,"serial","accountNumber","servicePointTimeZone","connectionType","relationStartDate","relationEndDate"
                ,"deviceType","brand","model","validacion_intervalos"]

                df_LoadProfile = df_LoadProfile.select(*lista_columnas)
                df_Registers_Events = df_Registers_Events.select(*lista_columnas)

                union = df_LoadProfile.union(df_Registers_Events).coalesce(1)

        
        # PARTE 6: VALIDACION DE INTERVALOS

        # WIP (Work in progress)


        # escribir csv
        union.write.format('csv').mode("overwrite").save("./output/cleaned", header="true", emptyValue="")
        df_logs.write.format('csv').mode("overwrite").save("./output/cleaning_dump", header="true", emptyValue="")

        return union