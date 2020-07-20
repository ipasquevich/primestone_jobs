import json
import random
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType ,ArrayType
from pyspark.sql.functions import lit, udf, struct, col, concat, explode, max, min
from pyspark import SparkContext ,SparkConf
from functools import reduce 
from collections import OrderedDict
from datetime import datetime, timedelta
from urllib import request, parse



# BLOQUE CLEANING

def cleaner(df_union, spark):

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
                        lp_futuro = item['future']
                        lp_pasado = item['past']
                else:
                        re_futuro = item['future']
                        re_pasado = item['past']

        ceros = req_config["withNullsOrCero"]["withCeros"]

        lista_apagon = req_config["QualityCode"]["PowerOutage"]

        def check_empty(df):
                return len(df.head(1)) > 0



        # PARTE 1: FLAGS
        #descarte de banderas, si alguna es false, se descarta el registro (se guarda en otro dataframe que se 
        # va a usar para crear los logs)
        print(df_union.head(1))
        print(len(df_union.head(1)))
        print(len(df_union.head(1))>0)
        if check_empty(df_union):
                # primero revisamos por la bandera de estado de asociacion, si esta es falsa todos los registros con dicha condicion
                # van a separarse el dataframe original para ir al dataframe de logs

                df_logs = df_union.filter(df_union.result_rD == False).withColumn("Descripcion_log",lit("Estado de asociacion"))
                df_logs.show()
                df_logs.printSchema()

                df_union = df_union.filter(df_union.result_rD == True)


                # Ahora separamos aquellos registros que tienen falso en el estado de la variable o en la bandera de guardado
                # y lo concatenamos al df de logs

                df_logs_aux = df_union.filter((df_union.result_sPV == False) | (df_union.toStock == False))\
                .withColumn("Descripcion_log",lit("result variable - to stock "))

                df_logs = df_logs.union(df_logs_aux).coalesce(1)

                df_union = df_union.filter((df_union.result_sPV == True) & (df_union.toStock == True))

                df_union.write.format('csv').mode("overwrite").save("./output/cleaned/paso1", header="true", emptyValue="")


        # PARTE 2: DIAS PASADOS FUTUROS
        # en base a la respuesta del request (req_config) tenemos que encontrar aquellas lecturas de los dias que correspondan

        if check_empty(df_union):

                # creamos la funcion para aplicar a cada fila del df
                hoy = datetime.today()
                date_format = '%Y-%m-%d'

                def pasado_futuro(row):
                        last_date = datetime.strptime(row.lastReadDate[:10],date_format)
                        asoc = datetime.strptime(row.relationStartDate[:10],date_format)
                        #### Reading Time is empty!??!?!#### CHARLY
                        if row.readingUtcLocalTime is not None:
                                reading_time = datetime.strptime(row.readingUtcLocalTime[:10],date_format)
                        else:
                                reading_time = last_date
                        #### Reading Time is empty!??!?!#### CHARLY
                        if (asoc - last_date).days > 0:
                                last_date = asoc

                        if row.readingType == "LOAD PROFILE READING":
                                return (last_date - timedelta(days=lp_pasado)) <= reading_time <= (hoy + timedelta(days=lp_futuro))
                        else:
                                return (last_date - timedelta(days=re_pasado)) <= reading_time <= (hoy + timedelta(days=re_futuro))



                # Aplico la funcion al dataframe
                udf_object = udf(pasado_futuro, BooleanType())
                df_union = df_union.withColumn("lastReadDate_result", udf_object(struct([df_union[x] for x in df_union.columns])))


                # Ahora separamos aquellos registros que tienen falso en el lastReadDate_result
                # y lo concatenamos al df de logs

                df_logs_aux = df_union.filter(df_union.lastReadDate_result == False)\
                .withColumn("Descripcion_log",lit("dias pasado-futuro"))\
                .drop("lastReadDate_result")

                df_logs = df_logs.union(df_logs_aux).coalesce(1)

                df_union = df_union.filter(df_union.lastReadDate_result == True)
                df_union = df_union.drop("lastReadDate_result")

                df_union.write.format('csv').mode("overwrite").save("./output/cleaned/paso2", header="true", emptyValue="")

        
        # PARTE 3: DUPLICADOS E INCONSISTENCIAS
        # las lecturas totalmente repetidas hay que descartar todas menos una (paso 1), pero si hay alguna lectura con el mismo conjunto
        # de datos (servicePointId, deviceId, meteringType, variableId, readingLocalTime,logNumber) pero con distinto  
        # readingsValue entonces se descartan todas las filas por inconsistencia en los datos (paso 2).

        if check_empty(df_union):

                # paso 1 (DUPLICADOS)
                #union_unique = union.dropDuplicates(['servicePointId','deviceId','meteringType','variableId','readingLocalTime','logNumber','readingsValue'])
                #df_logs_aux = union.subtract(union_unique).withColumn("Descripcion_log",lit("Duplicados"))

                ### Get Duplicate rows in pyspark

                df_logs_aux = df_union.groupBy('servicePointId','deviceId','meteringType','variableId','readingLocalTime','logNumber','readingsValue').count()
                df_logs_aux = df_logs_aux.filter(df_logs_aux["count"] > 1)
                df_logs_aux = df_logs_aux.select( col("servicePointId").alias("servicePointId_2")
                                                                ,col("deviceId").alias("deviceId_2")
                                                                ,col("meteringType").alias("meteringType_2")
                                                                ,col("variableId").alias("variableId_2")
                                                                ,col("readingLocalTime").alias("readingLocalTime_2")
                                                                ,col("logNumber").alias("logNumber_2")
                                                                ,col("readingsValue").alias("readingsValue_2") )
                df_logs_aux = df_logs_aux.withColumn("Descripcion_log",lit("Duplicados"))
                df_logs_aux = df_union.join(df_logs_aux, [(df_union.servicePointId == df_logs_aux.servicePointId_2)
                                                        , (df_union.deviceId == df_logs_aux.deviceId_2)
                                                        , (df_union.meteringType == df_logs_aux.meteringType_2)
                                                        , (df_union.variableId == df_logs_aux.variableId_2)
                                                        , (df_union.readingLocalTime == df_logs_aux.readingLocalTime_2)
                                                        , (df_union.logNumber.eqNullSafe(df_logs_aux.logNumber_2))
                                                        , (df_union.readingsValue == df_logs_aux.readingsValue_2)]
                                                        ,how = 'inner').coalesce(1)
                df_logs_aux = df_logs_aux.dropDuplicates(['servicePointId','deviceId','meteringType','variableId','readingLocalTime','logNumber','readingsValue'])
                columns_to_drop = ["servicePointId_2","deviceId_2","meteringType_2","variableId_2","readingLocalTime_2","logNumber_2","readingsValue_2"]
                df_logs_aux = df_logs_aux.drop(*columns_to_drop)

                df_union = df_union.dropDuplicates(['servicePointId','deviceId','meteringType','variableId','readingLocalTime','logNumber','readingsValue'])

                df_logs = df_logs.union(df_logs_aux).coalesce(1)


                # paso 2 (INCONSISTENCIAS)
                # funciona con el left anti join
                union_unique = df_union.dropDuplicates(['servicePointId','deviceId','meteringType','variableId','readingLocalTime','logNumber'])
                df_duplicates = df_union.subtract(union_unique).select( 'servicePointId','deviceId','meteringType'
                                                                ,'variableId','readingLocalTime','logNumber')
                df_final = union_unique.join(df_duplicates, [(union_unique.servicePointId == df_duplicates.servicePointId)
                                                        , (union_unique.deviceId == df_duplicates.deviceId)
                                                        , (union_unique.meteringType == df_duplicates.meteringType)
                                                        , (union_unique.variableId == df_duplicates.variableId)
                                                        , (union_unique.readingLocalTime == df_duplicates.readingLocalTime)
                                                        , (union_unique.logNumber.eqNullSafe(df_duplicates.logNumber))]
                                                        ,how = 'left_anti').coalesce(1)
                # con esta linea salvamos la union entre nulos de pyspark, (union_unique.logNumber.eqNullSafe(df_duplicates.logNumber))

                df_logs_aux = df_union.subtract(union_unique).withColumn("Descripcion_log",lit("Valores inconsitentes")).coalesce(1)
                df_logs = df_logs.union(df_logs_aux).coalesce(1)

                df_union = df_final

                df_union.write.format('csv').mode("overwrite").save("./output/cleaned/paso3", header="true", emptyValue="")

        




        # PARTE 4: VERSIONADO


        # creo las columnas de editado y estimado con valores vacios
        df_union = df_union.withColumn("estimationReading", lit(""))\
                        .withColumn("estimationValid", lit(""))\
                        .withColumn("editionReading", lit(""))\
                        .withColumn("editionValid", lit(""))

        # verificar si el df tiene datos de REGISTERS Y LOAD PROFILE READING
        if check_empty(df_union.filter(df_union.readingType == "LOAD PROFILE READING")):
        

                df_nuevo = df_union.filter((df_union.readingType == "REGISTERS") | (df_union.readingType == "LOAD PROFILE READING"))

                max_ts = df_nuevo.agg(max('readingUtcLocalTime')).collect()[0][0]
                min_ts = df_nuevo.agg(min('readingUtcLocalTime')).collect()[0][0]

                # pongo todos los valores de version en 1, como si fueran nuevos, despues se van modificando
                df_nuevo = df_nuevo.withColumn("version",lit(1))



                # este es el df que esta presente en trustdata (base de datos), hacer una consulta que traiga datos con readinType REGISTERS O
                # LOAD PROFILE READING y ademas que tenga una fecha comprendida entre max_ts y min_ts
                df_orig = spark.read.csv(
                "../tests/Examples/original_modificado_glue.csv", header=True, mode="DROPMALFORMED"
                )




                # ATHENA






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


                df_union.write.format('csv').mode("overwrite").save("./output/cleaned/paso4", header="true", emptyValue="")


        # PARTE 5: RELLENO DE CEROS Y NULOS

        if check_empty(df_union.filter(df_union.readingType == "LOAD PROFILE READING")):
                # Se trabaja solo para los datos de LOAD PROFILE READING
                df_load_profile = df_union.filter(df_union.readingType == "LOAD PROFILE READING")
                # Crear columna quality_flag
                df_load_profile = df_load_profile.withColumn("quality_flag", concat(col("qualityCodesSystemId"),lit("-"),col("qualityCodesCategorization"),lit("-"),col("qualityCodesIndex")))

                variables_id = df_load_profile.select("variableId").distinct().collect()
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

                        df_lp = df_load_profile.filter(df_load_profile.variableId == variable)

                        # obtengo los limites de los tiempos y los paso a timestamp
                        max_ts = datetime.strptime((df_lp.agg(max('readingUtcLocalTime')).collect()[0][0]),"%Y-%m-%d %H:%M:%S")
                        min_ts = datetime.strptime((df_lp.agg(min('readingUtcLocalTime')).collect()[0][0]),"%Y-%m-%d %H:%M:%S")
                        delta = max_ts - min_ts


                        # interval indica el intervalo en minutos
                        interval = df_lp.select("intervalSize").first()[0]
                        mins = delta.seconds//60
                        lista = [min_ts]
                        for rep in range(1,mins//interval):
                                lista.append(min_ts + timedelta(minutes = rep*interval))
                        if min_ts != max_ts:
                                lista.append(max_ts)

                        all_elem = [fecha.strftime('%Y-%m-%d %H:%M:%S') for fecha in lista]
                        all_elem = ",".join(all_elem)

                        elem = df_lp.select("readingUtcLocalTime").orderBy("readingUtcLocalTime",ascending=True).collect()
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
                        df_portion = df_lp.select(col("readingUtcLocalTime").alias("readingUtcLocalTime_ref")
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
                        df_lp = df_lp.join(df_timestamps, [(df_lp.readingUtcLocalTime == df_timestamps.readingUtcLocalTime_2)]
                                                                ,how = 'inner').coalesce(1)
                        df_lp = df_lp.drop("readingUtcLocalTime_2")

                        # explode de los arrays de la columna 
                        df_lp = df_lp.withColumn("complete_interval", explode("timestamps_arrays"))
                        df_lp = df_lp.drop("timestamps_arrays")


                        # hago join del df original con el df de referencia, para asi tener los valores consistentes de los campos valor y quality flags
                        df_lp = df_lp.join(df_reference, [(df_lp.complete_interval == df_reference.complete_interval_ref)]
                                                                ,how = 'inner').coalesce(1)


                        # hago el rellenado con nulos y ceros
                        # veo si rellanar con ceros o con nulos
                        
                        if ceros:
                                relleno = "0"
                        else:
                                relleno = None


                        # Create your UDF object (which accepts func_apagon  python function)
                        udf_object = udf(func_apagon, StringType())
                        df_lp = df_lp.withColumn("readingsValue_ref", udf_object(struct([df_lp[x] for x in df_lp.columns])))


                        # Create your UDF object (which accepts func_apagon_intervalos python function)
                        udf_object = udf(func_apagon_intervalos, StringType())
                        df_lp = df_lp.withColumn("validacion_intervalos", udf_object(struct([df_lp[x] for x in df_lp.columns])))


                        # Elimino algunas columnas y renombro otras para mantener el formato de salida esperado

                        columns_to_drop = ['readingUtcLocalTime_ref', 'complete_interval_ref', 'readingUtcLocalTime', 'readingDateSource',
                                                'readingLocalTime', "qualityCodesSystemId" ,"qualityCodesCategorization", 'qualityCodesIndex',
                                                "readingsValue", "referencia" , "quality_flag"]
                        df_lp = df_lp.drop(*columns_to_drop)


                        if iteracion == 0:
                                df_load_profile_final = df_lp
                        else:
                                df_load_profile_final = df_load_profile.union(df_lp).coalesce(1)


                df_load_profile = df_load_profile_final.withColumnRenamed("complete_interval","readingUtcLocalTime").withColumnRenamed("readingLocalTime_ref","readingLocalTime") .withColumnRenamed("readingDateSource_ref","readingDateSource").withColumnRenamed("qualityCodesSystemId_ref","qualityCodesSystemId").withColumnRenamed("qualityCodesCategorization_ref","qualityCodesCategorization").withColumnRenamed("qualityCodesIndex_ref","qualityCodesIndex").withColumnRenamed("readingsValue_ref","readingsValue")

                # selecciona las columnas en el orden que van y ademas hacer el union con el df original filtrado por
                # los otros tipos de lecturas

                df_registers_events = df_union.filter((df_union.readingType == "REGISTERS") | (df_union.readingType == "EVENTS"))
                df_registers_events = df_registers_events.withColumn("validacion_intervalos",lit(""))

                lista_columnas = ["servicePointId","readingType","variableId","deviceId","meteringType","readingUtcLocalTime","readingDateSource","readingLocalTime","dstStatus"
                ,"channel","unitOfMeasure","qualityCodesSystemId","qualityCodesCategorization","qualityCodesIndex","intervalSize","logNumber"
                ,"version","readingsValue","primarySource","readingsSource","owner","guidFile","estatus"
                ,"registersNumber","eventsCode","agentId","agentDescription","multiplier","deviceMaster","deviceDescription","deviceStatus"
                ,"serial","accountNumber","servicePointTimeZone","connectionType","relationStartDate","relationEndDate"
                ,"deviceType","brand","model","validacion_intervalos"]

                df_load_profile = df_load_profile.select(*lista_columnas)
                df_registers_events = df_registers_events.select(*lista_columnas)

                df_union = df_load_profile.union(df_registers_events).coalesce(1)

                df_union.write.format('csv').mode("overwrite").save("./output/cleaned/paso5", header="true", emptyValue="")
        


        # PARTE 6: VALIDACION DE INTERVALOS

        if check_empty(df_union.filter(df_union.readingType == "LOAD PROFILE READING")):
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

                df_union.write.format('csv').mode("overwrite").save("./output/cleaned/paso6", header="true", emptyValue="")

        # escribir csv
        df_union.write.format('csv').mode("overwrite").save("./output/cleaned/final", header="true", emptyValue="")

        return df_union
