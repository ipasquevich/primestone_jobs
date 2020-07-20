from job_translate_amrdef import translator
from  job_enrich import enricher
from  job_clean import cleaner
#from intervals import interval

from dependencies.spark import start_spark


def main():
    
    
    spark, sc = start_spark(
        app_name="PySpark - AMRDEF",
        config='localhost')
    
    # job-translate-amrdef
    data = translator("file:////home/ivan/Documents/Primestone/Esquemas/AMRDEF_sample_modif_flags_actualdates.xml",spark)
    print("\n"*10,"data translation done","\n"*10)
    
    # job-enrich
    data = enricher(data,spark)
    print("\n"*10,"data enrichment done","\n"*10)

    # job-clean
    data = cleaner(data,spark)
    print("\n"*10,"data cleaning done","\n"*10)


if __name__ == '__main__':
    main()


#spark-submit --jars='/home/ivan/Documents/Primestone/Scripts/spark-xml_2.11-0.4.1.jar' --py-files='../packages.zip'  main.py

