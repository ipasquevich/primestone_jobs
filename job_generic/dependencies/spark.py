"""
spark.py
~~~~~~~~

Module containing helper function for use with Apache Spark
"""

import __main__

from pyspark.sql import SparkSession
from pyspark import SparkContext


def start_spark(app_name='my_spark_app', config='localhost', jar_packages=[],
                files=[], spark_config={}):
    """Start Spark session.

    Start a Spark session on the worker node.
    """

    spark = SparkSession.builder \
        .appName(app_name).config("localhost") \
        .getOrCreate()

    sc = SparkContext.getOrCreate()


    return spark, sc
