import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time, datetime
import re
import os
import numpy as np
from config_parser import get_config
from plot_features import visualize_features
def create_spark_session():
    appName = "Smart Building"
    master = "local"
    # download postgresql-42.2.18.jar and place it in /opt to avoid errors
    # https://search.maven.org/artifact/org.postgresql/postgresql/42.2.18/jar
    spark = SparkSession.builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/opt/postgresql-42.2.18.jar")\
    .getOrCreate()
    return spark

def create_psql(spark,config):
    psql = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://"+config.db_host+":"+ str(config.db_port)+"/"+config.db_name)\
    .option("dbtable", config.db_table) \
    .option("user", config.db_user) \
    .option("password", config.db_pass) \
    .option("driver", "org.postgresql.Driver") \
    .load()
    return psql

if __name__ == "__main__":
    building_devices = "object_list/results.bak/bacnet_objects_501.csv"
    most_common_features = ["Zone Temperature","Actual Cooling Setpoint","Cooling Command",\
        "Actual Heating Setpoint","Common Setpoint","Heating Valve Command","Occupancy Status"]
    global_var = {}
    config = get_config(global_var,configfile="config.ini")
    
    spark = create_spark_session()
    psql = create_psql(spark,config)

    psql.printSchema()

    df = pd.read_csv(building_devices)
    for i,name in enumerate(df.jci_name):
        if name is np.nan:
            continue
        match = re.search('[0-9]{3,5}', name)
        if match is not None:
            room_no = name[match.start():match.end()]
            if df.description[i] == most_common_features[0]:
                # visualize_features(df.uuid[i],psql)
                spark.sql("select* from brick_data WHEN uuid::text='urn:uuid:5f0b1216-d9ae-4138-b5f0-2f1f58570a36' limit 10")
                c

