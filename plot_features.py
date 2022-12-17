# import matplotlib.pyplot as plt
from pyspark.sql.functions import *
from pyspark.sql.types import *

def visualize_features(uuid,psql):
    psql.select(col("*"), expr("WHEN uuid='urn:uuid:5f0b1216-d9ae-4138-b5f0-2f1f58570a36' limit 10"))
    