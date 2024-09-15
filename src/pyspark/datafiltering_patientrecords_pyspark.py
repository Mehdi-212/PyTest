from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def fonction_4_spark(spark):
    df = spark.createDataFrame([
        (1, 34, 'Cardiology'),
        (2, 45, 'Neurology'),
        (3, 50, 'Orthopedics'),
        (4, 20, 'Cardiology'),
        (5, 15, 'Neurology')
    ], ['patient_id', 'age', 'department'])

    filtered_df = df.filter(F.col("age") > 30)

    return filtered_df
