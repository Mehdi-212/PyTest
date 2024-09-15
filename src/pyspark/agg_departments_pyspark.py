from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#spark = SparkSession.builder.appName("example").getOrCreate()

def fonction_1_spark(spark):
    df = spark.createDataFrame([
        (1, 34, 'Cardiology', 10),
        (2, 45, 'Neurology', 12),
        (3, 23, 'Cardiology', 5),
        (4, 64, 'Orthopedics', 8),
        (5, 52, 'Cardiology', 9)
    ], ['patient_id', 'age', 'department', 'visit_count'])

    agg_df = df.groupBy("department").agg(
        F.sum("visit_count").alias("sum_visit_count"),
        F.mean("age").alias("mean_age"),
        F.max("age").alias("max_age")
    )

    return agg_df
