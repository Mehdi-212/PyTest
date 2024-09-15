from pyspark.sql import functions as F

def fonction_3_spark(spark):
    df = spark.createDataFrame([
        (1, 34, 'Cardiology'),
        (2, 70, 'Neurology'),
        (3, 50, 'Orthopedics'),
        (4, 20, 'Cardiology'),
        (5, 15, 'Neurology')
    ], ['patient_id', 'age', 'department'])

    df = df.withColumn("age_category", F.when(F.col("age") > 60, "senior")
                                   .when(F.col("age") > 18, "adult")
                                   .otherwise("minor"))

    return df
