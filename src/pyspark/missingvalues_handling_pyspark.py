from pyspark.sql import functions as F

def fonction_5_spark(spark):
    df = spark.createDataFrame([
        (1, 34, 'Cardiology'),
        (2, None, 'Neurology'),
        (3, 50, 'Orthopedics'),
        (4, None, None),
        (5, 15, 'Neurology')
    ], ['patient_id', 'age', 'department'])

    df = df.na.fill({'age': df.select(F.mean('age')).collect()[0][0], 'department': 'Unknown'})

    return df
