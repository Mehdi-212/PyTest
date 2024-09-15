from pyspark.sql import functions as F

def fonction_2_spark(spark):
    df = spark.createDataFrame([
        ('John Doe', 'Diabetes'),
        ('Jane Smith', 'Heart Disease'),
        ('Alice Brown', 'Hypertension')
    ], ['patient_name', 'diagnosis'])

    df = df.withColumn("diagnosis_lower", F.lower(F.col("diagnosis")))
    df = df.withColumn("full_info", F.concat_ws(" - ", F.col("patient_name"), F.col("diagnosis_lower")))

    return df
