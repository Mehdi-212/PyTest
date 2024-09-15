"""import pytest
import pandas as pd
from pyspark.sql import SparkSession
import numpy as np
import sys
import os

# Ajoutez le répertoire de votre projet au PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

# Importer les fonctions Pandas et PySpark
from src.pandas.agg_departments import fonction_1_pandas
from src.pyspark.agg_departments_pyspark import fonction_1_spark
from src.pandas.concat_medicalinfo import fonction_2_pandas
from src.pyspark.concat_medicalinfo_pyspark import fonction_2_spark
from src.pandas.conditionalcalculations_medicalage import fonction_3_pandas
from src.pyspark.conditionalcalculations_medicalage_pyspark import fonction_3_spark
from src.pandas.datafiltering_patientrecords import fonction_4_pandas
from src.pyspark.datafiltering_patientrecords_pyspark import fonction_4_spark
from src.pandas.missingvalues_handling import fonction_5_pandas
from src.pyspark.missingvalues_handling_pyspark import fonction_5_spark

# Fixture pour créer la session Spark
@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("test").getOrCreate()

# Test 1 : Vérifier l'agrégation (agg-departments)
def test_fonction_1(spark):
    pandas_df = fonction_1_pandas()
    pyspark_df = fonction_1_spark(spark).toPandas()

    # Comparer les résultats Pandas et PySpark
    pd.testing.assert_frame_equal(pandas_df, pyspark_df)

# Test 2 : Vérifier la concaténation (concat-medicalinfo)
def test_fonction_2(spark):
    pandas_df = fonction_2_pandas()
    pyspark_df = fonction_2_spark(spark).toPandas()

    # Comparer les résultats Pandas et PySpark
    pd.testing.assert_frame_equal(pandas_df[['full_info']], pyspark_df[['full_info']])

# Test 3 : Vérifier les calculs conditionnels (conditionalcalculations-medicalage)
def test_fonction_3(spark):
    pandas_df = fonction_3_pandas()
    pyspark_df = fonction_3_spark(spark).toPandas()

    # Comparer les colonnes age_category
    pd.testing.assert_series_equal(pandas_df['age_category'], pyspark_df['age_category'])

# Test 4 : Vérifier le filtrage (datafiltering-patientrecords)
def test_fonction_4(spark):
    pandas_df = fonction_4_pandas()
    pyspark_df = fonction_4_spark(spark).toPandas()

    # Comparer les DataFrames Pandas et PySpark
    pd.testing.assert_frame_equal(pandas_df, pyspark_df)

# Test 5 : Vérifier le traitement des valeurs manquantes (missingvalues-handling)
def test_fonction_5(spark):
    pandas_df = fonction_5_pandas()
    pyspark_df = fonction_5_spark(spark).toPandas()

    # Comparer les DataFrames Pandas et PySpark
    pd.testing.assert_frame_equal(pandas_df, pyspark_df)

"""

import unittest
import pandas as pd
from pyspark.sql import SparkSession
import numpy as np
import sys
import os

# Ajoutez le répertoire de votre projet au PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

# Importer les fonctions Pandas et PySpark
from src.pandas.agg_departments import fonction_1_pandas
from src.pyspark.agg_departments_pyspark import fonction_1_spark
from src.pandas.concat_medicalinfo import fonction_2_pandas
from src.pyspark.concat_medicalinfo_pyspark import fonction_2_spark
from src.pandas.conditionalcalculations_medicalage import fonction_3_pandas
from src.pyspark.conditionalcalculations_medicalage_pyspark import fonction_3_spark
from src.pandas.datafiltering_patientrecords import fonction_4_pandas
from src.pyspark.datafiltering_patientrecords_pyspark import fonction_4_spark
from src.pandas.missingvalues_handling import fonction_5_pandas
from src.pyspark.missingvalues_handling_pyspark import fonction_5_spark


class TestPandasPySpark(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Créer la session Spark avant l'exécution des tests"""
        cls.spark = SparkSession.builder.master("local").appName("test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Arrêter la session Spark après l'exécution des tests"""
        cls.spark.stop()

    # Test 1 : Vérifier l'agrégation (agg-departments)
    def test_fonction_1(self):
        pandas_df = fonction_1_pandas()
        pyspark_df = fonction_1_spark(self.spark).toPandas()

        # Comparer les résultats Pandas et PySpark
        pd.testing.assert_frame_equal(pandas_df, pyspark_df)

    # Test 2 : Vérifier la concaténation (concat-medicalinfo)
    def test_fonction_2(self):
        pandas_df = fonction_2_pandas()
        pyspark_df = fonction_2_spark(self.spark).toPandas()

        # Comparer les résultats Pandas et PySpark
        pd.testing.assert_frame_equal(pandas_df[['full_info']], pyspark_df[['full_info']])

    # Test 3 : Vérifier les calculs conditionnels (conditionalcalculations-medicalage)
    def test_fonction_3(self):
        pandas_df = fonction_3_pandas()
        pyspark_df = fonction_3_spark(self.spark).toPandas()

        # Comparer les colonnes age_category
        pd.testing.assert_series_equal(pandas_df['age_category'], pyspark_df['age_category'])

    # Test 4 : Vérifier le filtrage (datafiltering-patientrecords)
    def test_fonction_4(self):
        pandas_df = fonction_4_pandas()
        pyspark_df = fonction_4_spark(self.spark).toPandas()

        # Comparer les DataFrames Pandas et PySpark
        pd.testing.assert_frame_equal(pandas_df, pyspark_df)

    # Test 5 : Vérifier le traitement des valeurs manquantes (missingvalues-handling)
    def test_fonction_5(self):
        pandas_df = fonction_5_pandas()
        pyspark_df = fonction_5_spark(self.spark).toPandas()

        # Comparer les DataFrames Pandas et PySpark
        pd.testing.assert_frame_equal(pandas_df, pyspark_df)


if __name__ == '__main__':
    unittest.main()
