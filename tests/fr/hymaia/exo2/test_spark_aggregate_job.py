from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.spark_aggregate_job import agg_pop_by_departement
from pyspark.sql import Row
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType,StructField, StringType, LongType


class TestSparkAggregate(unittest.TestCase):

    def test_input_file_empty(self):
        # GIVEN
        input = spark.createDataFrame([], StructType([]))

        # WHEN/THEN
        with self.assertRaises(AnalysisException):
            agg_pop_by_departement(input)

    def test_integration(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(zip=19999, name="a", age=18, city="CITY A", department="19"),
                Row(zip=19999, name="a", age=18, city="CITY A", department="19"),
                Row(zip=19999, name="a", age=18, city="CITY A", department="19"),
                Row(zip=20000, name="b", age=18, city="CITY B", department="2A"),
                Row(zip=20191, name="d", age=18, city="CITY D", department="2B"),
                Row(zip=30000, name="f", age=18, city="CITY F", department="30")
            ] 
        )

        expected_schema = StructType([
                                        StructField("department", StringType(), True),
                                        StructField("nb_people", LongType(), False)
                                    ])
        expected_data = [("19", 3),                                              
                         ("2A", 1),
                         ("2B", 1),
                         ("30", 1)]                            
        expected = spark.createDataFrame(data=expected_data, schema=expected_schema)
 
        # WHEN
        actual = agg_pop_by_departement(input)
        # THEN

        self.assertEqual(actual.collect(), expected.collect())
        self.assertEqual(actual.schema, expected.schema)




        


