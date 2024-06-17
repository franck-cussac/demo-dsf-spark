from test.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.spark_clean_job import filter_on_age, add_department, clean_data
from pyspark.sql import Row


class TestSparkClean(unittest.TestCase):

    def test_keep_adult(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(age=-1),
                Row(age=0),
                Row(age=1),
                Row(age=17),
                Row(age=18),
                Row(age=19)
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(age=18),
                Row(age=19)
            ]
        )

        # WHEN
        actual = filter_on_age(input, 'age')


        # THEN
        self.assertCountEqual(actual.collect(), expected.collect())
        self.assertEqual(actual.schema, expected.schema)

    def test_add_department(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(zip=19999),
                Row(zip=20000),
                Row(zip=20190),
                Row(zip=20191),
                Row(zip=29999),
                Row(zip=30000)
            ]
        )

        expected = spark.createDataFrame(
            [
                Row(zip=19999, department="19"),
                Row(zip=20000, department="2A"),
                Row(zip=20190, department="2A"),
                Row(zip=20191, department="2B"),
                Row(zip=29999, department="29"),
                Row(zip=30000, department="30")
            ]
        )

        # WHEN
        actual = add_department(input)

        # THEN
        self.assertCountEqual(actual.collect(), expected.collect())
        self.assertEqual(actual.schema, expected.schema)
 
    def test_integration(self):
        # GIVEN
        input_client = spark.createDataFrame(
            [
                Row(name="aa", age=10, zip=19999),
                Row(name="a", age=18, zip=19999),
                Row(name="b", age=18, zip=20000),
                Row(name="c", age=18, zip=20190),
                Row(name="d", age=18, zip=20191),
                Row(name="e", age=18, zip=29999),
                Row(name="f", age=18, zip=30000)
            ]
        )
 
        input_city = spark.createDataFrame(
            [
                Row(zip=19999, city="CITY A"),
                Row(zip=20000, city="CITY B"),
                Row(zip=20190, city="CITY C"),
                Row(zip=20191, city="CITY D"),
                Row(zip=29999, city="CITY E"),
                Row(zip=30000, city="CITY F")
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(zip=19999, name="a", age=18, city="CITY A", department="19"),
                Row(zip=20000, name="b", age=18, city="CITY B", department="2A"),
                Row(zip=20190, name="c", age=18, city="CITY C", department="2A"),
                Row(zip=20191, name="d", age=18, city="CITY D", department="2B"),
                Row(zip=29999, name="e", age=18, city="CITY E", department="29"),
                Row(zip=30000, name="f", age=18, city="CITY F", department="30")
            ] 
        )
 
        # WHEN
        actual = clean_data(input_client, input_city)
        # THEN
        actual.show()
        expected.show()
        self.assertCountEqual(actual.collect(), expected.collect())
        self.assertEqual(actual.schema, expected.schema)




        


