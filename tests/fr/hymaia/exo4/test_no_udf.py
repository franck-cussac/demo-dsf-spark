from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo4.no_udf import add_column_total_price_per_category_last_30_days
from pyspark.sql import Row


class TestWindowFunction(unittest.TestCase):

    def test_add_column_total_price_per_category_last_30_days(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(date="2022-12-31", category="5", price="10.0"),
                Row(date="2023-01-01", category="5", price="20.0"),
                Row(date="2023-01-31", category="5", price="40.0"),
                Row(date="2019-02-16", category="6", price="40.0"),
                Row(date="2019-02-17", category="6", price="33.0"),
                Row(date="2019-02-18", category="6", price="70.0"),
                Row(date="2019-02-16", category="4", price="12.0"),
                Row(date="2019-02-17", category="4", price="20.0"),
                Row(date="2019-02-18", category="4", price="25.0"),
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(date="2022-12-31", category="5", price="10.0", total_price_per_category_last_30_days=10.0),
                Row(date="2023-01-01", category="5", price="20.0", total_price_per_category_last_30_days=30.0),
                Row(date="2023-01-31", category="5", price="40.0", total_price_per_category_last_30_days=60.0),
                Row(date="2019-02-16", category="6", price="40.0", total_price_per_category_last_30_days=40.0),
                Row(date="2019-02-17", category="6", price="33.0", total_price_per_category_last_30_days=73.0),
                Row(date="2019-02-18", category="6", price="70.0", total_price_per_category_last_30_days=143.0),
                Row(date="2019-02-16", category="4", price="12.0", total_price_per_category_last_30_days=12.0),
                Row(date="2019-02-17", category="4", price="20.0", total_price_per_category_last_30_days=32.0),
                Row(date="2019-02-18", category="4", price="25.0", total_price_per_category_last_30_days=57.0),
            ]
        )

        # WHEN
        actual = add_column_total_price_per_category_last_30_days(input)

        # THEN
        self.assertCountEqual(actual.collect(), expected.collect())