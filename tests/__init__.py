import unittest
from src.sample_pyspark import spark

class SampleTest(unittest.TestCase):
    def test_spark_session(self):
        self.assertIsNotNone(spark)

if __name__ == "__main__":
    unittest.main()