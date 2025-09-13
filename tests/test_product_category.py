import unittest
from pyspark.sql import SparkSession
from src.product_category import get_product_category_pairs

class TestProductCategoryPairs(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("ProductCategoryTest") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_pairs_and_no_category(self):
        products = self.spark.createDataFrame([
            (1, "Apple"),
            (2, "Banana"),
            (3, "Carrot")
        ], ["product_id", "product_name"])
        
        categories = self.spark.createDataFrame([
            (1, "Fruit"),
            (2, "Vegetable")
        ], ["category_id", "category_name"])
        
        product_categories = self.spark.createDataFrame([
            (1, 1),  # Apple -> Fruit
            (2, 1)   # Banana -> Fruit
        ], ["product_id", "category_id"])
        
        result_df = get_product_category_pairs(products, categories, product_categories)
        result = [tuple(row) for row in result_df.collect()]
        
        expected = [
            ("Apple", "Fruit"),
            ("Banana", "Fruit"),
            ("Carrot", None)
        ]
        
        self.assertCountEqual(result, expected)

if __name__ == "__main__":
    unittest.main()
