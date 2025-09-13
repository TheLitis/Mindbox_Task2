from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

def get_product_category_pairs(products: DataFrame, categories: DataFrame, product_categories: DataFrame) -> DataFrame:
    """
    Возвращает DataFrame с парами "Имя продукта - Имя категории"
    и отдельные продукты без категорий.
    
    :param products: DataFrame с колонками ["product_id", "product_name"]
    :param categories: DataFrame с колонками ["category_id", "category_name"]
    :param product_categories: DataFrame с колонками ["product_id", "category_id"]
    :return: DataFrame с колонками ["product_name", "category_name"]
    """
    
    # Соединяем продукты с их категориями
    joined_df = products.join(product_categories, on="product_id", how="left") \
                        .join(categories, on="category_id", how="left") \
                        .select("product_name", "category_name")
    
    # Отдельные продукты без категорий (category_name == null)
    no_category_df = joined_df.filter(col("category_name").isNull())
    
    return joined_df
