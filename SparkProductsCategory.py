from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

spark = SparkSession.builder.appName("ProductCategory").getOrCreate()

products = [
    ("Product1", ["Category1", "Category2"]),
    ("Product2", ["Category2", "Category3"]),
    ("Product3", ["Category1"]),
    ("Product4", []),
]

schema = ["Product", "Categories"]

df = spark.createDataFrame(products, schema)

df = df.select("Product", explode("Categories").alias("Category"))

all_products = df.union(spark.createDataFrame(df.select("Product").distinct().rdd.map(lambda x: (x[0], "No Category")), ["Product", "Category"]))

all_products.show()