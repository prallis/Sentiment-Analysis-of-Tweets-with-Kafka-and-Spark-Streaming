from pyspark.sql import SparkSession
from pyspark.sql import Row

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://s01:5432/products_project") \
    .option("driver","org.postgresql.Driver") \
    .option("dbtable", "(select * from shoes ) as shoes") \
    .option("user", "prallis_ds") \
    .option("password", "13131966") \
    .load()

df.printSchema()
df.show(10)