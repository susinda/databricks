# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Define schema for the DataFrame
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True)
])

# Hardcoded data
data = [
    ("John Doe", 30, 5000.0),
    ("Jane Smith", None, 6000.0),
    ("Mike Johnson", 35, 5500.0),
    ("Emily Brown", 25, 4800.0),
    ("David Lee", None, 5200.0),
    ("Sarah Johnson", 32, 6200.0),
    ("Michael Smith", 28, 4500.0),
    ("Jessica Brown", None, 5100.0),
    ("Daniel Johnson", 40, 5800.0),
    ("Olivia Davis", 27, 4700.0)
]

# Create a DataFrame
df = spark.createDataFrame(data, schema)

# Write DataFrame to Delta Lake (overwrite mode for simplicity)
delta_path = "/test"  # Replace with your Delta Lake path
df.write.format("delta").mode("overwrite").save(delta_path)

# COMMAND ----------

# Register the Delta Lake table as a temporary view
df.createOrReplaceTempView("my_table")

# Create an external table in Hive using the Delta Lake path
spark.sql("CREATE TABLE my_external_table USING DELTA LOCATION '/test'")

# Now you can query the table using the Hive Metastore catalog
spark.sql("SELECT * FROM my_external_table").show()

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Custom Metastore Example") \
    .config("spark.sql.catalogImplementation", "hive") \  # Optional: Use 'hive' for Hive compatible metastore
    .enableHiveSupport() \
    .getOrCreate()

# Create a database in your custom metastore
spark.sql("CREATE DATABASE IF NOT EXISTS my_metastore")

# Use the database
spark.sql("USE my_metastore")

# Create a table (using Delta Lake format as an example)
spark.sql("""
    CREATE TABLE IF NOT EXISTS my_metastore.my_table (
        id INT,
        name STRING,
        age INT
    )
    USING delta
""")

# Insert data into the table
data = [(1, "John", 30), (2, "Jane", 28)]
df = spark.createDataFrame(data, ["id", "name", "age"])
df.write.format("delta").mode("overwrite").saveAsTable("my_metastore.my_table")

# Query the table
spark.sql("SELECT * FROM my_metastore.my_table").show()

# Stop SparkSession
spark.stop()

