from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .getOrCreate()

    sc = spark.sparkContext

    # Load a text file and convert each line to a tuple.
    lines = sc.textFile("hdfs://devenv/user/spark/spark_sql_101/data/people.txt")
    parts = lines.map(lambda l: l.split(","))
    people = parts.map(lambda p: (p[0], int(p[1].strip())))

    # 1 The schema is specified using a StructType object
    schema = "name string, age int"

    # Apply the schema to the RDD.
    schemaPeople = spark.createDataFrame(people, schema)

    schemaPeople.printSchema()

    schemaPeople.show()

    # SQL can be run over DataFrames that have been registered as a table.
    schemaPeople.createOrReplaceTempView("people")
    teenagers = spark.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")

    teenagers.show()
   