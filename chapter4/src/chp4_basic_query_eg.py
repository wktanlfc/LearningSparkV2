import os
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as f 

spark = (SparkSession
       .builder
       .appName("Spark SQL - Chapter 4")
       .getOrCreate())
# ?Create a DataFrame using the schema defined above
departure_delays_datasets = "/Users/admin/Desktop/LearningSparkV2/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

#Read and Create a temporary view.
#Infer schema, note for bigger tables you might want to specify schema
delays_df = (spark.read.format("csv")
             .option("infer.schema", "true")
             .option("header", "true")
             .load(departure_delays_datasets)
             )

# This method creates a view for you to churn out records for your use.
delays_df.createOrReplaceTempView("us_delay_flights_tbl")

spark.sql(
    """
    SELECT distance, origin, destination 
    FROM us_delay_flights_tbl 
    WHERE distance > 1000 
    ORDER BY distance DESC
    """
).show()