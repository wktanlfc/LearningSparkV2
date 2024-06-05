"""
        ?Aggregations and Various other Operations
"""
# Main thing is to use the DataframeReader to read csv files into spark for computations
# second thing is to use DataFrameWriters to write csv files from spark into other formats.
import os
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as f 
spark = (SparkSession
       .builder
       .appName("Aggregations and Computations in Spark Exercise - Chapter 3")
       .getOrCreate())
# ?Create a DataFrame using the schema defined above
sf_fire_datasets = "/Users/admin/Desktop/LearningSparkV2/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"


#StructType Parameter with a List of StructField() methods per column.
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
              StructField('UnitID', StringType(), True),
              StructField('IncidentNumber', IntegerType(), True),
              StructField('CallType', StringType(), True),
              StructField('CallDate', StringType(), True),
              StructField('WatchDate', StringType(), True),
              StructField('CallFinalDisposition', StringType(), True),
              StructField('AvailableDtTm', StringType(), True),
              StructField('Address', StringType(), True),
              StructField('City', StringType(), True),
              StructField('Zipcode', IntegerType(), True),
              StructField('Battalion', StringType(), True),
              StructField('StationArea', StringType(), True),
              StructField('Box', StringType(), True),
              StructField('OriginalPriority', StringType(), True),
              StructField('Priority', StringType(), True),
              StructField('FinalPriority', IntegerType(), True),
              StructField('ALSUnit', BooleanType(), True),
              StructField('CallTypeGroup', StringType(), True),
              StructField('NumAlarms', IntegerType(), True),
              StructField('UnitType', StringType(), True),
              StructField('UnitSequenceInCallDispatch', IntegerType(), True),
              StructField('FirePreventionDistrict', StringType(), True),
              StructField('SupervisorDistrict', StringType(), True),
              StructField('Neighborhood', StringType(), True),
              StructField('Location', StringType(), True),
              StructField('RowID', StringType(), True),
              StructField('Delay', FloatType(), True)])

# Using the DataFrameReader Interface to read the CSV file into Spark.
fire_df = spark.read.csv(sf_fire_datasets, header = True, schema = fire_schema)


if __name__ == "__main__":
    #?SELECT
    fire_df.select("CallNumber", "UnitID", "IncidentNumber").show(5, truncate = False )

    #?GROUP BY, ORDER BY, SELECT, WHERE: Count the number of rows that are in each unique call type.

    (fire_df.select("CallType")
            .where(col("CallType").isNotNull()) #*Is this step needed, does spark innately possess the ability to remove rows with null values?
            .groupBy("CallType")
            .count()
            .orderBy('count', ascending = False)
            .show(truncate = False)
            )

"""
        *The DataFrame API provides collect() method, but this is heavily computationally intensive, and dangerous for large datasets.
        *as it might cause OOM errors.
        
        *count() : computes, and returns a single number to the driver.
        !collect() : returns a collection of ALL row objects in the entire dataframe or dataset.
        *take(n) : returns the first n rows objects of the dataframe.
        
        ?! Recall what does OOM errors come from? Look at notion notes for spark optimisations.
        ?! Recall what does ROWs in Dataframes represent?
"""


#! This example doesnt work. Spark doesnt follow the conventional order of execution like SQL.
"""  
     * (fire_df.select("CallType", f.sum("NumAlarms"), f.avg("Delay"), f.min("Delay"))
     *        .groupBy("CallType") Adding this line, the code doesnt run...
     *        ).show(truncate = False)
     
""" 
#* Correct way. List out all the relevant transactional columns in select(). Use .agg() method with
#* Pyspark SQL functions as parameters.

(fire_df.select("CallType", "NumAlarms", "Delay")
        .groupBy("CallType")
        .agg(f.sum("NumAlarms").alias("sum_alarms"), f.avg("Delay").alias("average_delays"), f.min("Delay").alias("min_delays"))
        .orderBy("sum_alarms", ascending = True) #?You can also use the alias stated in the statement before as your statements
        .show(truncate = False)
)

#! Come back to the final part of Chapter 3 on Datasets(Java and Scala).
#! The rest of the chapter on execution plan can be on Notion.

