# Main thing is to use the DataframeReader to read csv files into spark for computations
# second thing is to use DataFrameWriters to write csv files from spark into other formats.
import os
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = (SparkSession
       .builder
       .appName("Example-Reading Large CSV File")
       .getOrCreate())
# ?Create a DataFrame using the schema defined above
sf_fire_datasets = "/Users/admin/Desktop/LearningSparkV2/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"

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

""" 
    * Common data operation is to explore and transform your data, and PERSIST it into the PARQUET format, 
    * or save it as a SQL table.
    * Explore > Transform > Save as SQL table. 
    
    Persisting a dataframe is as easy as reading it. For example you can persist a dat we were working with
    as a file after reading it you would do the following.
    
"""
if __name__ == "__main__":
    print(fire_df.schema)
    fire_df.show(5 , truncate = False)
    
    # What are the differences between the where() method and the filter() methods?
    fire_df.select('CallType') \
            .where(col('CallType').isNotNull()) \
            .agg(countDistinct("CallType")).show() 
            
    # Getting distinct values of CallType column
    fire_df.select('CallType') \
        .where(col('CallType').isNotNull()) \
        .distinct().show(10, truncate = False)
        
    """
    ?RENAMING AND DROPPING COLUMNS
        *1.) Renaming adding and dropping columns with .withColumnRenamed() method
        *2.) You can rename all columns during creation of the dataframe, by specifyinh the names in StructField (Programmatically)
    """
    newfire_df = fire_df.withColumnRenamed('IncidentNumber', 'IncidentNo')
    
    newfire_df.select("IncidentNo", "Delay") \
        .where(col("Delay") > 5) \
        .show()
        
    # ! Note : Dataframes are Immutable, when we renamed a column we get a new dataframe, while retaining the old fire_df df.
    
    """ 
    ?CONVERT STRINGS INTO UNIX_TIMESTAMPS, OR DATES.
        Use the spark.sql.functions package. It has alot of to/from date/time-stamp functions 
        i.e. to_timestamp() , to_date()
        
        *.withColumn(NewName, spark.sql.function(<column>)) method: to create a new column
        *.drop(<column>): to drop a column in the existing spark dataframe.
        
        Convert existing data to a new column with converted datatype. then drop the old column. It makes sense
        ,as you are not going to replace the data type of a column with millions of rows and risk data error.
        
    """
    fire_ts_df = (newfire_df
                  .withColumn("IncidentDate", to_date(col("CallDate"),"MM/dd/yyyy"))
                  .drop("CallDate")
                  .withColumn("OnWatchDate", to_timestamp(col("WatchDate"),"MM/dd/yyyy")) # Without providing hh;mm;ss, by default it will need 00:00:00 for to_timestamp
                  .drop("WatchDate")
                  .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),"MM/dd/yyyy hh:mm:ss a"))
                  .drop("AvailableDtTm")
                  )
    
    
    
    """

    """
    fire_ts_df.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show()
    
    """ Find out how many calls were logged in the last 7 days? 
        How many years worth of data are in this dataset? 
    """
    (fire_ts_df.select("IncidentDate")
     .orderBy("IncidentDate", ascending = False)
     .show()
     )
    
    