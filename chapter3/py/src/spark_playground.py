import os
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pathlib import Path 
spark = (SparkSession
       .builder
       .appName("Example-Reading Large CSV File")
       .getOrCreate())

curr_path = os.getcwd()
# sys_info = os.environ()
# print(os.environ)
print("************" + curr_path + "************" )

for dirpath,dirnames,file in os.walk(curr_path):
    if "databricks-datasets/learning-spark-v2/sf-airbnb" in dirpath and 'lr-pipeline-model' not in dirpath:
        try: 
            df = spark.read.format("parquet").load(dirpath)
            print(f"path read is from {dirpath}")
        except:
            "File Wrong Error"
            
# final_path = "./databrick_datasets/learning-spark-v2/sf-airbnb/"
# os.chdir(os.path.join(curr_path, final_path))
# print(f"********* This path is : " + {os.getcwd()})
df.printSchema()

df.createOrReplaceTempView("temp_tbl")

spark.sql("""
          select * from temp_tbl limit 100
          """)