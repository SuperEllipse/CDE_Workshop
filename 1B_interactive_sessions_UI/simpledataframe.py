from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
import numpy as np

schema = StructType([
   StructField("RollNo", IntegerType(), False),
   StructField("First", StringType(), False),
   StructField("Last", StringType(), False),
   StructField("Class", IntegerType(), False),
   StructField("Marks", IntegerType(), False)])

data = [[1, "A", "Rajagopalan", 3, 22], 
    [2, "B", "John", 5, 33],
    [3, "C", "Verse", 10, 44],
    [4, "D", "Example", 4, 55]
    ]

class_df = spark.createDataFrame(data, schema)

class_df.withColumn("Primary Class", (expr("Class <=4"))).show()

class_df.show()