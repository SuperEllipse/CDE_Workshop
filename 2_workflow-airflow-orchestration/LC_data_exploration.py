from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
## Launching Spark Session

spark = SparkSession\
    .builder\
    .appName("DataExploration")\
    .config("spark.hadoop.fs.s3a.s3guard.ddb.region","us-east-1")\
    .config("spark.yarn.access.hadoopFileSystems","s3a://go01-demo/")\
    .getOrCreate()


    
    
print(spark.conf.get("spark.yarn.access.hadoopFileSystems"))


## Creating Spark Dataframe from raw CSV datagov

df = spark.read.option('inferschema','true').csv(
  "s3a://go01-demo/datalake/cde-demo/LoanStats_2015_subset_071821.csv",
  header=True,
  sep=',',
  nullValue='NA'
)

print(f"Argument passed to this Spark Job is : {sys.argv}")

## Printing number of rows and columns:
print('Dataframe Shape')
print((df.count(), len(df.columns)))

## Showing Different Loan Status Values
df.select("loan_status").distinct().show()

## Types of Loan Status Aggregated by Count

print(df.groupBy('loan_status').count().show())

