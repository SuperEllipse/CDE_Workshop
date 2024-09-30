from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

input_path="s3a://go01-demo/user/vishr/airport-codes-na.txt"
df = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", "\t")
    .load(input_path))

df.show(n=5, truncate=False)
count_airport_df = (df.select("State",  "City")
#                    .where(col("city").isNotNull())
                .groupBy("State", "City")
                .count()
                .orderBy("count", ascending=False))

# show all the resulting aggregation for all the dates and colors
count_airport_df.show(n=60, truncate=False)
print("Total Rows = %d" % count_airport_df.count())