from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


sc =SparkSession.builder.getOrCreate()
rdd1=sc.sparkContext.parallelize([1,2,3])
print(rdd1.collect())  #parallelize method to collect the data through list.

print("number of partitions are: "+str(rdd1.getNumPartitions()))  # Number of partitions
rdd2=rdd1.repartition(2)  # repartition of cores

print("After repartition, number of partitions are: ",str(rdd2.getNumPartitions()))
print(rdd1.collect())
