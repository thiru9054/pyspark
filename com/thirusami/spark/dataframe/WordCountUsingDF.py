import re
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import split


def createSparkSess():
    return SparkSession.builder.master("local").appName("Word Count").getOrCreate()

if __name__=="__main__":
    #create Spark Session
    spark=createSparkSess()
    wc_df=spark.read.option('header','false').option('inferSchema','false').csv("../input/sample.txt")
    wc_df_col=wc_df.withColumnRenamed('_c0','lines')
    wc_df_w=wc_df_col.select(split(wc_df_col['lines'],'\s+').alias('words'))
    word_count=wc_df_w.rdd.flatMap(lambda x:x.words).countByValue()
    for k in word_count:
        print("{} = {}".format(k, word_count[k]))




