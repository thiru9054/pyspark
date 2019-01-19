import os
from pyspark.sql import SparkSession
spark_output_dir="../output/spark_custom_delim/"


def createSparkSess():
    return SparkSession.builder.master("local").appName("Word Count").getOrCreate()

if __name__=="__main__":
    #create Spark Session
    spark=createSparkSess()
    #read a csv file and get dataframe
    emp_df=spark.read.format('csv').option('header',True).option('inferSchema','True').load("../input/emp.csv")
    #create file with upper case cedilla delimiter
    #coalesce(1) creates one parttion ie one partfile while using bigger files donot use coalesce(1)
    emp_df.coalesce(1).write.mode('OverWrite').option('delimiter','\u00C7').csv(spark_output_dir)
    #rename partfile

    for filename in os.listdir(spark_output_dir):
        if(filename.endswith("csv")):
            print(filename)
            os.rename(spark_output_dir + filename, spark_output_dir + "emp_with_cidella delim.txt")
        else:
            #cleanup the hidden files created by spark such us .success etc
            os.remove(spark_output_dir + filename)



