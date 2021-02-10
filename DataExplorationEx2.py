# _*_ coding: utf-8 _*_
"""
Created on Thu Feb 4 19:47:40 2021
@author: PMD14

Before running this code make hardcode change to confPath and xmlPath location.
Reads one xml source file which has been unzipped stackoverflow.com.PostLinks.7z
Using pySpark does some Feature Engineering analysis and then loads to a postgresql database
followed by reading from the database back into dataframe, selects from from dataframe into
spark dataset and finally creates spark table for session(s).
"""
__author__ = "P. Douglas"
__copyright__ = "Copyright (C) 2021 P. Douglas"
__license__ = "Public Domain"
__version__ = "0.1"

import pyspark.sql.functions as f
from pyspark.sql import Window
from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Start a pyspark session
spark.sparkContext._conf.getAll()

# capture the config context and modify as require for the environment to run with
conf = spark.sparkContext._conf.setAll([('spark.master', 'local[*]'), \
        ('spark.app.name', 'Exercise2'), \
        ('spark.sql.shuffle.partitions', 600)] \
        #("spark.sql.execution.arrow.pyspark.enabled", "true") \
        )
# stop the context we have setup the config we need to run with now
spark.sparkContext.stop()

# spark-shell --packages com.databricks:spark-xml_2.12:0.11.0
# spark = SparkSession.builder.appName("MyApp") \
#   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#   .getOrCreate()
# import delta.tables
# .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0") \
# .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \

def Ex2(name):
    """ Sets a relevant context for the spark cluster.
        prints output and performs ETL to postgresql(ORDBMS) database.
        Reads from the ORBMS into spark dataframe,dataset,table(s).
        There are three assertions as processing progresses.
        Refactoring and class OOP refinement for another day.
        Class reuse/design not available, code written from scratch.
    """
    # .format('com.databricks.spark.xml')
    # SparkSession provides immediate access to sparkContext
    # instantiate the session with bespoke configuration (there can be many sessions)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext._conf.getAll()

    # There is only one context that services all the sessions.

    #dirdelta = r'C:\Users\username\Spark\spark-3.0.1-bin-hadoop2.7\delta-table'
    # data = spark.range(0, 5)
    # data.write.format("delta").save(dirdelta)

    # dfdelta = spark.read.format("delta").load("/tmp/delta-table")
    # dfdelta = spark.read.format("delta").load(dirdelta)
    # dfdelta.show()

    custom_schema = StructType([
        StructField("Id", StringType(), True),
        StructField("CreationDate", StringType(), True),
        StructField("PostId", StringType(), True),
        StructField("RelatedPostId", StringType(), True),
        StructField("LinkTypeId", StringType(), True)
    ])

    # databricks library .schema(custom_schema).so used infer default

    df1 = (spark.read \
          .format('com.databricks.spark.xml') \
          .option("rootTag", "postlinks") \
          .option('rowTag', 'row') \
          .option('attributePrefix', '_') \
          .option('mode', 'FAILFAST') \
          .option('samplingRatio', '0.25') \
          .load(xmlPath))
    df1.groupBy("_LinkTypeId").count().show()
    # null values removed represents some transformation cleaning
    df1.na.drop()

    df2 = df1.withColumn('_Id', f.col('_Id').cast(IntegerType())) \
            .withColumn('_CreationDate', f.col('_CreationDate').cast(TimestampType())) \
            .withColumn('_PostId', f.col('_PostId').cast(IntegerType())) \
            .withColumn('_RelatedPostId', f.col('_RelatedPostId').cast(IntegerType())) \
            .withColumn('_LinkTypeId', f.col('_LinkTypeId').cast(IntegerType()))
    df2.printSchema()

    # RI dedupe....
    #w = Window.partitionBy("_Id", '_LinkTypeId', '_PostID', '_RelatedPostID')
    #print('groupby 4 cols count')
    #df2.select("_Id", '_LinkTypeId', '_PostID', '_RelatedPostID', f.count('_CreationDate').over(w).alias('n')).sort('_CreationDate').show()

    #filter(column('count') == 1)

    result = df2.groupBy("_LinkTypeId").count().show()

    # filter for _LinkTypeId1=1 represents some transformation

    df = df2.where('_LinkTypeId!=1')
    #df = df2.where('_LinkTypeId==3')
    expected_result_count = 1054222

    print('linktype !=1')
    df.show()
    # test1
    result_count = df.distinct().count()
    assert (result_count == expected_result_count),f'Error count df.Linktype {result_count} NE {expected_result_count}'
    print(expected_result_count)
    df.printSchema()

    df.agg({'_CreationDate': 'min', '_CreationDate': 'max', '_PostId': 'min'}).show()

    print(f'Completed Analysis for {name}')
    # Save the dataframe to the postgresql database table.
    print('write dataframe to RDBMS table ')
    df.write.jdbc(url=rdbm_url, table='public.postlink', \
                  mode='overwrite', properties=rdbm_properties)

    #df.write.jdbc(url=rdbm_url, table='public.postlink', \
    #             mode='append', properties=rdbm_properties)

    # Read from database postgresql
    jdbcDF = spark.read.format("jdbc"). \
        options( \
        url=rdbm_url, \
        dbtable='postlink', \
        user=rdbm_properties['user'], \
        password=rdbm_properties['password'], \
        driver=rdbm_properties['driver']). \
        load()
    # will return DataFrame
    jdbcDF.show()
    print('jdbcDF schema')
    print(f'jdbcDF: {type(jdbcDF)}')
    jdbcDF.printSchema()

    # test2
    result_count = jdbcDF.distinct().count()
    assert (result_count == expected_result_count), f'Error count jdbcDF {result_count} NE {expected_result_count}'

    print('select with predicate the dataframe into collection spark dataset')
    # collect a spark dataset from the spark dataframe and process
    for rec in jdbcDF.select('*').filter('_PostId < 10050').collect():
        print('jdbcDF:', rec)

    # Register the spark DataFrame with data read from postgres table
    # as a SQL temporary view
    jdbcDF.createOrReplaceTempView('PostLink')

    print('select from the TempView Postlink view')
    sqlDF = spark.sql('SELECT _PostId,_CreationDate FROM PostLink')
    sqlDF.show()

    # test3 e.g. AssertionError: Error count sqlDF 943465 NE 1054222
    result_count = sqlDF.count()  # change to  distinct.count() will error
    assert (result_count == expected_result_count), f'Error count sqlDF {result_count} NE {expected_result_count}'

    # Register the DataFrame as a global temporary view
    jdbcDF.createOrReplaceGlobalTempView('PostLinkGlobalView')

    # Global temporary view is tied to a system preserved database `global_temp`
    print('select from the GlobalTempView PostLinkGlobalView')
    spark.sql("SELECT _CreationDate,_LinkTypeId FROM global_temp.PostLinkGlobalView ORDER BY _Id LIMIT length('0123456789')").show()

    # Global temporary view is cross-session
    print('New Session select from the GlobalTempView PostLinkGlobalView')
    spark.newSession().sql("SELECT * FROM global_temp.PostLinkGlobalView ORDER BY _Id LIMIT length('SPARKQL')").show()

    print('stop context')
    spark.stop()  # stop context

if __name__ == "__main__":
    # change confPath to the absolute path location of the db_properties.ini file.
    confPath = r'C:\Users\username\Spark\spark-3.0.1-bin-hadoop2.7\db_properties.ini'
    xmlPath = r'C:\Users\username\Spark\spark-3.0.1-bin-hadoop2.7\spark_xml\PostLinks.xml'

    import configparser, traceback

    rdbm_properties = {}
    config = configparser.ConfigParser()
    config.read(confPath)
    rdbm_prop = config['postgres']
    rdbm_url = rdbm_prop['url']
    rdbm_properties['user'] = rdbm_prop['user']
    rdbm_properties['password'] = rdbm_prop['password']
    # rdbm_properties['url'] = rdbm_prop['url']
    rdbm_properties['driver'] = rdbm_prop['driver']

    print(rdbm_prop)
    print(rdbm_properties)
    try:
        Ex2('Exercise2')
    except Exception as error:
        traceback.print_exc()
        print('###### An exception occurred ######')
    else:
        print('\ncompleted')
    # See PyCharm help at https://www.jetbrains.com/help/pycharm/