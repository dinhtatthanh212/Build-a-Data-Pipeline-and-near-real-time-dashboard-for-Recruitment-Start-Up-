# import findspark
# findspark.init()

#import necessary libraries

import os
import time
import datetime
import pyspark.sql.functions as sf
from uuid import *
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import when
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from pyspark import SparkConf, SparkContext
from uuid import * 
from uuid import UUID
import time_uuid 
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.window import Window as W

spark = SparkSession.builder.config("spark.jars.packages",'com.datastax.spark:spark-cassandra-connector_2.12:3.1.0').getOrCreate()
spark = SparkSession.builder.config("spark.jars.packages", "com.mysql:mysql-connector-java_8.0.33").getOrCreate()

#read data from cassandra

#data = spark.read.format("org.apache.spark.sql.cassandra").options(table = "tracking",keyspace = "study_de").load()

#prepraring_data

def prepraring_data(data):

    df = data.select("create_time","custom_track","bid","job_id","campaign_id","group_id","publisher_id")

    df = df.filter(df.job_id.isNotNull() & df.custom_track.isNotNull())

    create_time_list = df.select("create_time").collect()

    create_time = []

    for i in range(len(create_time_list)):
        create_time.append(create_time_list[i][0])
    
    timestamp_time = []

    #########Using lambda for faster speed compare to using for loop
    # 1.Select the 'create_time' column: Extract the 'create_time' column from the DataFrame df using df.select('create_time').

    # 2.Convert to RDD: Transform the column into an RDD (Resilient Distributed Dataset) using .rdd. This allows for efficient distributed processing.
    # 3.Process within lambda:
    # Create a TimeUUID object from the UUID bytes in x[0].
    # Extract the datetime object using .get_datetime().
    # Format the datetime as a string using .strftime('%Y-%m-%d %H:%M:%S').
    # Return the formatted string.
    
    timestamp_time = df.select('create_time')\
        .rdd\
            .map(lambda x: time_uuid.TimeUUID(bytes=UUID(x[0]).bytes)\
                .get_datetime()\
                    .strftime('%Y-%m-%d %H:%M:%S'))\
                        .collect()
    
    time_data  = spark.createDataFrame(zip(create_time,timestamp_time), schema=["create_time","ts"]) 

    data_result = time_data.join(df, on='create_time',how='inner')

    data_result = data_result.na.fill({'bid':0})
    data_result = data_result.na.fill({'job_id':0})
    data_result = data_result.na.fill({'publisher_id':0})
    data_result = data_result.na.fill({'group_id':0})
    data_result = data_result.na.fill({'campaign_id':0})

    return data_result

def clicks(data_result):

    clicks_data = data_result.filter(data_result.custom_track == 'click')

    clicks_data.registerTempTable('Clicks')

    click_outputs = spark.sql(""" select job_id,date(ts) as Date , hour(ts) as Hour , campaign_id, publisher_id,  group_id,
                                            avg(bid) as bid_set , count(*) as clicks , sum(bid) as spend_hour 
                                from clicks
                                group by job_id,date(ts), hour(ts), publisher_id , campaign_id , group_id""")
    
    return click_outputs

def conversion(data_result):

    conversion_data = data_result.filter(data_result.custom_track == 'conversion')

    conversion_data.registerTempTable('conversion')

    conversion_outputs = spark.sql("select job_id, date(ts) as date,hour(ts) as hour,campaign_id, publisher_id,  group_id, count(*) as conversion \
                    from conversion \
                    group by job_id, date(ts),hour(ts), publisher_id, campaign_id, group_id\
                    ")
    
    return conversion_outputs

def qualified(data_result):

    qualified_data = data_result.filter(data_result.custom_track == 'qualified')

    qualified_data.registerTempTable('qualified')

    qualified_outputs = spark.sql("select job_id, date(ts) as date,hour(ts) as hour,campaign_id, publisher_id,  group_id, count(*) as qualified \
                    from qualified \
                    group by job_id, date(ts),hour(ts), publisher_id, campaign_id, group_id\
                    ")

    return qualified_outputs

def unqualified(data_result):

    unqualified_data = data_result.filter(data_result.custom_track == 'unqualified')

    unqualified_data.registerTempTable('unqualified')

    unqualified_outputs = spark.sql("select job_id, date(ts) as date,hour(ts) as hour,campaign_id, publisher_id,  group_id, count(*) as unqualified \
                    from unqualified \
                    group by job_id, date(ts),hour(ts), publisher_id, campaign_id, group_id\
                    ")
    return unqualified_outputs

def merge_data(click_outputs,conversion_outputs,qualified_outputs, unqualified_outputs):

    final_data = click_outputs.join(conversion_outputs,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full').\
                                join(qualified_outputs,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full').\
                                join(unqualified_outputs,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full')
    
    return final_data

def map_data_cassandra(final_data,url,driver,user,password):
    
    sql = """(SELECT id as job_id, company_id FROM job) test"""

    company = spark.read.format('jdbc').options(url=url, driver=driver, dbtable=sql, user=user, password=password).load()

    result = company.join(final_data, on=["job_id"], how="right")
    output = result.select("job_id","hour","date","unqualified","qualified","conversion",\
                           "company_id","group_id","campaign_id","publisher_id","bid_set","clicks","spend_hour")
    
    final_output = output.withColumnRenamed('date','dates')\
                        .withColumnRenamed('hour','hours').\
                            withColumnRenamed('qualified','qualified_application').\
                                withColumnRenamed('unqualified','disqualified_application')
    
    final_output = final_output.withColumn('sources',lit('Cassandra'))
    
    return final_output

def load_data_to_data_warehouse(final_output):

    final_output.write.format("jdbc") \
    .option("driver","com.mysql.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/data_warehouse") \
    .option("dbtable", "events") \
    .mode("append") \
    .option("user", "root") \
    .option("password", "admin") \
    .save()

    return print('Data is loaded successfully')

def main_task(mysql_time,data):

    host = 'localhost'

    port = '3306'

    db_name = 'data_warehouse'

    user = 'root'

    password = 'admin'

    url = 'jdbc:mysql://' + host + ':' + port + '/' + db_name

    driver = "com.mysql.jdbc.Driver"

    print('The host is ' ,host)

    print('The port using is ',port)
    
    print('The db using is ',db_name)

    print("___________Extracting data from Datalake (Cassandra)___________")

    data_result = prepraring_data(data)

    print("___________Transforming data___________")

    _clicks = clicks(data_result)

    _conversion = conversion(data_result)

    _qualified = qualified(data_result)

    _unqualified = unqualified(data_result)

    final_data = merge_data(_clicks, _conversion, _qualified, _unqualified)


    final_output = map_data_cassandra(final_data,url,driver,user,password)

    print("___________Loading data to Data warehouse (MySQL)___________")

    load_data_to_data_warehouse(final_output)

    return print('Task Finished')

#Near real-time feature by CDC

#Get latest time data from data lake and data warehouse

def get_latest_time_cassandra(data_in_cassandra):

    cassandra_latest_time = data_in_cassandra.agg({'ts':'max'}).take(1)[0][0]

    return cassandra_latest_time

def get_mysql_latest_time(url,driver,user,password):    

    sql = """(select max(latest_update_time) from events) data"""

    mysql_time = spark.read.format('jdbc').options(url=url, driver=driver, dbtable=sql, user=user, password=password).load()

    mysql_time = mysql_time.take(1)[0][0]

    if mysql_time is None:

        mysql_latest = '1998-01-01 23:59:59'

    else :

        mysql_latest = mysql_time.strftime('%Y-%m-%d %H:%M:%S')

    return mysql_latest 

host = 'localhost'

port = '3306'

db_name = 'data_warehouse'

user = 'root'

password = 'admin'

url = 'jdbc:mysql://' + host + ':' + port + '/' + db_name

driver = "com.mysql.jdbc.Driver"

# compare time between data lake and data warehouse to find there is any changes?

# run again after every 5 minutes

while True :

    start_time = datetime.datetime.now()
    
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table = "tracking",keyspace = "study_de").load()

    data_in_cassandra = prepraring_data(data)

    cassandra_time = get_latest_time_cassandra(data_in_cassandra)

    print('Cassandra latest time is {}'.format(cassandra_time))
    
    mysql_time = get_mysql_latest_time(url,driver,user,password)

    print('MySQL latest time is {}'.format(mysql_time))

    if cassandra_time > mysql_time : 
        data = spark.read.format("org.apache.spark.sql.cassandra").options(table = "tracking",keyspace = "study_de").load().where(col('ts')> mysql_time)
        main_task(mysql_time,data)

    else :

        print("No new data found")

    end_time = datetime.datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    print('Job takes {} seconds to execute'.format(execution_time))

    time.sleep(30)
    