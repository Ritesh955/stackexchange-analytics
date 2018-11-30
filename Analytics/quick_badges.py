from pyspark import SparkConf, SparkContext
import sys,re,math,os,gzip,uuid,datetime
from pyspark.sql.functions import col

#assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row

spark = SparkSession.builder.appName('StackExchange Data Loader').config("spark.executor.memory", "70g").config("spark.driver.memory", "50g").config("spark.memory.offHeap.enabled",'true').config("spark.memory.offHeap.size","16g").getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
sc = spark.sparkContext


def main(users_table,badges_table,badge_name):

    # Read users table

    users = spark.read.parquet(users_table).repartition(96)
    
    users.createOrReplaceTempView('users')

    #Read badges table

    badges = spark.read.parquet(badges_table).repartition(96)

    badges.createOrReplaceTempView('badges')

    #joining users table and badges table on user id and sites
    
    final_badges = spark.sql("select badges.date as datewon , Name ,UserId from badges where badges.Name ='" +badge_name+"'")

    final_badges.createOrReplaceTempView('final_badges')

    #join_users=spark.sql("select users.Id as userlink,users.CreationDate as membersince,users.DisplayName as username\
                            #from badges inner join users on badges.UserId = users.Id and users.sites=badges.sites where badges.Name ='" +badge_name+"'")

    #join_users.createOrReplaceTempView('join_users')

    #join_tables=spark.sql("select * from join_users inner join final_badges where join_users.userlink=final_badges.UserId")

    #join_tables.createOrReplaceTempView('join_tables')

    #final_users=spark.sql("select users.Id as userlink,users.CreationDate as membersince , 1+DateDiff(Day,users.CreationDate,badges.Date) As DaysMembership")

    final=spark.sql(" SELECT users.Id as [User Link],users.CreationDate as [Member Since],badges.Date as [Date Won],DateDiff(TIMESTAMP(users.CreationDate), TIMESTAMP(badges.Date)) As [DaysMembership]\
                        FROM badges INNER JOIN users ON badges.UserId = users.Id and users.sites=badges.sites WHERE badges.Name ='"+(badge_name)+"'")

    #final_badges.createOrReplaceTempView('join_users')

    #groupby user id and display name and counting the total num of badges

    #group_by=final.groupby("UserId","DisplayName").count()

    #newtable=group_by.select(col("UserId"),col("count").alias("numbadges"),col("DisplayName").alias("username"))

    #sorting the values in descending order and displaying top 100 users who has highest number of badges.

    #ordered_table=newtable.sort("numbadges", ascending=False).show(100)

    #displaying table
 
    final.show()
                                           
if __name__ == "__main__":

    users_table = sys.argv[1]

    badges_table=sys.argv[2]

    badge_name=sys.argv[3]

    main(users_table,badges_table,badge_name)

#How to run this file?
#spark-submit top100_badges.py Users_parquet badges_parquet
