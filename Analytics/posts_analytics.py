from pyspark import SparkConf, SparkContext
import sys,re,math,os,gzip,uuid,datetime
assert sys.version_info >= (3, 5)
 # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row

spark = SparkSession.builder.appName('StackExchange Data Loader').config("spark.executor.memory", "70g").config("spark.driver.memory", "50g").config("spark.memory.offHeap.enabled",'true').config("spark.memory.offHeap.size","16g").getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
sc = spark.sparkContext

def main(inputs):
    #Read Posts table
    posts = spark.read.parquet(inputs).repartition(96)
    #filter questions
    questions = posts.filter(posts['PostTypeId']==1).cache()
    #filter answers
    answers = posts.filter(posts['PostTypeId']==2).cache()

    q_per_site = questions.groupBy('sites').agg(functions.count('id').alias('ques'))
    a_per_site = answers.groupBy('sites').agg(functions.count('id').alias('ans'))


    q_with_year = questions.withColumn("year",functions.year('CreationDate'))
    a_with_year = answers.withColumn("year",functions.year('CreationDate'))

    q_per_year = q_with_year.groupBy('year').agg(functions.count('id').alias('no_of_ques'))
    a_per_year = a_with_year.groupBy('year').agg(functions.count('id').alias('no_of_ans'))
    
    posts.createOrReplaceTempView('posts')
    #Calcuate average response time in days for each site
    res = spark.sql("select avg(datediff(TIMESTAMP(a.creationdate),TIMESTAMP(q.creationdate))) as days, stddev(datediff(TIMESTAMP(a.creationdate),TIMESTAMP(q.creationdate))) as stdev \
                    ,q.sites from posts q join posts a on a.id = q.acceptedanswerid and a.sites = q.sites group by q.sites")   
    
    #number of questions vs number of answers per year
    qa_per_year = q_per_year.join(a_per_year,['year']).sort(['year'])
    qa_per_year.show()
    qa_per_year.write.csv('qa_per_year',mode="overwrite")

    #number of questions vs number of answers for all sites 
    qa_per_site = a_per_site.join(q_per_site,['sites'])
    qa_per_site.show()
    qa_per_site.write.csv("qa_per_site",mode="overwrite")


    # Display "Expected Response time in days for all sites"
    res.show()
    res.write.csv("avg_resp_time_days",mode="overwrite")

    # Find Accepted Answer Id's and average number of words in accepted answer
    questions.createOrReplaceTempView("q")
    answers.createOrReplaceTempView("a")
    res = spark.sql("select q.title, a.body as ans,q.sites as sites from q join a on q.AcceptedAnswerId == a.Id and a.sites== q.sites")
    res = res.withColumn('wordCount',functions.size(functions.split(functions.col("ans"), " ")))
    res = res.groupBy('sites').agg(functions.avg('wordCount').alias('wordCount'))

    #Displaying "Average number of words used in accepted answers"
    res.show()
    res.write.csv("avg_acc_ans_words",mode="overwrite")
    
    # Average number of answers for a question per site
    final = q_per_site.join(a_per_site,['sites'])
    final = final.withColumn('avg_num_of_ans_per_site',final.ans/final.ques)
    final.show()
    final.write.csv("avg_num_of_ans_per_site",mode="overwrite")


if __name__ == "__main__":
    inputs = sys.argv[1]
    main(inputs)

#How to run this file?
#spark-submit posts_analytics.py /user/cmandava/Posts_parquet
