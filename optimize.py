'''
Optimize the query plan

Suppose we want to compose query in which we get for each question also the
number of answers to this question for each month. See the query below which
does that in a suboptimal way and try to rewrite it to achieve a more optimal plan.
'''


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month
import time
import os


spark = SparkSession.builder.appName('Optimize I').getOrCreate()

base_path = os.getcwd()

project_path = ('/').join(base_path.split('/')[0:-3])

answers_input_path = os.path.join(project_path, 'data/answers')

questions_input_path = os.path.join(project_path, 'data/questions')
s0a=time.perf_counter()
answersDF = spark.read.option('path', answers_input_path).load()
e0a=time.perf_counter()

s0=time.perf_counter()
questionsDF = spark.read.option('path', questions_input_path).load().coalesce(4)
e0=time.perf_counter()
'''
Answers aggregation

Here we : get number of answers per question per month
'''
s1=time.perf_counter()
answers_month = answersDF.withColumn('month', month('creation_date')) \
    .groupBy('question_id', 'month').agg(count('*').alias('cnt')).coalesce(4)
e1=time.perf_counter()

s2=time.perf_counter()
resultDF = questionsDF.join(answers_month, 'question_id') \
    .select('question_id', 'creation_date', 'title', 'month', 'cnt')
e2=time.perf_counter()

s3=time.perf_counter()
resultDF.orderBy('question_id', 'month').show()
e3=time.perf_counter()
'''
Task:

see the query plan of the previous result and rewrite the query to optimize it
'''
print(f"Completed answersDF in {e0a-s0a} seconds")
print(f"Completed questionsDF in {e0-s0} seconds")
print(f"Completed answers_month in {e1-s1} seconds")
print(f"Completed resultsDF in {e2-s2} seconds")
print(f"Completed resultsDF.show in {e3-s3} seconds")
print(f"Total elapsed time: {e3-s0a} seconds")
