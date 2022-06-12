answersDF.createOrReplaceTempView("adfTab")
print('**************************************************ANSWERS**********************************\
*******************\n')
answersDF. \
    withColumn('creation_date',date_format('creation_date','MM/dd/yyyy')). \
    withColumnRenamed('creation_date','date'). \
    withColumnRenamed('comments','cmts'). \
    show(5,truncate=False)
print('total answers provided: ' + str(answersDF.count())+'\n')
print('schema: ')
answersDF.printSchema()
print('\n\n**********************************************QUESTIONS****************************\
*************************\n')
questionsDF.withColumnRenamed('accepted_answer_id','answer_id').createOrReplaceTempView("qdfTab")
spark.sql('SELECT * from qdfTab where (length(title) + length(string(tags))) < 41'). \
    withColumn('creation_date',date_format('creation_date','MM/dd/yyyy')). \
    withColumnRenamed('creation_date','date'). \
    withColumnRenamed('comments','cmts'). \
    show(5,truncate=False)
print('questions without ans: ' + str(questionsDF.withColumnRenamed('accepted_answer_id','answer_id'). \
                          filter('accepted_answer_id IS NULL').count()))
print('questions with answer: ' + str(questionsDF.withColumnRenamed('accepted_answer_id','answer_id').\
                          filter('accepted_answer_id IS NOT NULL').count()))
print('total questions asked: ' + str(questionsDF.count())+'\n')
print('schema: ')
questionsDF.withColumnRenamed('accepted_answer_id','answer_id').printSchema()

qDF = questionsDF.withColumnRenamed('comments', 'cmtQ'). \
    withColumnRenamed('user_id', 'userQ'). \
    withColumnRenamed('creation_date', 'dateQ'). \
    withColumnRenamed('question_id', 'idQ'). \
    withColumnRenamed('accepted_answer_id', 'idAQ')
        
aDF = answersDF.withColumnRenamed('creation_date', 'dateA'). \
    withColumnRenamed('comments', 'cmtA'). \
    withColumnRenamed('user_id', 'userA'). \
    withColumnRenamed('question_id', 'idQ'). \
    withColumnRenamed('score', 'S'). \
    withColumnRenamed('answer_id', 'idA')

jDF = qDF.join(aDF, 'idQ'). \
    withColumn('dateQ',date_format('dateQ','MMddyyyy')). \
    withColumn('dateA',date_format('dateA','MMddyyyy')). \
    orderBy('idA')

jDF.createOrReplaceTempView("jTab")
spark.sql('SELECT * from jTab where (length(title) + length(string(tags))) < 39 and size(tags)>1'). \
    select('idA','dateA','userA','cmtA','S','idQ','dateQ','title','tags','userQ','cmtQ','views'). \
    withColumn('tags',concat_ws(',',col('tags'))). \
    show(truncate=False)

print('\ncount of questions left-joined to the right of answers: ' + str(jDF.count()) +'\n')

qDF2 = questionsDF.withColumnRenamed('comments', 'cmtQ'). \
    withColumnRenamed('user_id', 'userQ'). \
    withColumnRenamed('creation_date', 'dateQ'). \
    withColumnRenamed('question_id', 'idQ'). \
    withColumnRenamed('accepted_answer_id', 'idA')

aDF2 = answersDF.withColumnRenamed('creation_date', 'dateA'). \
    withColumnRenamed('comments', 'cmtA'). \
    withColumnRenamed('user_id', 'userA'). \
    withColumnRenamed('question_id', 'idQA'). \
    withColumnRenamed('score', 'S'). \
    withColumnRenamed('answer_id', 'idA')

jDF2 = aDF2.join(qDF2, 'idA'). \
    withColumn('dateQ',date_format('dateQ','MMddyyyy')). \
    withColumn('dateA',date_format('dateA','MMddyyyy')). \
    orderBy(['idQ','S','views'],ascending=[True,False,False])

jDF2.createOrReplaceTempView("jTab2")
print('Answers left-joined to the right of questions:\n')
spark.sql('SELECT * from jTab2 where (length(title) + length(string(tags))) < 39 and size(tags)>1'). \
    select('idQ','dateQ','title','tags','userQ','cmtQ','views','idA','dateA','userA','cmtA','S'). \
    withColumn('tags',concat_ws(',',col('tags'))). \
    show(truncate=False)

print('\ncount of answers left-joined to the right of questions: ' + str(jDF2.count()) +'\n')

print('number of answers per question per month: \n')
answers_month.show(3)
print('answers_month total rows: '+str(answers_month.count()))
print('\n\nresultDF: \n')
resultDF.orderBy('question_id', 'month').show(truncate=False)
print('resultDF #rows: '+str(resultDF.count()))

print('questionsDF total # rows:                  '+str(questionsDF.select('question_id').count()))
print('questionsDF question_id # distinct:        '+str(questionsDF.select('question_id').distinct().count())+' (total # of unique questions)')
print('questionsDF accepted_answer_id # distinct: '+str(questionsDF.select('accepted_answer_id').distinct().count())+' (< 86936 because some questions have no answers)\n')
print('answersDF total # rows:           '+str(answersDF.select('question_id').count()) + ' (total # of unique answers)')
print('answersDF  answer_id  # distinct: '+str(answersDF.select('answer_id').distinct().count())+ ' (> 86936 because some questions have multiple proposed answers)')
print('answersDF question_id # distinct:  '+str(answersDF.select('question_id').distinct().count())+' (> 33283 because not some answers arent accepted)')
print('                                         (< 86936 because some questions arent answers)')


