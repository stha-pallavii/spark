# #### data: udemy_courses.json
# #### questions: udemy_questions.docx
# #### notebook: udemy_task_solutions.ipynb
# #### python file: udemy_task_solutions.py

# At first, create a spark application, read the json file into a dataframe, and explore the data

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("udemy").getOrCreate()


# read the udemy_courses.json file
udemy_df = spark.read.format('json').load('udemy_courses.json')


#view the data in the dataframe
udemy_df.show(10)


#removing corrupt record column

udemy_df = udemy_df.drop('_corrupt_record')


#view the schema of the dataframe
udemy_df.printSchema()



#all columns have data type string. So, we need to convert the datatypes using cast method

from pyspark.sql.types import StringType, FloatType, IntegerType, BooleanType, TimestampType
from pyspark.sql.functions import col


udemy_df = udemy_df\
            .withColumn('content_duration', col('content_duration').cast(FloatType()))\
            .withColumn('course_id', col('course_id').cast(IntegerType()))\
            .withColumn('course_title', col('course_title').cast(StringType()))\
            .withColumn('is_paid', col('is_paid').cast(BooleanType()))\
            .withColumn('level', col('level').cast(StringType()))\
            .withColumn('num_lectures', col('num_lectures').cast(IntegerType()))\
            .withColumn('num_reviews', col('num_reviews').cast(IntegerType()))\
            .withColumn('num_subscribers', col('num_subscribers').cast(IntegerType()))\
            .withColumn('price', col('price').cast(IntegerType()))\
            .withColumn('published_timestamp', col('published_timestamp').cast(TimestampType()))\
            .withColumn('subject', col('subject').cast(StringType()))\
            .withColumn('url', col('url').cast(StringType()))


udemy_df.printSchema()


udemy_df.show()



# create a separate dataframe for course (since most of the questions are related to course only)

course_df=udemy_df.select(["course_id","course_title","url","is_paid", "price", "num_subscribers","num_reviews",
                           "num_lectures", "subject","level","content_duration", "subject"
                            ])

course_df.printSchema()
course_df = course_df.drop_duplicates()

# course_df.show()


#create SQL table from dataframe

udemy_df.createOrReplaceTempView("udemy_table")
course_df.createOrReplaceTempView("course_table")


# ---------------------------------------------------------------------------------------------------------------------------------
# # Questions and Solutions


# ###   1. What are the best free courses by subject?

#assumption: best course = course having maximum reviews

free_courses_df= course_df.filter(course_df.is_paid == "False")

# free_courses_df.show()
# free_courses_df.printSchema()

from pyspark.sql.functions import rank, max, desc, collect_list
from pyspark.sql import Window as W 


best_free_courses_df = free_courses_df\
                    .groupBy('subject', 'course_title')\
                    .agg(max('num_reviews').alias('Number of Reviews'))\
                    .orderBy(desc('Number of Reviews'))


#creating partition by subject in descending order of num_reviews
window_spec = W.partitionBy('subject')\
                .orderBy(desc(col('Number of Reviews')))

#get courses with top 3 rank in each partition
top_3_best_courses = best_free_courses_df\
                            .withColumn('rn', rank().over(window_spec))\
                            .where('rn <= 3')\
                            .drop('rn')

# subjects and their three best courses along with Number of Reviews
top_3_best_courses\
    .withColumn('Best Free Courses', collect_list('course_title').over(W.partitionBy('subject')))\
    .select('subject', 'Best Free Courses', 'Number of Reviews')\
    .distinct()\
    .dropna()\
    .show()


# ---------------------------------------------------------------------------------------------------------------------------------


# ###     2. What are the most popular courses?

#assumption: num_subscribers indicates popularity

#sql way
# getting top 10 most popular courses

popular_courses = spark.sql("""
                            SELECT course_title AS Course_Titles, num_subscribers AS Number_of_Subscribers
                            FROM course_table
                            ORDER BY 2 DESC
                            LIMIT 10                            
                            """)

popular_courses.show()



#dfway

from pyspark.sql.functions import desc

popular_courses_df = course_df.select(col('course_title').alias("Course_Titles"),\
                                      col('num_subscribers').alias("Number_of_Subscribers"))\
                                      .orderBy(desc(col("num_subscribers")))

popular_courses_df.show(10) 


# ---------------------------------------------------------------------------------------------------------------------------------

# ###   3. List the courses that are specialized to “Business Finance” and find the average number of subscribers, reviews, price and lectures on the subject.

#sql way
#step1. create a df business_finance_courses consisting of courses that are specialized to “Business Finance”

business_finance_courses = spark.sql("""
                                        SELECT * FROM course_table
                                        WHERE subject == "Business Finance"                         
                                    """)

# business_finance_courses.show()

#step2. calculate average no. of subscribers, reviews, price, and lectures on subject “Business Finance”
#using business_finance_courses df

business_finance_courses.createOrReplaceTempView("business_finance_courses_table")

average_data = spark.sql("""
                            SELECT 
                                avg(num_lectures) `Average Lectures`,
                                avg(num_subscribers) `Average Subscribers`,
                                avg(num_reviews) `Average Reviews`,
                                avg(price) `Average Price`                                
                            FROM business_finance_courses_table
                        """)

average_data.show()



#dfway

#step1. create dataframe business_finance_df containing info of only those courses that are in "Business Finance" subject
business_finance_df = course_df.filter(col('subject')=='Business Finance')
business_finance_df.show(5)


#step2. get list of courses in the subject "Business Finance"

courses_business_finance = business_finance_df.select('course_title', 'subject')
courses_business_finance.show(10)


#step3. calculating average values for the subject

business_finance_df.agg({'num_lectures': 'mean', 'num_subscribers' : 'mean', 'num_reviews': 'mean',\
                         'price': 'mean'}).show()


# ---------------------------------------------------------------------------------------------------------------------------------

# ###   4. How are courses related?

#grouping titles by subject

#sqlway
titles_subjects = spark.sql("""
                                SELECT subject, course_title 
                                FROM course_table
                                ORDER BY subject
                            """)

titles_subjects.show()


#dfway

titles_subjects_df = course_df.select(col('subject'), col('course_title'))\
                              .dropna()\
                              .distinct()\
                              .orderBy(col('subject'))\
                              .show()



#dfway 2

from pyspark.sql.functions import collect_set

window_spec = W.partitionBy('subject')
course_relation_df = course_df\
                .withColumn('Course List', collect_set('course_title').over(window_spec))\
                .select('subject', 'Course List')\
                .distinct().dropna()

course_relation_df.show()


# ---------------------------------------------------------------------------------------------------------------------------------

# ###  5. Which courses offer the best cost benefit?

import pyspark.sql.functions as f

#Assumption 1: course having price less than 100, reviews more than 5000, and subscribers more than 50000 
#offer the best cost benefit

best_cost_benefit_courses=course_df.filter(f.col("num_subscribers")>50000)\
                                   .filter(f.col("num_reviews")>5000)\
                                   .filter(f.col("price")<500)

best_cost_benefit_courses.show()


#Assumption 2: longest yet cheapest course = best cost benefit 
# find courses having longest content_duration with lowest price

#sqlway
more_duration_less_price_courses = spark.sql("""
                                                select course_title, content_duration, price from course_table
                                                order by content_duration desc, price asc                                            
                                            """)

more_duration_less_price_courses.show(10)



#dfway
from pyspark.sql.functions import asc

course_df.orderBy(desc('content_duration'), asc('price')).select('course_title', 'content_duration', 'price').show()

# ---------------------------------------------------------------------------------------------------------------------------------


# ###     6. Find the courses which have more than 15 lectures.

#sqlway

courses_morethan15 = spark.sql("""
                                SELECT course_title Course, num_lectures `Number of Lectures`
                                FROM course_table
                                WHERE num_lectures > 15
                                ORDER BY num_lectures
                            """)

courses_morethan15.count()
courses_morethan15.show(10)


#dfway

course_df.filter(col('num_lectures') > 15).select(col('course_title').alias('Course'),\
                                                  col('num_lectures').alias('Number of Lectures'))\
         .orderBy(col('num_lectures'))\
         .show(10)


# ---------------------------------------------------------------------------------------------------------------------------------


# ###     7. List courses on the basis of level.

#dfway
from pyspark.sql.functions import collect_set

window_spec = W.partitionBy('level')
course_list_df = course_df\
                .withColumn('Course List', collect_set('course_title').over(window_spec))\
                .select('level', 'Course List')\
                .distinct().dropna()

course_list_df.show()


# ---------------------------------------------------------------------------------------------------------------------------------


# ###     8. Find the courses which have duration greater than 2 hours.

#sqlway
courses_duration = spark.sql("""
                                SELECT course_id, course_title, content_duration `Duration(Hours)`
                                FROM course_table
                                WHERE content_duration > 2.0
                                ORDER BY 3,1
                            """)

courses_duration.show()


#dfway
course_df.filter(col('content_duration') > 2.0).select('course_id', 'course_title', 'content_duration')\
         .distinct().orderBy(col('content_duration')).show()