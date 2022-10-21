# data: walmart_ecommerce_product_details.json
# questions: walmart questions.docx
# notebook file: walmart_task_solutions.ipynb
# python file: walmart_task_solutions.py

# At first, create a spark application, read the json file into a dataframe, and explore the data

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('walmart')\
    .config('spark.driver.extraClassPath', '/usr/lib/jvm/java-11-openjdk-amd64/lib/postgresql-42.5.0.jar')\
    .getOrCreate()

# read the walmart_ecommerce_product_details.json file into a dataframe df

df_walmart = spark.read.json("walmart_ecommerce_product_details.json")


#view the data in the dataframe

# df_walmart.show()


#removing corrupt record column

df_walmart = df_walmart.drop('_corrupt_record')


#view the schema of the dataframe

df_walmart.printSchema()

###
df_walmart\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "public.walmart",
        properties={"user": "postgres", "password": ""})

# Clean the data: assign appropriate data types of Columns and rename columns to remove spaces
#all columns have data type string. So, we need to convert the datatypes using cast method

from pyspark.sql.types import StringType, FloatType, IntegerType, BooleanType, TimestampType
from pyspark.sql.functions import col

df_walmart = df_walmart\
        .withColumn('Available', col('Available').cast(BooleanType()))\
        .withColumn('Brand', col('Brand').cast(StringType()))\
        .withColumn('Category', col('Category').cast(StringType()))\
        .withColumn('Crawl Timestamp', col('Crawl Timestamp').cast(TimestampType()))\
        .withColumn('Description', col('Description').cast(StringType()))\
        .withColumn('Gtin', col('Gtin').cast(IntegerType()))\
        .withColumn('Item Number', col('Item Number').cast(IntegerType()))\
        .withColumn('List Price', col('List Price').cast(FloatType()))\
        .withColumn('Package Size', col('Package Size').cast(StringType()))\
        .withColumn('Postal Code', col('Postal Code').cast(StringType()))\
        .withColumn('Product Name', col('Product Name').cast(StringType()))\
        .withColumn('Product Url', col('Product Url').cast(StringType()))\
        .withColumn('Sale Price', col('Sale Price').cast(FloatType()))\
        .withColumn('Uniq Id', col('Uniq Id').cast(StringType()))


#rename columns with spaces in their names

df_walmart = df_walmart\
            .withColumnRenamed('Crawl Timestamp', 'Crawl_Timestamp')\
            .withColumnRenamed('Item Number', 'Item_Number')\
            .withColumnRenamed('List Price', 'List_Price')\
            .withColumnRenamed('Package Size', 'Package_Size')\
            .withColumnRenamed('Postal Code', 'Postal_Code')\
            .withColumnRenamed('Product Name', 'Product_Name')\
            .withColumnRenamed('Product Url', 'Product_Url')\
            .withColumnRenamed('Sale Price', 'Sale_Price')\
            .withColumnRenamed('Uniq Id', 'Uniq_Id')


#view the schema 

df_walmart.printSchema()



#create a separate dataframe for storing product-related data only (as most questions are related to products)

df_product= df_walmart.select(
    ["Product_Name", "Category", "Brand", "Description",\
     "List_Price","Sale_Price","Available", "Product_Url"])

# df_product.show()
df_product.printSchema()


#remove duplicate rows from product dataframe
df_product = df_product.dropDuplicates()
# df_product.orderBy(df_product.Product_Name).show()



#create sql table for df_walmart and df_product

df_walmart.createOrReplaceTempView("walmart_table")
df_product.createOrReplaceTempView("product_table")



# # Questions and Solutions

# ###       1. Get the Brand along with products associated with it.


#sqlway

brand_products = spark.sql("""
                            SELECT Brand, Product_Name
                            FROM product_table 
                            """)
brand_products.show()


#dataframe way
#using df_product and window function

import pyspark.sql.functions as f
from pyspark.sql import Window
import pyspark.sql.types as t


windowSpec=Window.partitionBy("Brand")

brand_products=df_product.withColumn("Products",f.collect_list(f.col("Product_Name")).over(windowSpec))
question1 = brand_products.select('Brand', 'Product_Name')

question1.show()
question1.toPandas().to_csv('walmart_output_csv/1.Brand_with_Products.csv', index=False)




# ###         2. List all the product names whose list price is greater than sales price

product_names = spark.sql("""
                          SELECT Product_Name, List_Price, Sale_Price, (List_Price - Sale_Price) as Difference
                          FROM product_table
                          WHERE List_Price > Sale_Price
                            """)

product_names.show()


#dfway
from pyspark.sql.functions import col

product_names_df = df_product.filter(col('List_Price') > col('Sale_Price')).select(col('Product_Name')\
.alias("Product Names"), col('List_Price').alias("List Price"), col('Sale_Price')\
.alias("Sale Price"),\
(col('List_Price')-col('Sale_Price')).alias('Difference in Price'))

product_names_df.show()

product_names_df.toPandas().to_csv('walmart_output_csv/2.Products_ListPrice_greaterthan_SalePrice.csv', index=False)




# ###       3. Count the number of product names whose list price is greater than sales price

#sqlway

product_names.createOrReplaceTempView("product_names_tbl")

number_of_products = spark.sql("""
                                SELECT COUNT(Product_Name) as Number_of_Products
                                FROM product_names_tbl                                
                                """)

number_of_products.show()




#dfway

number_of_products_df = product_names_df.count()
number_of_products_df


# ###       4. List all the products belong to a “women” category.

#sqlway

women_products = spark.sql("""
                            SELECT Product_Name, Category
                            FROM product_table
                            WHERE Category LIKE "%Women%" 
                            OR Category LIKE "%women%"
                        """)
women_products.show()


#dfway

women_products_df = df_product.where(col('Category').contains('Women') | col('Category').contains('women'))\
                              .select('Product_Name', 'Category').distinct()

women_products_df.show()

women_products_df.toPandas().to_csv('walmart_output_csv/4.Products_of_Category_Women.csv', index=False)



# ###      5. List the products which are not available.

# SQL way
unavailable_products = spark.sql("""
                            SELECT DISTINCT(Product_Name), Available as Availability
                            FROM product_table
                            WHERE Available = FALSE
                        """)

unavailable_products.show()


#dfway
unavailable_products_df = df_product.filter(df_product['Available'] == 'False')
unavailable_products_df.show()

unavailable_products_df.toPandas().to_csv('walmart_output_csv/5.Products_Not_Available.csv', index=False)

# df way
df_product.select(["Product_Name", "Available"]).where(df_product.Available == "FALSE").show()




# ###         6. Count the number of products which are available.

#sqlway
products_count = spark.sql("""
                            SELECT COUNT(Product_Name)
                            FROM product_table
                            WHERE Available = TRUE""")

products_count.show()


#dfway
available_products = df_product.filter(df_product['Available']== True)

available_products.count()

available_products.toPandas().to_csv('walmart_output_csv/6.Available_Products.csv', index=False)



# ###         7. List the products that are made up of Nylon.

#sqlway
products = spark.sql("""
                        SELECT DISTINCT(Product_Name), Description 
                        FROM product_table 
                        WHERE Description LIKE "%Nylon%" 
                        OR Description LIKE "%nylon%" 
                    """)
products.show()
products.count()


#dfway
products_nylon = df_product.where(col('Description').contains('Nylon') | col('Description').contains('nylon'))\
                           .select('Product_Name', 'Description').distinct()
products_nylon.show()

products_nylon.toPandas().to_csv('walmart_output_csv/7.Products_made_of_Nylon.csv', index=False)


#counting to make sure 
df_product.where(col('Description').contains('Nylon') | col('Description').contains('nylon'))\
.select('Product_Name', 'Description').distinct().count()