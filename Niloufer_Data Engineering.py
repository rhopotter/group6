#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install pyspark


# In[21]:


from pyspark.sql import SparkSession


# In[22]:


# Create a Spark session
spark = SparkSession.builder.appName("CSVExtractorNiloufer").getOrCreate()


# In[23]:


# Specify the path to your CSV file
csv_file_path = 'D:\D DRIVE\Niloufer\FY23\Analytics\Training\Data Enginerring\Hydra-Movie-Scrape.csv'


# In[24]:


# Specify the path to your CSV file
csv_file_path = 'D:/D DRIVE/Niloufer/FY23/Analytics/Training/Data Enginerring/Hydra-Movie-Scrape.csv'


# In[25]:


# Read the CSV file into a DataFrame
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)


# In[26]:


# Show the DataFrame
df.show()


# i) Which movies were released in the year 2020?

# In[28]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("MovieFilterNiloufer").getOrCreate()

# Assuming you have a DataFrame named movies_df
# Read your data into movies_df or replace this line with your data loading logic
movies_df = spark.read.csv('D:/D DRIVE/Niloufer/FY23/Analytics/Training/Data Enginerring/Hydra-Movie-Scrape.csv', header=True, inferSchema=True)

# Filter movies released in the year 2020
movies_2020_df = movies_df.filter(col('Year') == 2020)


# In[29]:


movies_2020_df.show()


# ii)What is the average IMDb rating of the movies in the dataset?
# 

# In[31]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Create a Spark session
spark = SparkSession.builder.appName("AverageRatingNiloufer").getOrCreate()

# Assuming you have a DataFrame named movies_df
# Read your data into movies_df or replace this line with your data loading logic
movies_df = spark.read.csv('D:/D DRIVE/Niloufer/FY23/Analytics/Training/Data Enginerring/Hydra-Movie-Scrape.csv', header=True, inferSchema=True)

# Calculate the average IMDb rating
average_rating = movies_df.agg(avg(col('rating')).alias('average_rating')).collect()[0]['average_rating']

# Display the average rating
print(f"The average IMDb rating of the movies is: {average_rating:.2f}")


# iii) Which movies have the longest and shortest runtimes?

# In[34]:


longest_runtime_movie = movies_df.orderBy(col('Runtime').desc()).first()
print(f"The movie with the longest runtime is: {longest_runtime_movie['Title']} with a runtime of {longest_runtime_movie['Runtime']} minutes.")


shortest_runtime_movie = movies_df.orderBy(col('Runtime').asc()).first()
print(f"The movie with the shortest runtime is: {shortest_runtime_movie['Title']} with a runtime of {shortest_runtime_movie['Runtime']} minutes.")


# iv) How many movies were directed by each director?
# 

# In[36]:


from pyspark.sql.functions import count

# Count the number of movies per director
movies_per_director = movies_df.groupBy('Director').agg(count('*').alias('num_movies'))

# Display the results
movies_per_director.show()


# v) Who are the unique writersin the dataset?

# In[38]:


# Get unique writers from the DataFrame
unique_writers = movies_df.select('Writers').distinct()

# Display the results
unique_writers.show()


# vi) Which movies have an IMDb rating greater than 8.0?
# 

# In[39]:


# Filter movies with an IMDb rating greater than 8.0
high_rated_movies = movies_df.filter(col('Rating') > 8.0)

# Display the results
high_rated_movies.show()


# vii) Which movies do not have a YouTube trailer code?

# In[40]:


# Filter movies without a YouTube trailer code
movies_without_trailer = movies_df.filter(col('YouTube Trailer').isNull() | (col('YouTube Trailer') == ''))

# Display the results
movies_without_trailer.show()


# viii) How many movies does each cast member appear in?

# In[41]:


# Count the number of movies per cast member
movies_per_cast_member = movies_df.groupBy('Cast').agg(count('*').alias('num_movies'))

# Display the results
movies_per_cast_member.show()


# Load the transformed data into a destination (e.g., another file, database).

# In[55]:


from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("WriteToParquet_niloufer").getOrCreate()


# Assuming you have a DataFrame named transformed_data
# Replace this line with your transformation logic
transformed_data = spark.read.csv('D:/D DRIVE/Niloufer/FY23/Analytics/Training/Data Enginerring/Hydra-Movie-Scrape.csv', header=True, inferSchema=True)

# Specify the path for the new Parquet file
output_parquet_path = 'D:/D DRIVE/Niloufer/FY23/Analytics/Training/Data Enginerring/New/output.parquet'

# Write the DataFrame to a Parquet file
transformed_data.write.parquet(output_parquet_path, mode='overwrite')


# In[ ]:




