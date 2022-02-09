# Databricks notebook source
# MAGIC %md
# MAGIC #Summersalt Data Engineering Exercise
# MAGIC <p>
# MAGIC   As part of the next steps in our interview process, we would like you to complete the following data engineering exercise. This exercise is meant to be a quick assessment of some high-level concepts that will be pivotal in the success of this position. Our focus is on the approach you take to each step, so if complete code can not be written, please do your best to use pseudo-code / outlines / descriptions of how you would complete the task.
# MAGIC We want to be conscious of your time, so please do not spend more than 1 hour completing the exercise.
# MAGIC Please submit your results via email as a git bundle.
# MAGIC </p>
# MAGIC <br>
# MAGIC <p>
# MAGIC   Datausa.io is a website that provides demographic data for the United States. The site offers a simple to use API that does not require authentication. To keep this exercise as quick and concise as possible, we will be using these API endpoints to pull data. These endpoints pull yearly data sets at the PUMA (public use micro areas) level.
# MAGIC Please keep in mind that this is just a toy dataset, some of the asks may seem odd or not provide usable information, but we wanted to make the data as easy to understand as possible and not require any authentication.
# MAGIC <br /><br />
# MAGIC University Graduations by PUMA<br />
# MAGIC https://datausa.io/api/data?measures=Completions&drilldowns=PUMA,University
# MAGIC <br /><br />
# MAGIC Engineering Graduations by PUMA<br />
# MAGIC http://datausa.io/api/data?CIP=14&drilldowns=PUMA&measure=Completions
# MAGIC <br /><br />
# MAGIC Population metrics by PUMA <br>
# MAGIC https://datausa.io/api/data?measure=ygcpop%20RCA,Total%20Population,Total%20Population%20 MOE%20Appx,Average%20Wage,Average%20Wage%20Appx%20MOE,Record%20Count&drilldown s=PUMA&Record%20Count%3E=5
# MAGIC <br /><br />
# MAGIC Population metrics for engineers by PUMA<br />
# MAGIC https://datausa.io/api/data?CIP2=14&measure=ygcpop%20RCA,Total%20Population,Total%20Popul ation%20MOE%20Appx,Average%20Wage,Average%20Wage%20Appx%20MOE,Record%20Count& drilldowns=PUMA&Record%20Count%3E=5&Workforce%20Status=true</p>

# COMMAND ----------

#import necessary libraries
import json
import requests
import pandas as pd
import numpy as np

# COMMAND ----------

def get_urls(): 
    urls=[
        #University graduation by PUMA
        "https://datausa.io/api/data?measures=Completions&drilldowns=PUMA,University",

        #Engineering graduation by PUMA
        "https://datausa.io/api/data?CIP=14&drilldowns=PUMA&measure=Completions", 
    
        #Population metrics by PUMA
        "https://datausa.io/api/data?measure=ygcpop%20RCA,Total%20Population,Total%20Population%20", 
    
        #Population metrics for engineers by PUMA
        "https://datausa.io/api/data?CIP2=14&measure=ygcpop%20RCA,Total%20Population,Total%20Popul" 
    ]
    return urls

# COMMAND ----------

def load_API_data(urls):
    for i in urls:
        json_data = requests.get(i).text
        json_data_ = json.loads(json_data)
        df = pd.json_normalize(json_data_, record_path=['data'])
   
        df.rename(columns={'ID PUMA':'ID_PUMA','ID University': 'ID_University','ID Year':'ID_Year',
                                'Slug University':'Slug_University','ygcpop RCA':'ygcpop_RCA','Total Population':'Total_Population','ID CIP2':'ID_CIP2'}, inplace=True)
  
        #df.drop(labels=["ID PUMA", "ID University", "ID Year","Slug University","Slug PUMA",'ygcpop RCA',"Total Population","ID CIP2"], axis=1,                          inplace=True, errors='ignore')
        df_spark = spark.createDataFrame(df)
        display (df.head())

    return df_spark

# COMMAND ----------

def save_spark_data(spark_data__):
    puma_data_df.write.format("parquet").saveAsTable(i)

# COMMAND ----------

def main():
    #Get API data urls 
    data_urls = get_urls()
    
    #Load data from API data urls
    puma_data_df = load_API_data(data_urls)
    
    data_tables = ['graduatesbyuniversity', 'pumacompletions', 'totapulation', 'rcapopulation' ]
    for i in data_tables:
        #Save puma dataframe in cloud storage
        puma_data_df.write.format("parquet").saveAsTable(i)
    
if __name__ == '__main__':
    main()

# COMMAND ----------

# MAGIC %md 
# MAGIC #SQL queries from data tables 
# MAGIC <ul>
# MAGIC   <li> total number of graduations</li> <strong>Pseudo: Aggregate all graducation counts</strong>
# MAGIC <li> total number of graduations from engineering majors</li> Pseudo: Aggregate all graducation where major is engineer
# MAGIC <li> % of each states graduations that were from engineering majors</li> Pseudo: Aggregate all graducation counts
# MAGIC <li> Estimate of total wages for the entire population</li>
# MAGIC <li> Estimate of total wages for all engineers</li>
# MAGIC <li> % of wages that were from engineers</li>
# MAGIC <li> Any additional columns that may be helpful from a Data Engineering perspective (think items that could help with internal tasks or maintenance, no need to add columns for better analysis)</li>
# MAGIC </ul>

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT SUM(column_name) as total_graduates FROM table_name 

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT SUM(column_name) FROM table_name WHERE major='engineering'; 

# COMMAND ----------

# MAGIC %md
# MAGIC <p>For the following questions feel free to write code if needed, but simple explanations would also work if that is quicker.</p>
# MAGIC <ul>
# MAGIC <li>How would you determine on what frequency to update the data? After determining the appropriate updating frequency (i.e. hourly, daily, monthly, etc), how would you automate the process to keep the data up to date?</li>
# MAGIC   <strong>I suppose graduation or census data gets updated once every year. But generally, the business (data) requirements define the frequency that a dataset needs to be updated. I would use azure databricks job to automate the process to keep the data up to date, as defined by the data requirements.</strong> 
# MAGIC <li> Assume that past graduation data can be updated for up to two years. Write a SQL query (or series of queries) to keep our dataset in sync with the most recent information.</li>
# MAGIC <li> Using SQL, how can we prevent duplicate data? How can we test to make sure our unique identifier is still unique?</li>
# MAGIC   <strong>There are many different ways to properly manage duplicate data in a data table, depending on the function in question (i.e insert or select...). If we are checking for dupicates, we can use select disctinct command. If we are inserting data into a table, we can use INSERT IGNORE command. We can also get creative with UNIQUE index and PRIMARY KEYS. </strong>
# MAGIC <li> Give a brief explanation on how you would make this new aggregated data available for an external client to consume–on either a push or pull basis. Let’s assume the data is too large to pass via email/csvs.</li>
# MAGIC <strong>In this exercise, we started out by reading json data from an API url endpoint. And then we parsed json data strings into an object, from an object to a dataframe which we later flatten with pandas to render pretty data tables.  After that, we store the data in a cloud data storage in parquet format for quick spark processing. After storing the parsed data, we can easily make this new aggregated data available for an external client to consume on either a push or pull basis. Another effective option involves creating a simple API.</strong>

# COMMAND ----------


