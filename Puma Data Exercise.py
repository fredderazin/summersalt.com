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


