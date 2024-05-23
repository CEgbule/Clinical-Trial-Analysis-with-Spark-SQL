-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Implementing the task into SQL

-- COMMAND ----------

-- Creating clinical_2020 table
CREATE TABLE IF NOT EXISTS clinical_2020
USING CSV
OPTIONS (
  header "true",
  inferSchema "true",
  delimiter "|",
  path "dbfs:/FileStore/table/clinicaltrial_2020.csv"
);


-- COMMAND ----------

-- View data from the clinical_2020 table
SELECT * FROM clinical_2020

-- COMMAND ----------

-- Creating clinical_2021 table
CREATE TABLE IF NOT EXISTS clinical_2021
USING CSV
OPTIONS (
  header "true",
  inferSchema "true",
  delimiter "|",
  path "dbfs:/FileStore/table/clinicaltrial_2021.csv"
);


-- COMMAND ----------

-- View data from the clinical_2021 table
SELECT * FROM clinical_2021

-- COMMAND ----------

-- Creating clinical_2023 table
CREATE TABLE IF NOT EXISTS clinical_2023
USING CSV
OPTIONS (
  header "true",
  inferSchema "true",
  path "dbfs:/FileStore/tables/clinicaltrial_2023_1.csv"
);


-- COMMAND ----------

-- Creating pharma table
CREATE TABLE IF NOT EXISTS pharma
USING CSV
OPTIONS (
  header "true",
  inferSchema "true",
  delimiter ",",
  path "dbfs:/FileStore/table/pharma.csv"
);

-- COMMAND ----------

-- View data from the pharma table
SELECT * FROM pharma

-- COMMAND ----------

-- View data from the clinical_2023 table
SELECT * FROM clinical_2023;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### QUESTION 1

-- COMMAND ----------

--verifying that distinct count has no duplicate
SELECT COUNT(distinct id) 
FROM clinical_2023

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####QUESTION 2

-- COMMAND ----------

-- Select the 'Type' column and count the frequency of each type
SELECT Type, COUNT(*) AS frequency
-- From the table 'clinical_2023'
FROM clinical_2023
-- Group the results by the 'Type' column
GROUP BY Type
-- Order the results by frequency in descending order
ORDER BY frequency DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### QUESTION 3

-- COMMAND ----------

-- Split the Conditions column by commas
WITH s_conditions AS (
    SELECT split(Conditions, '[|]') AS Conditions
    FROM clinical_2023
),
-- Explode the array of conditions to separate rows
exp_conditions AS (
    SELECT explode(Conditions) AS Conditions
    FROM s_conditions
)
-- Count the frequency of each condition
SELECT Conditions, COUNT(*) AS Frequency
FROM exp_conditions
GROUP BY Conditions
-- Order the results by frequency in descending order
ORDER BY Frequency DESC
-- Limit the results to the top 5 conditions by frequency
LIMIT 5;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### QUESTION 4

-- COMMAND ----------

-- Count the occurrences of each Sponsor where there is no matching Parent Company in the Pharma table, then order by frequency
SELECT Sponsor, COUNT('Sponsor') AS Freq FROM clinical_2023 
LEFT JOIN Pharma ON clinical_2023.Sponsor = pharma.Parent_Company 
WHERE pharma.Parent_Company IS NULL 
GROUP BY Sponsor 
ORDER BY Freq DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### QUESTION 5

-- COMMAND ----------

-- Filter out rows with completion dates in 2023 and status as "COMPLETED"
WITH CompletedStudies2023 AS (
    SELECT 
        SUBSTRING(Completion, 6, 2) AS Month,
        COUNT(*) AS StudyCount
    FROM clinical_2023
    WHERE Completion LIKE '2023-%' AND Status = 'COMPLETED'
    GROUP BY SUBSTRING(Completion, 6, 2)
    ORDER BY SUBSTRING(Completion, 6, 2)
)

-- Extract months and corresponding study counts for plotting
SELECT Month, StudyCount
FROM CompletedStudies2023;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Further Analysis Carried on SQL: Using SQL to explore which research areas each entity in the clinical_2023 table sponsors the most.

-- COMMAND ----------

-- Split the Conditions column by commas
WITH s_conditions AS (
    SELECT id, split(Conditions, '[|]') AS Conditions
    FROM clinical_2023
),
-- Explode the array of conditions to separate rows
exp_conditions AS (
    SELECT id, explode(Conditions) AS Condition
    FROM s_conditions
)
-- Count the frequency of each condition for each sponsor
SELECT c.Sponsor, ec.Condition, COUNT(*) AS Frequency
FROM exp_conditions ec
JOIN clinical_2023 c ON ec.id = c.id
GROUP BY c.Sponsor, ec.Condition
-- Order the results by sponsor and frequency in descending order
ORDER BY c.Sponsor, Frequency DESC;

