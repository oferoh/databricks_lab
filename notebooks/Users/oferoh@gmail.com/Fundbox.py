# Databricks notebook source
# MAGIC %md
# MAGIC Example for Fundbox

# COMMAND ----------

# DBTITLE 1,Load From Files
# MAGIC %python
# MAGIC file_location = "/FileStore/tables/FL_insurance_sample.csv"
# MAGIC file_type = "csv"
# MAGIC 
# MAGIC # CSV options
# MAGIC infer_schema = "true"
# MAGIC first_row_is_header = "true"
# MAGIC delimiter = ","
# MAGIC 
# MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
# MAGIC df = spark.read.format(file_type) \
# MAGIC   .option("inferSchema", infer_schema) \
# MAGIC   .option("header", first_row_is_header) \
# MAGIC   .option("sep", delimiter) \
# MAGIC   .load(file_location)
# MAGIC 
# MAGIC # display(df)
# MAGIC temp_table_name = "FL_insurance"
# MAGIC df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# DBTITLE 1,Query from temp table
# MAGIC %sql
# MAGIC select * from FL_insurance

# COMMAND ----------

# DBTITLE 1,Import
# MAGIC %scala
# MAGIC import org.apache.spark.sql.DataFrame

# COMMAND ----------

# DBTITLE 1,Connection to snowflake
# MAGIC %scala
# MAGIC // Use secret manager to get snowflake user name and password
# MAGIC val user = dbutils.secrets.get("snowflake", "snowflake-user")
# MAGIC val password = dbutils.secrets.get("snowflake", "snowflake-password")
# MAGIC 
# MAGIC // snowflake connection options
# MAGIC val options = Map("sfUrl" -> "https://lpa91641.snowflakecomputing.com",
# MAGIC                   "sfUser" -> user,
# MAGIC                   "sfPassword" -> password,
# MAGIC                   "sfDatabase" -> "OFER",
# MAGIC                   "sfSchema" -> "PUBLIC",
# MAGIC                   "sfWarehouse" -> "COMPUTE_WH") 
# MAGIC     

# COMMAND ----------

# MAGIC %scala
# MAGIC // Generate a simple dataset containing five values and write the dataset to Snowflake.
# MAGIC spark.sql("select * from FL_insurance  ").write
# MAGIC   .format("snowflake")
# MAGIC   .options(options)
# MAGIC   .option("dbtable", "FL_insurance")
# MAGIC   .mode(SaveMode.Overwrite) //.mode(SaveMode.Append)
# MAGIC   .save()

# COMMAND ----------

# DBTITLE 1,Read from snowflake
# MAGIC %scala
# MAGIC val df: DataFrame = spark.read
# MAGIC   .format("snowflake")
# MAGIC .options(options)
# MAGIC   .option("query","select statecode,county	,eq_site_limit	,hu_site_limit	,fl_site_limit from FL_insurance")
# MAGIC // .option("dbtable", "FL_INSURANCE_SAMPLE_CSV")
# MAGIC   .load()
# MAGIC  df.count()
# MAGIC  df.createOrReplaceTempView("FL_insurance_from_snowflake")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from FL_insurance_from_snowflake

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.table("FL_insurance_from_snowflake").count()

# COMMAND ----------

# MAGIC %python 
# MAGIC spark.table("FL_insurance_from_snowflake").repartition(4).write.mode("overwrite").parquet("/temp/fundbox/FL_insuranc/FL_insurance_from_snowflake")
# MAGIC  

# COMMAND ----------

# .repartition($"key",$"another_key")
spark.read.parquet("/temp/fundbox/FL_insuranc/FL_insurance_from_snowflake").createOrReplaceTempView("FL_insurance_from_local_disk")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from FL_insurance_from_snowflake  limit 100

# COMMAND ----------

