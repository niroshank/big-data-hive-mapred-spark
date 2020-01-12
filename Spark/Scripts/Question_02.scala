// Databricks notebook source
// MAGIC %md  ####CMM705 Big Data Programming Coursework (Sep 2019)

// COMMAND ----------

// MAGIC %md ## Airbnb Singapore Spark
// MAGIC 
// MAGIC Analyze the flowing using Spark

// COMMAND ----------

// MAGIC %md #### 02. Histogram of number of rentals reviewed over time (based on last_review) in mouth granularity

// COMMAND ----------

// insert the data from csv
import org.apache.spark.sql.functions.{col, to_date}

var upDF=spark.read
          .option("header", "true")
          .option("treatEmptyValuesAsNulls", "true")
          .option("mode","DROPMALFORMED")
          .option("delimiter", ",")
          .option("inferSchema", "true")
          .csv("/FileStore/tables/listings.csv")

// convert the string format to date in date columns
val df = upDF.columns.filter(colName =>colName.endsWith("_review"))
.foldLeft(upDF) { (outputDF, columnName) =>
outputDF.withColumn(columnName, to_date(col(columnName), "MM/dd/yyyy").cast("date"))
}

df.count

// COMMAND ----------

//write data into data frame
var rentalDf = df.toDF();

// COMMAND ----------

//drop null values
rentalDf = rentalDf.select("last_review").where("last_review IS NOT NULL")

// COMMAND ----------

//get year with month substring "yyyy-MM"
val udf_get_month = udf((x:String)=>x.slice(0,7))

// COMMAND ----------

//add new column called last_review_month
rentalDf = rentalDf.withColumn("last_review_month",udf_get_month(rentalDf("last_review")))

// COMMAND ----------

//group last review
//display count per month granularity
rentalDf.groupBy("last_review_month").count().alias("review_count").sort("last_review_month").count

// COMMAND ----------

//group last review and show count per month granularity
rentalDf.groupBy("last_review_month").count().alias("review_count").sort("last_review_month").show(61)

// COMMAND ----------



// COMMAND ----------


