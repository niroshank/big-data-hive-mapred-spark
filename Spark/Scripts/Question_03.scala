// Databricks notebook source
// MAGIC %md  ####CMM705 Big Data Programming Coursework (Sep 2019)

// COMMAND ----------

// MAGIC %md ## Airbnb Singapore Spark
// MAGIC 
// MAGIC Analyze the flowing using Spark

// COMMAND ----------

// MAGIC %md #### 01. Percentage of owners who rent more than one property.

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

//convert the string format to date in date columns
val df = upDF.columns.filter(colName =>colName.endsWith("_review"))
.foldLeft(upDF) { (outputDF, columnName) =>
outputDF.withColumn(columnName, to_date(col(columnName), "MM/dd/yyyy").cast("date"))
}

//write data into data frame
var rentalDf = df.toDF();

// COMMAND ----------

//filter dataframe that availability equals 365
rentalDf = rentalDf.where("availability_365 = 365");

// COMMAND ----------

//create temporary view to store data
rentalDf.createOrReplaceTempView("Available356DaysView")

// COMMAND ----------

rentalDf=rentalDf.sqlContext.sql("SELECT neighbourhood, COUNT(*) AS rental_count FROM Available356DaysView" +
          " WHERE neighbourhood IN (SELECT neighbourhood FROM Available356DaysView GROUP BY neighbourhood ORDER BY avg(price)" +
          " LIMIT 5) GROUP BY neighbourhood")

rentalDf.show(10)
