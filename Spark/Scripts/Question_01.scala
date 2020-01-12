// Databricks notebook source
// MAGIC %md  ####CMM705 Big Data Programming Coursework (Sep 2019)

// COMMAND ----------

// MAGIC %md ## Airbnb Singapore Spark
// MAGIC 
// MAGIC Analyze the flowing using Spark

// COMMAND ----------

// MAGIC %md #### 01. Percentage of owners who rent more than one property

// COMMAND ----------

// insert the data from csv
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.functions._

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

//drop null values
val totalHostsIds = rentalDf.filter(rentalDf("host_id").isNotNull).select("host_id").distinct().count()

// COMMAND ----------

//hosts that contains more than one rentals
rentalDf = rentalDf.where("calculated_host_listings_count >1").select("host_id").distinct()

// COMMAND ----------

//Count the number of hosts that contains more than one rentals
rentalDf = rentalDf.groupBy("host_id").count().agg(count("host_id").alias("count"))

// COMMAND ----------

// Convert the hosts numbers by 100 to produce percentage
val udf_host_percentage = udf((x:Int)=>{(x*100)/totalHostsIds.toDouble})

// COMMAND ----------

//show results by adding new value named percentage
rentalDf.withColumn("percentage",udf_host_percentage(rentalDf("count"))).show()
