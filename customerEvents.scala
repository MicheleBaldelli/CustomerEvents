// Databricks notebook source
// MAGIC %fs
// MAGIC ls /output/

// COMMAND ----------

// DBTITLE 1,Create dir for *.gz
// MAGIC %fs
// MAGIC mkdirs dbfs:/FileStore/tables/reachPLC

// COMMAND ----------

// DBTITLE 1,Create Output dir
// MAGIC %fs
// MAGIC mkdirs dbfs:/output/custEvents

// COMMAND ----------

// DBTITLE 1,Create dir for Spark's runtime checkpoint
// MAGIC %fs
// MAGIC mkdirs dbfs:/chkPoint/reachSOL

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, ArrayType, StringType, MapType}
import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.Trigger

// COMMAND ----------

// DBTITLE 1,Test Json Object
lazy val testJson = """{
"id":{
"val":"1b8894862edd304784fe5cc8043495e7",
"type":"cookie"
},
"country":"GB",
"region":"eur",
"events":[
{
"tap":"DEVICE",
"c":7101,
"ts":1605236167,
"add":[48690285,8851984,81151927,24730684]
},
{
"tap":"S2S",
"c":7101,
"ts":1605236775,
"remove":[1867303,24730684]
},
{
"tap":"S2S",
"src":"ldx",
"subSrc":"b36b81bf",
"ts":1605236775,
"add":[50663195,52272251,50663191,2020144],
"remove":[64702453,64702142,64702469,64702467]
}
]
}"""

// COMMAND ----------

lazy val sourcePath = "/FileStore/tables/reachPLC"  // Source folder for the .gz files
lazy val outputPath = "/output/custEvents"  // Folder for the parquet output
lazy val chkPointFolder = "/chkPoint/reachSOL"  // Checkpoint folder for the streaming

spark.conf.set("spark.sql.shuffle.partitions", "4")

// COMMAND ----------

// StructType to filter the unwanted columns
lazy val jsonSchema = new StructType()

  .add("id", new StructType()
         .add("type",StringType,true)
         .add("val",StringType,true)
       ,true)
    
  .add("country",StringType,true)
  .add("events", ArrayType(
    new StructType().add("add", ArrayType(IntegerType, true))
))

// Schem of the output dataset
case class CustomerEvents (customerId: String, addEvent: BigInt)

// COMMAND ----------

// test case
lazy val eventsDS = spark.read
  .schema(jsonSchema)
  .json(Seq(testJson).toDS)
  .filter("country = 'GB'")
  .withColumn("events", explode(col("events")))
  .withColumn("addEvent", explode(col("events.add")))
  .select(col("id.val").alias("customerID"), col("addEvent"))
  .as[CustomerEvents]

// COMMAND ----------

eventsDS.createOrReplaceTempView("events")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT 
// MAGIC  addEvent, count(CustomerID)
// MAGIC FROM events
// MAGIC GROUP BY addEvent

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT 
// MAGIC  *
// MAGIC FROM events
// MAGIC WHERE CustomerID = "1b8894862edd304784fe5cc8043495e7"

// COMMAND ----------

lazy val streamEventsDS = spark.readStream
  .format("json")
  .schema(jsonSchema)
  .option("maxFilesPerTrigger", 1)
  .option("checkpointLocation", chkPoint)
  .option("multiline", true)
  .load(sourcePath)
  .filter("country = 'GB'")
  .withColumn("events", explode(col("events")))
  .withColumn("addEvent", explode(col("events.add")))
  .select(col("id.val").alias("customerID"), col("addEvent"))
  .as[CustomerEvents]

streamEventsDS
  .coalesce(1)
  .writeStream
  .format("parquet")
  .outputMode("append")
  .option("path", outputPath)
  .partitionBy("addEvent")
  .trigger(Trigger.ProcessingTime("15 seconds"))
  .option("checkpointLocation", chkPoint)
  .start()



// COMMAND ----------

display(streamEventsDS.select("*"))

// COMMAND ----------


