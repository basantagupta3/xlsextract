package com.home.xls

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

import scala.collection.mutable

object ReportGenerator {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf
      .setMaster("local[*]")
      .setAppName("sparkApp")

    val _sparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val path = getClass.getResource("/data/formatted_data.csv")
    var df = _sparkSession.read.format("com.databricks.spark.csv").options(Map(
      "header" -> "true",
      "delimiter" -> ",",
      "inferSchema" -> "true"
    )).load(path.getPath)

    df= df.withColumn("lead",lead("production",1,0).over(Window.partitionBy("year","item").orderBy("country")))

    df= df
      .withColumn("test",when(col("lead") === "0",col("production")).otherwise(col("lead")))
      .withColumn("test",col("test").cast(DoubleType))
      .withColumn("production",col("production").cast(DoubleType))
      .drop("lead")
      .withColumnRenamed("test","lead")
      .withColumn("us%",round(col("production")/col("lead")*100,2))
      .withColumn("country_item",concat_ws("_",col("country"),col("item")))
      .withColumn("us%",when(col("us%") === 100.0,col("production")).otherwise(col("us%")))
      //.drop("country")
      .drop("item")
      .drop("production")
      .drop("lead")
      .select("year","country_item","us%","country")
      .withColumn("country_item",when(col("country_item").contains("USA"),concat_ws("%",col("country_item"),lit(""))).otherwise(col("country_item")))

    val lst=df.filter(col("year") === "2016").select("country_item").collect()
      .map(t=>t.getAs[String](0)).toList.distinct

    var lstnew=new mutable.LinkedHashSet[String]()


    lstnew += "year"
    for(i <- 0 to lst.size/2 -1){
      lstnew += lst(i)
      lstnew += lst(i+12)
    }

   df=df.groupBy("year").pivot("country_item").agg(max(col("us%")))

    val colNames = lstnew.map(name => col(name)).toList

    val csvPath = getClass.getResource("/data").getPath+"/report.csv"
    df.coalesce(1).select(colNames : _*).orderBy("year")
      .write
      .option("header", "true")
      .option("sep","|")
      .csv(csvPath)

  }

}
