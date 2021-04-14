package com.bcg.services

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

class Analysis3 {

  def executeAnalysis(spark: SparkSession, inputPath: String, outputPath: String) = {
    val primaryPerson = spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(inputPath+"/Primary_Person_use.csv")

    import spark.implicits._

    val countsDF = primaryPerson.filter($"PRSN_GNDR_ID" === "FEMALE")
      .groupBy("DRVR_LIC_STATE_ID")
      .count()
    val results = countsDF.orderBy(desc("count"))
      .first
      .getAs[String]("DRVR_LIC_STATE_ID")

    spark.sparkContext.parallelize(Seq(results)).coalesce(1).saveAsTextFile(outputPath+"/analysis3")
  }


}
