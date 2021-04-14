package com.bcg.services

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum,desc}

class Analysis4 {

  def executeAnalysis(spark: SparkSession, inputPath: String, outputPath: String) = {
    val units = spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(inputPath+"/Units_use.csv")

    import spark.implicits._

    val countsDF = units.groupBy("VEH_MAKE_ID")
      .agg(sum("TOT_INJRY_CNT").as("INJRY"),sum("DEATH_CNT").as("DEATH"))

    val results = countsDF.withColumn("TOTAL",$"INJRY"+$"DEATH")
      .orderBy(desc("TOTAL"))
      .limit(15)
      .orderBy("TOTAL")
      .limit(11)
      .select("VEH_MAKE_ID")

    results.coalesce(1).write.text(outputPath+"/analysis4")
  }

}
