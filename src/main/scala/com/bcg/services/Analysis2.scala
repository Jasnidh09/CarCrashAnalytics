package com.bcg.services

import org.apache.spark.sql.SparkSession

class Analysis2 {

  def executeAnalysis(spark: SparkSession, inputPath: String, outputPath: String) = {
    val units = spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(inputPath+"/Units_use.csv")

    import spark.implicits._

    val results = units.filter($"VEH_BODY_STYL_ID".contains("CYCLE"))
      .count

    spark.sparkContext.parallelize(Seq(results)).coalesce(1).saveAsTextFile(outputPath+"/analysis2")
  }

}
