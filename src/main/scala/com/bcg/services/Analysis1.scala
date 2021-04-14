package com.bcg.services

import org.apache.spark.sql.{SparkSession,DataFrame}

class Analysis1 {

  def executeAnalysis(spark: SparkSession, inputPath: String, outputPath: String) = {
    val primaryPerson = spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(inputPath+"/Primary_Person_use.csv")

    import spark.implicits._

    val results = primaryPerson.filter($"PRSN_INJRY_SEV_ID" === "KILLED" && $"PRSN_GNDR_ID" === "MALE")
      .select("CRASH_ID")
      .distinct
      .count

    spark.sparkContext.parallelize(Seq(results)).coalesce(1).saveAsTextFile(outputPath+"/analysis1")
  }
}
