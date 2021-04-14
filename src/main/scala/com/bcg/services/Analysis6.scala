package com.bcg.services

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

class Analysis6 {

  def executeAnalysis(spark: SparkSession, inputPath: String, outputPath: String) = {
    val primaryPerson = spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(inputPath+"/Primary_Person_use.csv")

    val charges = spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(inputPath+"/Charges_use.csv")

    import spark.implicits._

    val joinedDF = primaryPerson.join(charges,Seq("CRASH_ID","UNIT_NBR","PRSN_NBR"),"inner")
      .select("CRASH_ID","UNIT_NBR","PRSN_NBR","CHARGE","DRVR_ZIP")
      .filter($"CHARGE".contains("ALCOHOL"))

    val results = joinedDF.groupBy("DRVR_ZIP").count()
      .orderBy(desc("count"))
      .limit(5)
      .select("DRVR_ZIP")

    results.coalesce(1).write.csv(outputPath+"/analysis6")
  }
}
