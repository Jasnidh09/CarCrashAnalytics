package com.bcg.services

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.expressions.Window.partitionBy

class Analysis5 {

  def executeAnalysis(spark: SparkSession, inputPath: String, outputPath: String) = {
    val primaryPerson = spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(inputPath+"/Primary_Person_use.csv")

    val units = spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(inputPath+"/Units_use.csv")

    import spark.implicits._

    val joinedDF = primaryPerson.join(units,Seq("CRASH_ID","UNIT_NBR"),"inner")
      .select("CRASH_ID","UNIT_NBR","VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID")

    val countsDF = joinedDF.groupBy("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID").count

    val results = countsDF.withColumn("rn",row_number()
      .over(partitionBy($"VEH_BODY_STYL_ID").orderBy($"count".desc)))
      .filter($"rn" === 1)
      .select("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID")

    results.coalesce(1).write.csv(outputPath+"/analysis5")
  }
}
