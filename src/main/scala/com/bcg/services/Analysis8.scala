package com.bcg.services

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions.col

class Analysis8 {

  def executeAnalysis(spark: SparkSession, inputPath: String, outputPath: String) = {

    val charges = spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(inputPath+"/Charges_use.csv")

    val speedChargesCrashes = charges.filter(col("CHARGE").contains("SPEED"))

    val units = spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(inputPath+"/Units_use.csv")

    val colors = units.groupBy("VEH_COLOR_ID").count()
      .orderBy(desc("count")).select("VEH_COLOR_ID")
      .limit(10).collect().map(_(0).toString).toList

    val coloredUnits = units.filter(col("VEH_COLOR_ID").isin(colors:_*))

    val primaryPerson = spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(inputPath+"/Primary_Person_use.csv")

    val states = primaryPerson.groupBy("DRVR_LIC_STATE_ID").count()
      .orderBy(desc("count"))
      .select("DRVR_LIC_STATE_ID")
      .limit(25).collect().map(_(0).toString).toList

    val licensedDrivers = primaryPerson.filter(col("DRVR_LIC_TYPE_ID") =!= "UNLICENSED")
      .filter(!col("DRVR_LIC_STATE_ID").isin(states:_*))

    import spark.implicits._

    val joinedDF = licensedDrivers.join(speedChargesCrashes,Seq("CRASH_ID","UNIT_NBR","PRSN_NBR"),"inner")
      .join(coloredUnits,Seq("CRASH_ID","UNIT_NBR"),"inner")
      .groupBy("VEH_MAKE_ID").count()

    val results = joinedDF.orderBy(desc("count")).select("VEH_MAKE_ID").limit(5)

    results.coalesce(1).write.csv(outputPath+"analysis8")
  }
}
