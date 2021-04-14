package com.bcg.services

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.functions.col

class Analysis7 {

  def executeAnalysis(spark: SparkSession, inputPath: String, outputPath: String) = {
    val damages = spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(inputPath+"/Damages_use.csv")
      .filter(col("DAMAGED_PROPERTY").contains("NO DAMAGE"))

    val units = spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(inputPath+"/Units_use.csv")
      .filter(col("FIN_RESP_TYPE_ID").contains("INSURANCE") &&
        ((col("VEH_DMAG_SCL_1_ID").contains("DAMAGED") && col("VEH_DMAG_SCL_1_ID") > "DAMAGED 4")
          || (col("VEH_DMAG_SCL_2_ID").contains("DAMAGED") && col("VEH_DMAG_SCL_2_ID") > "DAMAGED 4")))

    import spark.implicits._

    val results = units.join(broadcast(damages),Seq("CRASH_ID"),"inner")
      .select("CRASH_ID")
      .distinct
      .count

    spark.sparkContext.parallelize(Seq(results)).coalesce(1).saveAsTextFile(outputPath+"/analysis7")
  }
}
