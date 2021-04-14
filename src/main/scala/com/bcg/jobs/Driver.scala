package com.bcg.jobs

import com.bcg.services.Analysis1
import com.bcg.services.Analysis2
import com.bcg.services.Analysis3
import com.bcg.services.Analysis4
import com.bcg.services.Analysis5
import com.bcg.services.Analysis6
import com.bcg.services.Analysis7
import com.bcg.services.Analysis8
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Driver {

  def main(args: Array[String]) = {
    val conf  = new SparkConf().setAppName("CarCrashAnalysis")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val inputPath = args(0)
    val outputPath = args(1)

    //Analysis 1
    val analysis1 = new Analysis1()
    analysis1.executeAnalysis(spark,inputPath,outputPath)

    //Analysis 2
    val analysis2 = new Analysis2()
    analysis2.executeAnalysis(spark,inputPath,outputPath)

    //Analysis 3
    val analysis3 = new Analysis3()
    analysis3.executeAnalysis(spark,inputPath,outputPath)

    //Analysis 4
    val analysis4 = new Analysis4()
    analysis4.executeAnalysis(spark,inputPath,outputPath)

    //Analysis 5
    val analysis5 = new Analysis5()
    analysis5.executeAnalysis(spark,inputPath,outputPath)

    //Analysis 6
    val analysis6 = new Analysis6()
    analysis6.executeAnalysis(spark,inputPath,outputPath)

    //Analysis 7
    val analysis7 = new Analysis7()
    analysis7.executeAnalysis(spark,inputPath,outputPath)

    //Analysis 8
    val analysis8 = new Analysis8()
    analysis8.executeAnalysis(spark,inputPath,outputPath)
  }
}
