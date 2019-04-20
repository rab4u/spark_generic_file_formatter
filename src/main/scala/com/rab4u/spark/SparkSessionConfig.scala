package com.rab4u.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


trait SparkSessionConfig {

  def appName: String
  def logLevel : String
  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName(appName)
    .enableHiveSupport()
    .getOrCreate()

  // Logging
  @transient lazy val log: Logger = org.apache.log4j.LogManager.getLogger(appName)
  spark.sparkContext.setLogLevel(logLevel)

}

