
/*
SPARK SCALA APP      : GenericFileFormatter.scala
CREATED BY           : Ravindra chellubani
CREATED DATE         : 2019-03-07
MODIFIED DATE        :
MODIFIED BY          :
DESCRIPTION          : This spark job is used convert one file format to other file format


RUN COMMANDS

spark-submit --class com.rab4u.spark.GenericFileFormatter generic-file-formatter-1.0-SNAPSHOT.jar
--APP_NAME "<APP NAME>"
--INPUT_PATH "<HDFS PATH/ S3 PATH / LOCAL FILE PATH>"
--INPUT_TYPE <json / csv / orc / parquet / txt / seq>
--OMIT_COLUMN_FILE "<HDFS PATH/ S3 PATH / LOCAL FILE PATH>/omit_columns.dat"
--OUTPUT_PATH "<HDFS PATH/ S3 PATH / LOCAL FILE PATH>"
--OUTPUT_TYPE <json / csv / orc / parquet / txt / seq>
--WRITE_MODE <overwrite / append>
--COMPRESSION <snappy>
--OUTPUT_PARTITIONS <350>

spark-submit --class com.rab4u.spark.GenericFileFormatter generic-file-formatter-1.0-SNAPSHOT.jar
--APP_NAME "<APP NAME>"
--INPUT_SCHEMA "<HDFS PATH/ S3 PATH / LOCAL FILE PATH>"
--INPUT_PATH "<HDFS PATH/ S3 PATH / LOCAL FILE PATH>"
--INPUT_TYPE <json / csv / orc / parquet / txt / seq>
--SELECT_QUERY_FILE "<HDFS PATH/ S3 PATH / LOCAL FILE PATH>/select_query.dat"
--OUTPUT_PATH "<HDFS PATH/ S3 PATH / LOCAL FILE PATH>"
--OUTPUT_TYPE <json / csv / orc / parquet / txt / seq>
--WRITE_MODE <overwrite / append>
--COMPRESSION <snappy>
--OUTPUT_PARTITIONS <350>
*/


package com.rab4u.spark

import org.apache.spark.sql.functions.col
import org.kohsuke.args4j.{CmdLineException, CmdLineParser}
import DataFrameUtils._
import org.apache.spark.sql.DataFrame

object GenericFileFormatter extends SparkSessionConfig with SparkEssentials {

  def appName: String = "GenericFileFormatter_" + CliArgs.app_name
  def logLevel: String = CliArgs.log_level

  def main(args:Array[String]) : Unit = {

    // PARSING CMD LINE ARGUMENTS
    log.info("[INFO] PARSING COMMAND LINE ARGUMENTS STARTED...")
    println("[INFO] PARSING COMMAND LINE ARGUMENTS STARTED...")
    val parser = new CmdLineParser(CliArgs)
    try {
      parser.parseArgument(args: _*)
    } catch {
      case e: CmdLineException =>
        print(s"[ERROR] Parameters Missing or Wrong \n:${e.getMessage}\n Usage:\n")
        parser.printUsage(System.out)
        System.exit(1)
    }
    log.info("[INFO] PARSING COMMAND LINE ARGUMENTS FINISHED SUCCESSFULLY")
    println("[INFO] PARSING COMMAND LINE ARGUMENTS FINISHED SUCCESSFULLY")

    // READING STATIC INPUT SCHEMA IF PROVIDED IN THE OPTIONS
    val input_schema = if (!CliArgs.input_schema.isEmpty) {
      log.info("[INFO] READING STATIC INPUT SCHEMA FROM : " + CliArgs.input_schema)
      println("[INFO] READING STATIC INPUT SCHEMA FROM :  " + CliArgs.input_schema)
      getSchemaFromFile(CliArgs.input_schema)
    } else {
      log.info("[INFO] NO STATIC INPUT SCHEMA SPECIFIED HENCE EXTRACTING DYNAMIC SCHEMA: ")
      println("[INFO] NO STATIC INPUT SCHEMA SPECIFIED HENCE EXTRACTING DYNAMIC SCHEMA : ")
      spark.read.format(CliArgs.input_type).load(CliArgs.input_path).schema
    }

    // READING INPUT DATA
    log.info("[INFO] READING INPUT DATA STARTED FROM " + CliArgs.input_path)
    println("[INFO] READING INPUT DATA STARTED FROM " + CliArgs.input_path)
    val ip_df : DataFrame= spark
      .read
      .schema(input_schema)
      .format(CliArgs.input_type)
      .load(CliArgs.input_path)
    log.info("[INFO] READING INPUT DATA STARTED FROM " + CliArgs.input_path + "FINISHED SUCCESSFULLY")
    println("[INFO] READING INPUT DATA STARTED FROM " + CliArgs.input_path + "FINISHED SUCCESSFULLY")

    // SELECT THE REQUIRED COLUMNS FROM THE INPUT DATA
    log.info("[INFO] SELECTING REQUIRED COLUMNS STARTED ...")
    println("[INFO] SELECTING REQUIRED COLUMNS STARTED ...")
    val required_df = if(! CliArgs.select_cols_file.isEmpty){
      val select_columns = spark.sparkContext.textFile(CliArgs.select_cols_file).collect()
      val colNames = select_columns.map(name => col(name))
      ip_df.select(colNames: _*)
    } else if (! CliArgs.omit_cols_file.isEmpty) {
      val omit_columns = spark.sparkContext.textFile(CliArgs.omit_cols_file).collect()
      ip_df.dropMultipleColumns(omit_columns)
    } else if (! CliArgs.select_qry_file.isEmpty){
      ip_df.createOrReplaceTempView("ip_df_tbl_"+CliArgs.envr)
      val qry_txt = spark.sparkContext.wholeTextFiles(CliArgs.select_qry_file)
      val qry_str = qry_txt.take(1)(0)._2
      spark.sql(qry_str)
    } else {
      ip_df
    }
    // && CliArgs.select_qry_file.isEmpty
    log.info("[INFO] SELECTING REQUIRED COLUMNS FINISHED SUCCESSFULLY...")
    println("[INFO] SELECTING REQUIRED COLUMNS FINISHED SUCCESSFULLY ...")


    // WRITE THE DATA TO THE OUPUT PATH
    log.info("[INFO] WRITING THE DATA TO THE OUPUT PATH : " + CliArgs.output_path)
    println("[INFO] WRITING THE DATA TO THE OUPUT PATH : " + CliArgs.output_path)
    val output_partitions = if (CliArgs.output_partitions == 0) {
      required_df.rdd.getNumPartitions
    } else {
      CliArgs.output_partitions
    }
    required_df
      .repartition(output_partitions)
      .write.format(CliArgs.output_type)
      .option("compression", CliArgs.compression)
      .mode(CliArgs.write_mode)
      .save(CliArgs.output_path)
    log.info("[INFO] WRITING THE DATA TO THE OUPUT PATH : " + CliArgs.output_path + " FINISHED SUCCESSFULLY")
    println("[INFO] WRITING THE DATA TO THE OUPUT PATH : " + CliArgs.output_path + " FINISHED SUCCESSFULLY")

  }

}
