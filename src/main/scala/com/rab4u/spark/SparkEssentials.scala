package com.rab4u.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StructType}

trait SparkEssentials extends SparkSessionConfig {

  //THIS METHOD IS USED TO GET THE STATIC SCHEMA FROM A FILE
  def getSchemaFromFile(schema_file : String) : StructType = {

    // [EXAMPLE] val schema_txt = spark.sparkContext.wholeTextFiles("s3a://dwh-webtraffic/spark/isg_schema/raw_schema/iSGBaseSchema.json")
    val schema_txt = spark.sparkContext.wholeTextFiles(schema_file)
    val schema_str = schema_txt.take(1)(0)._2.replaceAll("\\s", "")
    DataType.fromJson(schema_str).asInstanceOf[StructType]

  }

  // GET THE COLUMN NAME FROM THE DATAFRAME
  def getColumnNames(df : DataFrame): Array[String] = {
    df.columns.map{x => x.toLowerCase}
  }

}
