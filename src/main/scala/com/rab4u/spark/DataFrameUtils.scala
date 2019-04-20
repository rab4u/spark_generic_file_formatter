package com.rab4u.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, DataType, ArrayType}

object DataFrameUtils {

  private def dropSubColumn(col: Column, colType: DataType, fullColName: String, dropColName: String): Option[Column] = {
    if (fullColName.equals(dropColName)) {
      None
    } else if (dropColName.startsWith(s"$fullColName.")) {
      colType match {
        case colType: StructType =>
          Some(struct(
            colType.fields
              .flatMap(f =>
                dropSubColumn(col.getField(f.name), f.dataType, s"$fullColName.${f.name}", dropColName) match {
                  case Some(x) => Some(x.alias(f.name))
                  case None => None
                })
              : _*))
        case colType: ArrayType =>
          colType.elementType match {
            case innerType: StructType =>
              Some(struct(innerType.fields
                .flatMap(f =>
                  dropSubColumn(col.getField(f.name), f.dataType, s"$fullColName.${f.name}", dropColName) match {
                    case Some(x) => Some(x.alias(f.name))
                    case None => None
                  })
                : _*))
          }

        case other => Some(col)
      }
    } else {
      Some(col)
    }
  }

  protected def dropColumn(df: DataFrame, colName: String): DataFrame = {
    df.schema.fields
      .flatMap(f => {
        if (colName.startsWith(s"${f.name}.")) {
          dropSubColumn(col(f.name), f.dataType, f.name, colName) match {
            case Some(x) => Some((f.name, x))
            case None => None
          }
        } else {
          None
        }
      })
      .foldLeft(df.drop(colName)) {
        case (df, (colName, column)) => df.withColumn(colName, column)
      }
  }

  /**
    * Extended version of DataFrame that allows to operate on nested fields
    */
  implicit class ExtendedDataFrame(df: DataFrame) extends Serializable {
    /**
      * Drops nested field from DataFrame
      *
      * @param colName Dot-separated nested field name
      */
    def dropNestedColumn(colName: String): DataFrame = {
      DataFrameUtils.dropColumn(df, colName)
    }

    def dropMultipleColumns(dropList: Array[String]): DataFrame = {
      dropList.foldLeft(df)((df, col) => df.dropNestedColumn(col))
    }
  }

}
