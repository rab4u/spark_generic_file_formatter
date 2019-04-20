package com.rab4u.spark

import org.kohsuke.args4j.Option

object CliArgs {

  @Option(name = "--APP_NAME", required = true,
    usage = "PLEASE SPECIFY THE APP NAME / PROJECT NAME")
  var app_name: String = ""

  @Option(name = "--INPUT_SCHEMA", required = false,
    usage = "PLEASE SPECIFY THE INPUT SCHEMA FILE")
  var input_schema: String = ""

  @Option(name = "--INPUT_PATH", required = true,
    usage = "[REQUIRED] PLEASE PROVIDE THE INPUT PATH")
  var input_path: String = null

  @Option(name = "--SELECT_COLUMN_FILE", required = false,
    usage = "[REQUIRED] PLEASE PROVIDE SELECTED COLUMN FILE TO SELECT SPECIFIC COLUMNS FROM THE INPUT DATA SET")
  var select_cols_file: String = ""

  @Option(name = "--SELECT_QUERY_FILE", required = false,
    usage = "[REQUIRED] PLEASE PROVIDE THE SELECT QUERY TO SELECT SPECIFIC COLUMNS FROM THE INPUT DATA SET")
  var select_qry_file: String = ""

  @Option(name = "--OMIT_COLUMN_FILE", required = false,
    usage = "[REQUIRED] PLEASE PROVIDE THE OMITTED COLUMN TO EXCLUDE SPECIFIC COLUMNS FROM THE INPUT DATA SET")
  var omit_cols_file: String = ""

  @Option(name = "--INPUT_TYPE", required = false,
    usage = "[OPTIONAL] PLEASE PROVIDED INPUT TYPE IF IT IS NOT PARQUET (DEFAULT : PARQUET FORMAT")
  var input_type: String = "parquet"

  @Option(name = "--OUTPUT_PATH", required = true,
    usage = "[REQUIRED] PLEASE PROVIDE THE OUTPUT PATH")
  var output_path: String = null

  @Option(name = "--OUTPUT_TYPE", required = false,
    usage = "[OPTIONAL] PLEASE PROVIDED OUTPUT TYPE IF IT IS NOT PARQUET (DEFAULT : PARQUET FORMAT")
  var output_type: String = "parquet"

  @Option(name = "--WRITE_MODE", required = false,
    usage = "OUTPUT FILE WRITE MODE IS REQUIRED (append,overwrite)")
  var write_mode: String = "overwrite"

  @Option(name = "--COMPRESSION", required = false,
    usage = "COMPRESSION IS REQUIRED ( none, uncompressed, snappy, gzip, lzo)")
  var compression: String = "snappy"

  @Option(name = "--OUTPUT_PARTITIONS", required = false,
    usage = "[OPTIONAL] NO.OF OUTPUT PARTITIONS IS REQUIRED TO WRITE THE OUTPUT")
  var output_partitions: Int = 0

  @Option(name = "--VALIDATE_COUNTS", required = false,
    usage = "VALIDATE COUNTS FLAG")
  var validate_counts: String = "true"

  @Option(name = "--ENVR", required = false,
    usage = "PLEASE SPECIFY THE ENVIRONMENT")
  var envr: String = "dev"

  @Option(name = "--LOG-LEVEL", required = false,
    usage = "PLEASE SPECIFY THE LOG-LEVEL (DEFAULT : ERROR LEVEL")
  var log_level: String = "error"

}