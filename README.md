# spark_generic_file_formatter
### INTRODUCTION
This is a simple spark utility which is used to convert any file format (JSON, CSV, TXT, SEQ, PARQUET, ORC, AVRO) to any file format (JSON, CSV, TXT, SEQ, PARQUET, ORC, AVRO). 
#### it supports :
1. exclusion columns in the Output (Supports Nested fields).
2. selection of specific columns
3. Query based selection
4. Controling output partitions
5. various types of compressions (Snappy, Gzip, Deflate, ..)

### HOW TO USE
STEP 1 : Clone the repo
STEP 2 : Build the JAR using maven package or open the project in the intellij and do the maven package from the maven life cycle.
STEP 3 : Run the JAR
'''
