# Databricks notebook source
from pyspark.sql.functions import col, lit, expr, to_utc_timestamp, to_timestamp
from configparser import ConfigParser
import ast
from delta.tables import DeltaTable
from functools import reduce

# COMMAND ----------

class DatabaseParameter:
  def __init__(self, server, database, schema, table, user, password):
    self.server = server
    self.database = database
    self.schema = schema
    self.table = table
    self.user = user
    self.password = password
  
  def get_server(self):
    return self.server
    
  def get_database(self):
    return self.database
    
  def get_schema(self):
    return self.schema
    
  def get_table(self):
    return self.table.replace("$","")
  
  def get_sql_server_table(self):
    return "[" + self.table + "]"
      
  def get_user(self):
    return self.user
    
  def get_password(self):
    return self.password

# COMMAND ----------

class WatermarkRange:
  def __init__(self, start_time, end_time):
    self.start_time = start_time
    self.end_time = end_time
    
  def get_start_time(self):
    return self.start_time
  
  def get_end_time(self):
    return self.end_time

# COMMAND ----------

class TableAttributes:
  def __init__(self, tablename_to_timestamp_columns, 
               tablename_to_datetimeoffset_columns):
    self.tablename_to_timestamp_columns = tablename_to_timestamp_columns
    self.tablename_to_datetimeoffset_columns = tablename_to_datetimeoffset_columns
    
  def get_tablename_to_timestamp_columns(self):
    return self.tablename_to_timestamp_columns
  def get_tablename_to_datetimeoffset_columns(self):
    return self.tablename_to_datetimeoffset_columns

# COMMAND ----------

class GoldLayerFilter:
  def __init__(self, table1, table2, table1_join_key, table2_join_key, filter_col, reporting_list):
    self.table1 = table1
    self.table2 = table2
    self.table1_join_key = table1_join_key
    self.table2_join_key = table2_join_key
    self.filter_col = filter_col
    self.reporting_list = reporting_list
  
  def get_table1(self):
    return self.table1
  
  def get_table2(self):
    return self.table2
  
  def get_table1_join_key(self):
    return self.table1_join_key
  
  def get_table2_join_key(self):
    return self.table2_join_key
  
  def get_filter_col(self):
    return self.filter_col  
  
  def get_reporting_list(self):
    return self.reporting_list

# COMMAND ----------

def create_table_attributes(config_file):
  try:
    # {table_name>:<List of timestamp columns>}
    tablename_to_timestamp_columns = read_config(config_file, 
                                            "change_timezone_tables", 
                                            "timestamp_tables")
    
    # {<table_name>:<List of timestamp columns>}
    tablename_to_datetimeoffset_columns = read_config(config_file, 
                                                 "change_timezone_tables", 
                                                 "datetimeoffset_tables")
    
    return TableAttributes(tablename_to_timestamp_columns, 
                           tablename_to_datetimeoffset_columns)
  except NoSectionError:
    print("Section does not exist.")
    traceback.print_exc()
    return

# COMMAND ----------

def set_spark_properties(partition):
  '''
  Set spark properties to avoid creation of meta parquet files.
  '''
  spark.conf.set("spark.sql.sources.commitProtocolClass", \
                 "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
  spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
  spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite","LEGACY")
  # spark optimization parameter to reduce default number of partitions
  spark.conf.set("spark.sql.shuffle.partitions", partition)

# COMMAND ----------

def file_reader(source, file_path, header=True, schema=True, multiline=True):
  '''
  Generic File Reader. Can read any text and binary files and returns the spark dataframe
  '''
  if source.lower() == "parquet":
    return spark.read.format("parquet").load(file_path)
  elif source.lower() == "csv":
    return spark.read.format("csv").option("header", header).option("inferSchema", schema) \
                                   .load(file_path)
  else:
    raise Exception("Invalid source type. Please specify a valid source type.")

# COMMAND ----------

def file_writer(target, df, file_path, save_mode="overwrite", part_cols=""):
  '''
  Generic File Writer. Can write any text and binary files
  '''
  if len(part_cols) > 0:
    df.write.format(target).mode(save_mode).partitionBy([col(x) for x in part_cols]) \
                           .save(file_path)
  else:
    df.repartition(Constant.PARTITION).write.format(target).mode(save_mode).save(file_path)

# COMMAND ----------

def table_reader_delta(db_name, table_name, columns=""):
  '''
  Delta Table Reader. Read the delta table and returns the spark dataframe
  '''
  if len(columns) > 0:
    return spark.table(db_name + "." + table_name).select([col(x) for x in columns])
  else:
    return spark.table(db_name + "." + table_name)

# COMMAND ----------

def table_reader_jdbc(server, db_name, table_name, user, passwd, reader_type, query=None):
  '''
  Generic JDBC Reader. Can read from any JDBC source and returns the spark dataframe
  '''
  if reader_type == "table":
    return spark.read.format("jdbc") \
                     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                     .option("url", server + db_name) \
                     .option("user", user) \
                     .option("password", passwd) \
                     .option("dbtable", table_name) \
                     .load()
  elif reader_type == "query":
    return spark.read.format("jdbc") \
                     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                     .option("url", server + db_name) \
                     .option("user", user) \
                     .option("password", passwd) \
                     .option("query", query) \
                     .load()

# COMMAND ----------

def table_writer_jdbc(df, server, db_name, table_name, user, passwd):
  '''
  Generic JDBC Writer. Can write to any JDBC supported database
  '''
  return df.write.format("jdbc") \
                 .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                 .option("url", server + db_name) \
                 .option("user", user) \
                 .option("password", passwd) \
                 .option("dbtable", table_name) \
                 .mode("overwrite") \
                 .save()

# COMMAND ----------

def table_writer_delta(df, db_name, table_name, save_mode, table_type="managed", file_path=None):
  '''
  Delta Table Writer. Writes to specified Delta table
  '''
  if table_type.lower() == "external":
    df.write.format("delta").mode(save_mode).option("overwriteSchema", "true") \
                            .option("path", file_path).saveAsTable(db_name + "." + table_name)
  elif table_type.lower() == "managed":
    df.write.format("delta").mode(save_mode).option("overwriteSchema", "true") \
                            .saveAsTable(db_name + "." + table_name)
  spark.catalog.refreshTable(db_name + "." + table_name)

# COMMAND ----------

def read_config(config_file, section, option):
  '''
  Read the python configuration file and returns the option as list
  '''
  with open(config_file, "r") as file:
    parser = ConfigParser() 
    parser.read(config_file)
    
  return ast.literal_eval(parser.get(section, option))

# COMMAND ----------

def get_delta_table(table_path):
  '''
  Delta Table Reader using Python API. Returns the DeltaTable object for a specified table
  '''
  return DeltaTable.forPath(spark, table_path)

# COMMAND ----------

def update_watermark(table_path, table, new_watermark_value):
  '''
  Update the watermark table in Delta lake
  '''
  try:
    deltaTable = get_delta_table(table_path)
    deltaTable.update(
      condition = col("table_name") == table,
      set = { "watermark_value": lit(new_watermark_value) } )
  except Exception:
    traceback.print_exc()
    raise Exception("Error occured when updating watermark for " + table)

# COMMAND ----------

def get_database_parameters(server, table, table_namespace_parts, user, password):
  '''
  returns the object of DatabaseParameters 
  '''
  # valid table format - <db_name>.<schema_name>.<table_name>
  table_parts = table.split(".")
  if len(table_parts) != table_namespace_parts:
    raise Exception("Config file is not in correct format " + table)
        
  return DatabaseParameter(server, \
                            table_parts[0].strip(), \
                            table_parts[1].strip(), \
                            table_parts[2].strip(), \
                            user, \
                            password)

# COMMAND ----------

def create_file_path(container_name, db_params, pipeline_runtime):
  '''
  If query end_time is 2020-09-01 14:20:55, then files are copied to
  /mnt/bronze/<db_name>/<table_name>/2020_09_01/2020_09_01-14_20_55/
  '''
  date_time_folder = pipeline_runtime.replace("-", "_").replace(":", "_") \
                                             .replace(" ","-")
  date_folder = date_time_folder.split("-")[0]
  return container_name + "/" + db_params.get_database() + "/" + db_params.get_table() + "/" \
                              + date_folder + "/" + date_time_folder

# COMMAND ----------

def columns_to_unix_timestamp(df, table_name, table_attributes):
  df = datetime_columns_to_unix_timestamp(df, table_name, table_attributes)
  df = datetimeoffset_columns_to_unix_timestamp(df, table_name, table_attributes)
  return df

# COMMAND ----------

def datetime_columns_to_unix_timestamp(df, table_name, table_attributes):
  '''
  This method converts datetime columns to unix timestamps based on the given table attributes
  '''
  column_list = table_attributes.get_tablename_to_timestamp_columns().get(table_name)
  if column_list:
    df = reduce(sql_datetime_to_unix_timestamp, column_list,  df)
  return df

# COMMAND ----------

def datetimeoffset_columns_to_unix_timestamp(df, table_name, table_attributes):
  '''
  This method converts datetimeoffset columns to unix timestamps based on the given table attributes
  '''
  column_list = table_attributes.get_tablename_to_datetimeoffset_columns().get(table_name)
  if column_list:
    df = reduce(sql_datetimeoffset_to_unix_timestamp, column_list,  df)
  return df

# COMMAND ----------

def sql_datetime_to_unix_timestamp(source_df, column_zoneid_tuple):
  '''
  Convert datetimes with the supplied timezone to unix timestamps
  column_zoneid_tuple - e.g. ("EnterDate", "EST")
  '''
  return source_df.withColumn(column_zoneid_tuple[0], to_utc_timestamp(column_zoneid_tuple[0], 
                                                                             column_zoneid_tuple[1]))

# COMMAND ----------

def sql_datetimeoffset_to_unix_timestamp(source_df, column_format_tuple):
  '''
  Convert datetimeoffset to UTC if you pass in a zone-offset
  column_format_tuple - e.g. ("ModifiedDate", "yyyy-MM-dd HH:mm:ss z")
  '''
  return source_df.withColumn(column_format_tuple[0], to_timestamp(column_format_tuple[0], 
                                                                       column_format_tuple[1]))

# COMMAND ----------

def get_stripped_param(widget_name):
  return dbutils.widgets.get(widget_name).strip()

# COMMAND ----------

def get_primary_keys(config_file, section):
  '''
  Returns a list of primary keys
  '''
  try:
    primary_keys = read_config(config_file, section, "primary_keys")
  except NoSectionError:
    traceback.print_exc()
    raise Exception("Section primary_keys does not exist in config file " + config_file)
  return primary_keys
