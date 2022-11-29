# Databricks notebook source
# Disclaimer: 
# The following code has been excessively commented for educational/demonstration purposes.
# In regular code, please follow a "clean code" approach to commenting instead.

# COMMAND ----------

import traceback
from pyspark.sql.functions import col
from configparser import NoSectionError

# COMMAND ----------

# MAGIC %run "../utility/util"

# COMMAND ----------

# MAGIC %run "../constants/constant"

# COMMAND ----------

class PipelineParameter:
  '''
  PipelineParameter holds all parameters passed from ADF pipeline
  '''
  def __init__(self, config_file, pipeline_runtime):
    self.config_file = config_file
    self.pipeline_runtime = pipeline_runtime
    
  def get_config_file(self):
    return self.config_file
  
  def get_pipeline_runtime(self): 
    return self.pipeline_runtime

# COMMAND ----------

def write_records_to_bronze_layer(db_params, df, pipeline_runtime):
  '''
  Writes the data to parquet file
  '''
  file_path = create_file_path(Constant.BRONZE_CONTAINER + "/primary_keys", db_params, \
                               pipeline_runtime)
  try:
    file_writer("parquet", df, file_path)
  except Exception:
    traceback.print_exc()
    raise Exception("Error while writing to parquet file for " + file_path)

# COMMAND ----------

def get_select_query(db_params, pipeline_params):
  '''
  Returns the select query which needs to be executed on SQL Server
  '''
  primary_keys = get_primary_keys(pipeline_params.get_config_file(), "call_center_tables") \
                            .get(db_params.get_table())
  if len(primary_keys) == 0:
    raise Exception("primary_keys not present for table " + db_params.get_table())
  
  columns = ', '.join(primary_keys)
  return "SELECT " + columns + " FROM " + db_params.get_schema() + "." + db_params.get_table()

# COMMAND ----------

def read_records_from_sql_table(db_params, pipeline_params):
  '''
  Reads and returns only primary key columns from SQL Server
  '''
  query = get_select_query(db_params, pipeline_params)
  server = "jdbc:sqlserver://" + db_params.get_server() + ".toromont.com:1433;databaseName="
  try:
    return table_reader_jdbc(server, db_params.get_database(), db_params.get_table(), \
                             db_params.get_user(), db_params.get_password(), "query", query)
  except Exception:
    traceback.print_exc()
    raise Exception("Error while reading from JDBC for " + db_params.get_database() + "." \
                                                         + db_params.get_table())

# COMMAND ----------

def load_primary_key_fields_to_bronze_layer(server, tables, pipeline_params):
  '''
  This function copies all of the primary key field data to the bronze layer for 
  later use in determining which keys were deleted
  '''
  password = dbutils.secrets.get(scope = "toromont-kv-secret", key = "datascienceazure")
  for table in tables:
    db_params = get_database_parameters(server, 
                                        table, 
                                        Constant.EXPECTED_TABLE_NAMESPACE_PARTS, 
                                        "datascienceazure", 
                                        password)
    df = read_records_from_sql_table(db_params, pipeline_params)
    write_records_to_bronze_layer(db_params, df, pipeline_params.get_pipeline_runtime())

# COMMAND ----------

def create_pipeline_parameters():
  # Read widget values sent from ADF pipeline
  config_file = get_stripped_param("config_file")
  pipeline_runtime = get_stripped_param("pipeline_runtime")
  
  return PipelineParameter(config_file, pipeline_runtime) 

# COMMAND ----------

def main():
  # Setting spark properties. Partition specifies no. of partition to create after shuffle operation.
  # This also indicates the no of parallel tasks that spark run to perform operation.
  set_spark_properties(Constant.SPARK_SHUFFLE_PARTITION)
  pipeline_params = create_pipeline_parameters()
  config_file = pipeline_params.get_config_file()
  
  try:
    initial_load = read_config(config_file, "call_center_tables", "initial_load")
    for server, tables in initial_load.items():
      load_primary_key_fields_to_bronze_layer(server, tables, pipeline_params)

  except FileNotFoundError:
    traceback.print_exc()
    raise Exception("Config file " + config_file + " does not exist.")
  except NoSectionError:
    traceback.print_exc()
    raise Exception("Section initial_load does not exist in " + config_file)

# COMMAND ----------

main()
