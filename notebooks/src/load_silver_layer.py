# Databricks notebook source
# Disclaimer: 
# The following code has been excessively commented for educational/demonstration purposes.
# In regular code, please follow a "clean code" approach to commenting instead.

# COMMAND ----------

import traceback
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
  def __init__(self, config_file, pipeline_runtime, load_type, watermark_table):
    self.config_file = config_file
    self.pipeline_runtime = pipeline_runtime
    self.load_type = load_type
    self.watermark_table = watermark_table
    
  def get_config_file(self):
    return self.config_file
  
  def get_pipeline_runtime(self): 
    return self.pipeline_runtime
  
  def get_load_type(self):
    return self.load_type
  
  def get_watermark_table(self):
    return self.watermark_table

# COMMAND ----------

def read_records_from_bronze_layer(db_params, pipeline_runtime):
  '''
  This will read the data from parquet file in bronze layer.
  '''
  file_path = create_file_path(Constant.BRONZE_CONTAINER, db_params, pipeline_runtime)
  try:
    return file_reader("parquet", file_path)  
  except Exception:
    traceback.print_exc()
    raise Exception("Error while reading from parquet file " + file_path)

# COMMAND ----------

def write_records_to_silver_layer(db_params, df):
  '''
  Writes the data to Delta Table
  '''
  try:
    table_writer_delta(df, db_params.get_database(), db_params.get_table(), "overwrite")
  except Exception:
    traceback.print_exc()
    raise Exception("Error while writing to Delta Table " + db_params.get_database() + "." \
                                                          + db_params.get_table())

# COMMAND ----------

def load_from_bronze_layer(server, tables, pipeline_params, load_type, table_attributes):
  '''
  This will do the full and initial load of data to Delta Lake
  '''
  password = dbutils.secrets.get(scope = "toromont-kv-secret", key = "datascienceazure")
  for table in tables:
    db_params = get_database_parameters(server, table, Constant.EXPECTED_TABLE_NAMESPACE_PARTS, \
                                              "datascienceazure", password)
    df = read_records_from_bronze_layer(db_params, pipeline_params.get_pipeline_runtime())
    df = columns_to_unix_timestamp(df, db_params.get_table(), table_attributes)
    write_records_to_silver_layer(db_params, df)
    if load_type == "initial_load":
      update_watermark(pipeline_params.get_watermark_table(), db_params.get_table(), \
                       pipeline_params.get_pipeline_runtime())

# COMMAND ----------

def create_pipeline_parameters():
  # Read widget values sent from ADF pipeline
  config_file = get_stripped_param("config_file")
  pipeline_runtime = get_stripped_param("pipeline_runtime")
  load_type = get_stripped_param("load_type").lower()
  watermark_table = get_stripped_param("watermark_table")
  
  return PipelineParameter(config_file, pipeline_runtime, load_type, watermark_table) 

# COMMAND ----------

def update_delta_lake(pipeline_params, load_type):
  '''
  This will update the Delta Lake with bronze layer data.
  '''
  config_file = pipeline_params.get_config_file()
  table_attributes = create_table_attributes(config_file)
  try:
    load = read_config(config_file, "call_center_tables", load_type)
    for server, tables in load.items():
      load_from_bronze_layer(server, tables, pipeline_params, load_type, table_attributes)
  
  except FileNotFoundError:
    traceback.print_exc()
    raise Exception("Config file " + config_file + " does not exist.")
  except NoSectionError:
    traceback.print_exc()
    raise Exception("Section " + load_type + " does not exist in config file " \
                               + config_file)

# COMMAND ----------

def main():
  # Setting spark properties. Partition specifies no. of partition to create after shuffle operation.
  # This also indicates the no of parallel tasks that spark run to perform operation.
  set_spark_properties(Constant.SPARK_SHUFFLE_PARTITION)
  pipeline_params = create_pipeline_parameters()
  load_type = pipeline_params.get_load_type()
  
  if load_type not in ["full_load", "initial_load", "full-initial"]:
    raise Exception("Invalid load type " + load_type)
  
  update_delta_lake(pipeline_params, "full_load" if load_type == "full-initial" else load_type)
  if load_type == "full-initial":
    update_delta_lake(pipeline_params, "initial_load")

# COMMAND ----------

main()
