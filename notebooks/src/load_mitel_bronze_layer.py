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
  def __init__(self, config_file, pipeline_runtime, load_type):
    self.config_file = config_file
    self.pipeline_runtime = pipeline_runtime
    self.load_type = load_type
    
  def get_config_file(self):
    return self.config_file
  
  def get_pipeline_runtime(self): 
    return self.pipeline_runtime
  
  def get_load_type(self):
    return self.load_type

# COMMAND ----------

def write_records_to_bronze_layer(db_params, df, pipeline_runtime):
  '''
  Writes the data to parquet file in bronze layer
  '''
  file_path = create_file_path(Constant.BRONZE_CONTAINER, db_params, pipeline_runtime)
  try:
    file_writer("parquet", df, file_path)
  except Exception:
    traceback.print_exc()
    raise Exception("Error while writing to parquet file for " + file_path)

# COMMAND ----------

def read_records_from_sql_table(db_params):
  '''
  This will read the data from SQL Server and store it to bronze layer in parquet format.
  '''
  server = "jdbc:sqlserver://" + db_params.get_server() + ".toromont.com:1433;databaseName="
  try:
    return table_reader_jdbc(server, db_params.get_database(), db_params.get_schema() + "."  \
                                   + db_params.get_sql_server_table(), "datascienceazure", \
                                     db_params.get_password(), "table")
  except Exception:
    traceback.print_exc()
    raise Exception("Error while reading from JDBC for " + db_params.get_database() + "." \
                                                         + db_params.get_sql_server_table())

# COMMAND ----------

def load_from_sql_tables(server, tables, pipeline_params):
  '''
  This will do the full and initial load of the data to Data Lake
  '''
  password = dbutils.secrets.get(scope = "toromont-kv-secret", key = "datascienceazure")
  for table in tables:
    db_params = get_database_parameters(server, table, Constant.EXPECTED_TABLE_NAMESPACE_PARTS, \
                                              "datascienceazure", password)
    df = read_records_from_sql_table(db_params)
    write_records_to_bronze_layer(db_params, df, pipeline_params.get_pipeline_runtime())

# COMMAND ----------

def create_pipeline_parameters():
  # Read widget values sent from ADF pipeline
  config_file = get_stripped_param("config_file")
  pipeline_runtime = get_stripped_param("pipeline_runtime")
  load_type = get_stripped_param("load_type").lower()
  
  return PipelineParameter(config_file, pipeline_runtime, load_type) 

# COMMAND ----------

def update_data_lake(pipeline_params, load_type):
  '''
  This will update the data Lake with sql server data.
  '''
  config_file = pipeline_params.get_config_file()
  try:
    load = read_config(pipeline_params.get_config_file(), "linked_tables", load_type)
    for server, tables in load.items():
      load_from_sql_tables(server, tables, pipeline_params)
  
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
  
  update_data_lake(pipeline_params, "full_load" if load_type == "full-initial" else load_type)
  if load_type == "full-initial":
    update_data_lake(pipeline_params, "initial_load")

# COMMAND ----------

main()
