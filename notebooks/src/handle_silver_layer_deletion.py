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

def find_deleted_primary_keys(primary_keys, bronze_df, silver_df):
  '''
  Find non-matching primary keys between bronze and silver layer
  '''
  return silver_df.select([col(pk) for pk in primary_keys]) \
                 .subtract(bronze_df.select([col(pk) for pk in primary_keys]))

# COMMAND ----------

def read_records_from_bronze_layer(db_params, pipeline_runtime):
  '''
  This will read the data from parquet file in bronze layer.
  '''
  file_path = create_file_path(Constant.BRONZE_CONTAINER + "/primary_keys", db_params, pipeline_runtime)
  try:
    return file_reader("parquet", file_path)  
  except Exception:
    traceback.print_exc()
    raise Exception("Error while reading from parquet file " + file_path)

# COMMAND ----------

def read_records_from_silver_layer(db_params, primary_keys):
  '''
  This will read the data from delta table in silver layer
  '''
  try:
    return table_reader_delta(db_params.get_database(),  db_params.get_table(), primary_keys)
  except Exception:
    traceback.print_exc()
    raise Exception("Error while reading from Delta Table " + db_params.get_database() + "." \
                                                            +  db_params.get_table())

# COMMAND ----------

def get_join_condition(primary_keys):
  '''
  This will return the join condition to be used in delete query
  '''
  return "".join(["a." + pk + " = " + pk + " AND " for pk in primary_keys])[:-5]

# COMMAND ----------

def get_delete_query(db_params, primary_keys):
  '''
  Returns delete query which needs to be executed on delta table
  '''
  columns = ', '.join(primary_keys)
  return "DELETE FROM " + db_params.get_database() + "." + db_params.get_table() + \
                        " a WHERE EXISTS (SELECT " + columns + " FROM " \
                        + db_params.get_table() + "_diff WHERE " + get_join_condition(primary_keys) \
                        + ")"

# COMMAND ----------

def execute_delete_query(db_params, primary_keys):
  '''
  This will execute the delete query on delta table
  '''
  try:
    spark.sql(get_delete_query(db_params, primary_keys))
    spark.catalog.refreshTable(db_params.get_database() + "." + db_params.get_table())
  except Exception:
    traceback.print_exc()
    raise Exception("Error while executing delete query on delta table. Query = " + query)

# COMMAND ----------

def handle_silver_layer_deletions(server, tables, pipeline_params):
  '''
  This function performs the record deletion from delta tables
  '''
  password = dbutils.secrets.get(scope = "toromont-kv-secret", key = "datascienceazure")
  for table in tables:
    db_params = get_database_parameters(server, 
                                        table, 
                                        Constant.EXPECTED_TABLE_NAMESPACE_PARTS, 
                                        "datascienceazure", 
                                        password)
    primary_keys = get_primary_keys(pipeline_params.get_config_file(), "call_center_tables") \
                              .get(db_params.get_table())
    if len(primary_keys) == 0:
      raise Exception("primary_keys not present for table " + db_params.get_table())

    bronze_df = read_records_from_bronze_layer(db_params, pipeline_params.get_pipeline_runtime())
    silver_df = read_records_from_silver_layer(db_params, primary_keys)
    find_deleted_primary_keys(primary_keys, bronze_df, silver_df) \
                        .createOrReplaceTempView(db_params.get_table() + Constant.DELETE_TABLE_SUFFIX)  
    execute_delete_query(db_params, primary_keys)

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
      handle_silver_layer_deletions(server, tables, pipeline_params)

  except FileNotFoundError:
    traceback.print_exc()
    raise Exception("Config file " + config_file + " does not exist.")
  except NoSectionError:
    traceback.print_exc()
    raise Exception("Section initial_load does not exist in " + config_file)

# COMMAND ----------

main()
