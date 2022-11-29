# Databricks notebook source
# Disclaimer: 
# The following code has been excessively commented for educational/demonstration purposes.
# In regular code, please follow a "clean code" approach to commenting instead.

# COMMAND ----------

import traceback

# COMMAND ----------

# MAGIC %run "../utility/util" 

# COMMAND ----------

# MAGIC %run "../constants/constant"

# COMMAND ----------

class PipelineParameter:
  '''
  PipelineParameter holds all parameters passed from ADF pipeline
  '''
  def __init__(self, 
               pipeline_runtime_parameter, 
               save_watermark_parameter, 
               watermark_table_parameter):
    
    self.pipeline_runtime_parameter = pipeline_runtime_parameter
    self.save_watermark_parameter = save_watermark_parameter
    self.watermark_table_parameter = watermark_table_parameter
    
  def get_pipeline_runtime_parameter(self): 
    return self.pipeline_runtime_parameter
  def get_save_watermark_parameter(self): 
    return self.save_watermark_parameter
  def get_watermark_table_parameter(self): 
    return self.watermark_table_parameter

# COMMAND ----------

def read_records(db_params, pipeline_runtime):
  '''
  Returns the dataframe from parquet file
  '''
  file_path = create_file_path(Constant.BRONZE_CONTAINER, db_params, pipeline_runtime)
  try:
    return file_reader("parquet", file_path)
  except Exception:
    traceback.print_exc()
    raise Exception("Error while reading from file.")

# COMMAND ----------

def get_merge_on_columns(primary_keys):
  '''
  Returns merge condition used in join clause of merge statement
  '''
  return "".join(["a." + column + " = b." + column + " AND " for column in primary_keys[:-1]]) \
                                + "a." + primary_keys[-1] + " = b." + primary_keys[-1]

# COMMAND ----------

def execute_merge(df, db_params, primary_keys):
  '''
  Execute the merge statement
  '''
  try:
    src_table = get_delta_table(Constant.SILVER_CONTAINER + "/" + db_params.get_database() \
                                                          + "/" + db_params.get_table())
    src_table.alias("a").merge(df.alias("b"), get_merge_on_columns(primary_keys)) \
                        .whenMatchedUpdateAll() \
                        .whenNotMatchedInsertAll() \
                        .execute()
  except Exception:
    traceback.print_exc()
    raise Exception("Error occured when performing merge operation for " + db_params.get_database() \
                                                                         + "." + db_params.get_table())

# COMMAND ----------

def watermark_update(table_name, pipeline_parameters):
  '''
  Updates watermark table with either pipeline runtime or end_watermark_value.
  '''
  try:
    if(pipeline_parameters.get_save_watermark_parameter() == "true"):
      update_watermark(pipeline_parameters.get_watermark_table_parameter(), 
                       table_name, 
                       pipeline_parameters.get_pipeline_runtime_parameter()) 
  except Exception:
    traceback.print_exc()
    raise Exception("Error occured when updating watermark for " + table_name)

# COMMAND ----------

def process_merge_data(tables, 
                       table_attributes, 
                       pipeline_parameters, 
                       table_name_to_primary_keys):
  '''
  Process merging data for list of tables
  '''
  password = dbutils.secrets.get(scope = "toromont-kv-secret", key = "datascienceazure")
  
  for table in tables:
    db_params = get_database_parameters(None, 
                                        table, 
                                        Constant.EXPECTED_TABLE_NAMESPACE_PARTS, 
                                        "datascienceazure", 
                                        password)
    df = read_records(db_params, pipeline_parameters.get_pipeline_runtime_parameter())
    df = columns_to_unix_timestamp(df, db_params.get_table(), table_attributes)
    primary_keys = table_name_to_primary_keys.get(db_params.get_table())
    execute_merge(df, db_params, primary_keys)
    watermark_update(db_params.get_table(), pipeline_parameters)

# COMMAND ----------

def create_pipeline_parameter():
  # Read widget values sent from ADF pipeline
  save_watermark_parameter = get_stripped_param("save_watermark_value")
  watermark_table_parameter = get_stripped_param("watermark_table")
  pipeline_runtime_parameter = get_stripped_param("pipeline_runtime")

  return PipelineParameter(pipeline_runtime_parameter, 
                           save_watermark_parameter, 
                           watermark_table_parameter)

# COMMAND ----------

def main():
  # Setting spark properties. Partition specifies no. of partition to create after shuffle operation.
  # This also indicates the no of parallel tasks that spark run to perform operation.
  set_spark_properties(Constant.SPARK_SHUFFLE_PARTITION)
  config_file = get_stripped_param("config_file")
  
  try:
    # read_config returns dictionary {<server_name>:<List of tables associated>}
    server_name_to_tables = read_config(config_file, 
                                     "call_center_tables", 
                                     "initial_load")
    # {table_name>:<List of primary keys>}
    table_name_to_primary_keys = get_primary_keys(config_file, "call_center_tables")
    table_attributes = create_table_attributes(config_file)    
    pipeline_parameters = create_pipeline_parameter()
    
    for server, tables in server_name_to_tables.items():
      process_merge_data(tables, 
                         table_attributes,
                         pipeline_parameters, 
                         table_name_to_primary_keys)
    
  except FileNotFoundError:
    traceback.print_exc()
    raise Exception("Config file " + config_file + " does not exist.")
  except NoSectionError:
    traceback.print_exc()
    raise Exception("Section initial_load does not exist.")

# COMMAND ----------

main()
