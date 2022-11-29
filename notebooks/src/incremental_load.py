# Databricks notebook source
# Disclaimer: 
# The following code has been excessively commented for educational/demonstration purposes.
# In regular code, please follow a "clean code" approach to commenting instead.

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql.functions import col
import traceback
from pytz import timezone

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
               last_watermark_parameter, 
               last_watermark_adjustment_parameter, 
               new_watermark_parameter, 
               watermark_table_parameter, 
               pipeline_runtime_parameter, 
               database_server_parameter):
    self.last_watermark_parameter = last_watermark_parameter
    self.last_watermark_adjustment_parameter = last_watermark_adjustment_parameter
    self.new_watermark_parameter = new_watermark_parameter
    self.watermark_table_parameter = watermark_table_parameter
    self.pipeline_runtime_parameter = pipeline_runtime_parameter
    self.database_server_parameter = database_server_parameter
    
  def get_last_watermark_parameter(self): 
    return self.last_watermark_parameter
  def get_last_watermark_adjustment_parameter(self): 
    return self.last_watermark_adjustment_parameter
  def get_new_watermark_parameter(self): 
    return self.new_watermark_parameter
  def get_watermark_table_parameter(self): 
    return self.watermark_table_parameter
  def get_pipeline_runtime_parameter(self): 
    return self.pipeline_runtime_parameter
  def get_database_server_parameter(self): 
    return self.database_server_parameter

# COMMAND ----------

def is_date_valid(date_str):
  '''
  Following function checks if the given date is in yyyy-MM-dd HH:mm:ss format
  '''
  try:
    datetime.strptime(date_str, Constant.YMD_HMS_DATETIME_FORMAT)
    return True
  except ValueError:
    return False

# COMMAND ----------

def get_stored_watermark(table_name, watermark_table):
  '''
  Retrieve watermark value from watermark_db.watermark_table 
  '''
  wt_list = watermark_table.split("/")    
  if len(wt_list) <= 1:
    raise Exception("Invalid watermark table path " + watermark_table)
      
  try:
    wdf = table_reader_delta(wt_list[-2], wt_list[-1])
    watermark_values = wdf.filter(col("table_name") == table_name) \
                           .select(col("watermark_value")).collect()
  except Exception:
    traceback.print_exc()
    raise Exception("Exception occured while retrieving last watermark value for " + table_name)
  
  if len(watermark_values) < 1:
    raise Exception("Data not found for table " + table_name)
                    
  return watermark_values[0][0].strftime(Constant.YMD_HMS_DATETIME_FORMAT)

# COMMAND ----------

def adjust_watermark(watermark, adjustment_in_minutes):
  '''
  adjusts last watermark table value
  '''
  if not adjustment_in_minutes.isdigit():
    raise Exception("Invalid last_watermark_adjustment_value")
  
  watermark_datetime = datetime.strptime(watermark, Constant.YMD_HMS_DATETIME_FORMAT)
  adjusted_watermark_datetime = watermark_datetime - \
                                timedelta(minutes = int(adjustment_in_minutes))
  return adjusted_watermark_datetime.strftime(Constant.YMD_HMS_DATETIME_FORMAT)

# COMMAND ----------

def get_adjusted_start_watermark(table_name, pipeline_params):
  '''
  check last_watermark_value received from pipeline parameter. 
  If not valid, retrieve from delta table,
  else check if date is valid and return converted datetime 
  '''
  last_watermark = pipeline_params.get_last_watermark_parameter()
  if not last_watermark:
    last_watermark = get_stored_watermark(table_name, pipeline_params.get_watermark_table_parameter())
    
  if not is_date_valid(last_watermark):
    raise Exception("Invalid last_watermark_value. Expected format 'yyyy-mm-dd hh:mm:ss'")
    
  return adjust_watermark(last_watermark, 
                          pipeline_params.get_last_watermark_adjustment_parameter())

# COMMAND ----------

def utc_datetime_to_est(pipeline_runtime):
  '''
  This function converts datetime string in utc to datetime string in est
  '''
  date_in_utc = datetime.strptime(pipeline_runtime, Constant.YMD_HMS_DATETIME_FORMAT)
  return date_in_utc.astimezone(timezone("US/Eastern")) \
                    .strftime(Constant.YMD_HMS_DATETIME_FORMAT)

# COMMAND ----------

def get_end_watermark(pipeline_runtime, new_watermark):
  '''
  returns end_time of window. 
  If new_watermark_value widget is blank then default to pipeline runtime. 
  Else the value provided
  '''
  end_watermark = new_watermark
  if not end_watermark:
    end_watermark = pipeline_runtime
  if not is_date_valid(end_watermark):
      raise Exception("Invalid end watermark: " + end_watermark + ". Expected format 'yyyy-mm-dd hh:mm:ss'")
  return end_watermark

# COMMAND ----------

def get_time_in_est(watermark_time, pipeline_input_time):
  '''
  change watermark start_time/end_time to EST in case start_time/end_time not provided in input
  '''
  if not pipeline_input_time:
    return utc_datetime_to_est(watermark_time)
  
  return watermark_time

# COMMAND ----------

def get_records_between_watermarks(db_params, watermark_range, pipeline_params):
  '''
  This method returns the records between start_time and end_time 
  '''
  watermark_start_time_in_est = get_time_in_est(watermark_range.get_start_time(), 
                                                pipeline_params.get_last_watermark_parameter())
  watermark_end_time_in_est = get_time_in_est(watermark_range.get_end_time(), 
                                              pipeline_params.get_new_watermark_parameter())
  
  query = "select * from " + db_params.get_schema() + "." + db_params.get_table() + " where ETLTimestamp between '" \
                             + watermark_start_time_in_est + "' and '" + watermark_end_time_in_est + "'"
  try:
    server = "jdbc:sqlserver://" + db_params.get_server() + ".toromont.com:1433;databaseName="
    return table_reader_jdbc(server, db_params.get_database(), db_params.get_table(), db_params.get_user(), db_params.get_password(), "query", query)
  except Exception:
    traceback.print_exc()
    raise Exception("Error while reading from JDBC for " + db_params.get_database() + "." + db_params.get_table())

# COMMAND ----------

def write_records(records, db_params, pipeline_runtime):
  '''
  Following function retrieves incremental data persisted in staging table
  in a window of time, and writes at a specific location
  '''
  file_path = create_file_path(Constant.BRONZE_CONTAINER, db_params, pipeline_runtime)
  try:
    file_writer("parquet", records, file_path)
  except Exception:
    traceback.print_exc()
    raise Exception("Error while writing to parquet file for " + file_path)

# COMMAND ----------

def copy_incremental_data(db_params, watermark_range, pipeline_params):
    '''
    Following function copies incremental data in a specific time period
    If pipeline runtime is 2020-09-01 14:20:55, then files are copied to
    /mnt/bronze/<db_name>/<table_name>/2020_09_01/2020_09_01-14_20_55/
    '''
    records = get_records_between_watermarks(db_params, watermark_range, pipeline_params)
    write_records(records, db_params, pipeline_params.get_pipeline_runtime_parameter())

# COMMAND ----------

def create_watermark_range(table_name, watermark_end_time, pipeline_params):
  '''
  Returns start_time and end_time between which records are fetched
  '''
  return WatermarkRange(start_time = get_adjusted_start_watermark(table_name, pipeline_params), 
                        end_time = watermark_end_time)

# COMMAND ----------

def process_incremental_data(server, tables, pipeline_params):
  '''
  This function process incremental data load for a list of tables
  '''
  password = dbutils.secrets.get(scope = "toromont-kv-secret", key = "datascienceazure")
  watermark_end_time = get_end_watermark(pipeline_params.get_pipeline_runtime_parameter(), 
                                         pipeline_params.get_new_watermark_parameter())
  for table in tables:
    db_params = get_database_parameters(server, 
                                        table, 
                                        Constant.EXPECTED_TABLE_NAMESPACE_PARTS, 
                                        "datascienceazure", 
                                        password)
    watermark_range = create_watermark_range(db_params.get_table(), 
                                             watermark_end_time, 
                                             pipeline_params)
    copy_incremental_data(db_params, watermark_range, pipeline_params)

# COMMAND ----------

def create_pipeline_parameter():
  # Read widget values sent from ADF pipeline
  last_watermark_parameter = get_stripped_param("last_watermark_value")
  last_watermark_adjustment_parameter = get_stripped_param("last_watermark_adjustment_value")
  new_watermark_parameter = get_stripped_param("new_watermark_value")

  watermark_table_parameter = get_stripped_param("watermark_table")
  pipeline_runtime_parameter = get_stripped_param("pipeline_runtime")
  database_server_parameter = get_stripped_param("database_server")
  
  return PipelineParameter(last_watermark_parameter, 
                           last_watermark_adjustment_parameter, 
                           new_watermark_parameter, 
                           watermark_table_parameter, 
                           pipeline_runtime_parameter, 
                           database_server_parameter)

# COMMAND ----------

def main():
  '''
  Main function which copies incremental load iteratively
  '''
  
  # Setting spark properties. Partition specifies no. of partition to create after shuffle operation.
  # This also indicates the no of parallel tasks that spark run to perform operation.
  set_spark_properties(Constant.SPARK_SHUFFLE_PARTITION)
  config_file = get_stripped_param("config_file")
  
  try:
    # read_config method returns dictionary {<server_name>:<List of tables associated>}
    server_tables_dict = read_config(config_file, "call_center_tables", "initial_load")
    # read all pipeline parameters
    pipeline_params = create_pipeline_parameter()
    
    for server, tables in server_tables_dict.items():
      process_incremental_data(server, tables, pipeline_params)
      
  except FileNotFoundError:
    traceback.print_exc()
    raise Exception("Config file " + config_file + " does not exist.")
  except NoSectionError:
    traceback.print_exc()
    raise Exception("Section initial_load does not exist.")

# COMMAND ----------

main()
