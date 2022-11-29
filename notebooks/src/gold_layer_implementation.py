# Databricks notebook source
# Disclaimer: 
# The following code has been excessively commented for educational/demonstration purposes.
# In regular code, please follow a "clean code" approach to commenting instead.

# COMMAND ----------

import traceback
from configparser import NoSectionError
from itertools import chain
from pyspark.sql.functions import create_map, lit, concat, when, to_date, substring

# COMMAND ----------

# MAGIC %run "../utility/util"

# COMMAND ----------

# MAGIC %run "../constants/constant"

# COMMAND ----------

def get_filter_list(filter_obj):
  '''
  Returns a list of agent id or queue id or extension id according to 
  filter col
  '''
  df1 = table_reader_delta("ccmdata", filter_obj.get_table1())
  df2 = table_reader_delta("ccmdata", filter_obj.get_table2()).where(col("reporting") \
                                                .isin(filter_obj.get_reporting_list()))
  result_df = df1.join(df2, df1[filter_obj.get_table1_join_key()] == df2[filter_obj.get_table2_join_key()], \
                       "inner").select(filter_obj.get_filter_col()).distinct()
  return [item[0] for item in result_df.select(filter_obj.get_filter_col()).collect()]

# COMMAND ----------

def get_lookup_data_as_map(lookup_file, option):
  '''
  Returns lookup data as map 
  '''
  try:
    lookup_data = read_config(lookup_file, "lookup", option)
  except FileNotFoundError:
    traceback.print_exc()
    raise Exception("lookup file " + lookup_file + " does not exist.")
  except NoSectionError:
    traceback.print_exc()
    raise Exception("Section lookup does not exist in lookup file.")
  return create_map([lit(x) for x in chain(*lookup_data.items())])

# COMMAND ----------

def agent_first_last_name_lookup(df, lookup_file):
  '''
  Agent first name and Last name lookup to get pop name
  '''
  mapping_expr = get_lookup_data_as_map(lookup_file, "agent_first_last_name")
  return df.withColumn('pop_name', mapping_expr[concat(df['agentfirstname'], lit(''), df['agentlastname'])])

# COMMAND ----------

def agent_first_name_lookup(df, lookup_file):
  '''
  Agent first name lookup to get the branch
  '''
  mapping_expr = get_lookup_data_as_map(lookup_file, "agent_first_name")
  return df.withColumn('branch', mapping_expr[substring(df['agentfirstname'], 0, 4)])

# COMMAND ----------

def branch_name_lookup(df, lookup_file):
  '''
  Branch name lookup to get region
  '''
  mapping_expr = get_lookup_data_as_map(lookup_file, "branch_name")
  return df.withColumn('region', mapping_expr[df['branchname']])

# COMMAND ----------

def queue_name_lookup(df, lookup_file):
  '''
  Queue name lookup to get Branch 
  '''
  mapping_expr = get_lookup_data_as_map(lookup_file, "queue_name")
  return df.withColumn('branch', mapping_expr[df['queuename']])

# COMMAND ----------

def get_date_from_timestamp(df, col_name):
  '''
  Extracts and returns date from timestamp column
  '''
  try:
    return df.withColumn("date", to_date(col(col_name), "dd-MM-yyyy"))
  except Exception:
    traceback.print_exc()
    raise Exception("Error while extracting date from " + col_name)

# COMMAND ----------

def customer_account_transformation(df):
  '''
  Performs transformation on table customer_account.
  Transformation: Replace NULL with 0
  '''
  try:
    return df.where("Internal == 0") \
             .fillna({'expenditure_5yavg_total': 0, 'expenditure_5yavg_partsnservices': 0})
  except Exception:
    traceback.print_exc()
    raise Exception("Error while performing customer_account_transformation")

# COMMAND ----------

def join_key_lookup(df, lookup_file):
  '''
  name lookup to get join key
  '''
  mapping_expr = get_lookup_data_as_map(lookup_file, "join_key")
  return df.withColumn('join_key', mapping_expr[df['name']]) \
           .fillna({'join_key': "not found"})

# COMMAND ----------

def tblconfig_queuegroup_transformation(df, lookup_file):
  '''
  Performs transformation on table tblconfig_queuegroup.
  Transformation: Join key transformation 
  '''
  try:
    return join_key_lookup(df, lookup_file)
  except Exception:
    traceback.print_exc()
    raise Exception("Error while performing tblconfig_queuegroup_transformation")

# COMMAND ----------

def tbldata_extension_performance_by_period_transformation(df):
  '''
  Performs transformation on table tbldata_extension_performance_by_period.
  Transformation: Date transformation 
  '''
  try:
    filter_obj = GoldLayerFilter("tblconfig_extensiongroupmembers", "tblconfig_extensiongroup", \
                               "fkextensiongroup", "pkey", "fkextension", ['##502'])
    extension_list = get_filter_list(filter_obj)
    df = df.where(col("fkextension").isin(extension_list))
    return get_date_from_timestamp(df, "midnightstartdate")
  except Exception:
    traceback.print_exc()
    raise Exception("Error while performing tbldata_extension_performance_by_period_transformation.")

# COMMAND ----------

def global_branch_address_transformation(df, lookup_file):
  '''
  Performs transformation on table global_branch_address.
  Transformation: branch name lookup 
  '''
  try:
    df = df.withColumn("branch_pbi", when(col("branchname") == "Concord", "Concord H.O.") \
                                    .when(col("branchname") == "Brampton", "Power Systems") \
                                    .when(col("branchname") == "Sault Ste. Marie", "Sault Ste Marie") \
                                    .otherwise(col("branchname")))
    return branch_name_lookup(df, lookup_file)    
  except Exception:
    traceback.print_exc()
    raise Exception("Error while performing tblconfig_queuegroup_transformation")

# COMMAND ----------

def agent_event_stats_transformation(df, lookup_file):
  '''
  Performs transformation on table agent_event_stats.
  Transformation: First name and Last name lookup, date transformation 
  '''
  try:
    filter_obj = GoldLayerFilter("tblconfig_agentgroupmembers", "tblconfig_agentgroup", \
                                 "fkagentgroup", "pkey", "fkagent", \
                                 ['520', '521', '522', '523', '524', '232'])
    agent_list = get_filter_list(filter_obj)
    df = df.where(col("agentid").isin(agent_list))    
    df = agent_first_last_name_lookup(df, lookup_file)
    df = agent_first_name_lookup(df, lookup_file)
    return get_date_from_timestamp(df, "midnightstartdate")
  except Exception:
    traceback.print_exc()
    raise Exception("Error while performing agent_event_stats_transformation")

# COMMAND ----------

def read_popup_data_from_silver_layer():
  '''
  Read records from table appdb_equiplink.partscallcentre_popup_data
  '''
  try:
    return table_reader_delta("appdb_equiplink", "partscallcentre_popup_data") \
                          .select(col("contact").alias("pop_name"))
  except Exception:
    traceback.print_exc()
    print("Error while reading from Delta Table appdb_equiplink.partscallcentre_popup_data")
    return

# COMMAND ----------

def write_unique_agent_names_to_gold_layer(unique_agent_name_df):
  '''
  Writes unique agent names to gold layer
  '''
  password = dbutils.secrets.get(scope = "toromont-kv-secret", key = "gold-layer-db-password")
  try:
    table_writer_jdbc(unique_agent_name_df, Constant.GOLD_LAYER_SERVER, Constant.GOLD_LAYER_DB, \
                                           "dbo.agent_name_unique", "datascienceazure", password)
  except Exception:
    traceback.print_exc()
    raise Exception("Error while writing to " + Constant.GOLD_LAYER_DB + ".agent_name_unique")

# COMMAND ----------

def unique_agent_name_creation(df):
  '''
  Performs union of popup data and agent data.
  Removes duplicate agent names.
  '''
  popup_df = read_popup_data_from_silver_layer()
  unique_agent_name_df = df.union(popup_df).distinct().where(col("pop_name").isNotNull())
  write_unique_agent_names_to_gold_layer(unique_agent_name_df)

# COMMAND ----------

def agent_performance_by_period_stats_transformation(df, lookup_file):
  '''
  Performs transformtion on table agent_performance_by_period_stats.
  Transformation: First name and Last name lookup, date transformation
  '''
  try:
    filter_obj = GoldLayerFilter("tblconfig_agentgroupmembers", "tblconfig_agentgroup", \
                                 "fkagentgroup", "pkey", "fkagent", \
                                 ['520', '521', '522', '523', '524', '232'])
    agent_list = get_filter_list(filter_obj)
    df = df.where(col("agentid").isin(agent_list))
    df = agent_first_last_name_lookup(df, lookup_file)
    df = agent_first_name_lookup(df, lookup_file)
    unique_agent_name_creation(df.select("pop_name"))
    return get_date_from_timestamp(df, "midnightstartdate")
  except Exception:
    traceback.print_exc()
    raise Exception("Error while performing agent_performance_by_period_stats_transformation")

# COMMAND ----------

def queue_performance_by_period_stats_transformation(df, lookup_file):
  '''
  Performs transformation on table queue_performance_by_period_stats.
  Transformation: Queue name lookup and date transformation
  '''
  try:
    filter_obj = GoldLayerFilter("tblconfig_queuegroupmembers", "tblconfig_queuegroup", \
                                 "fkqueuegroup", "pkey", "fkqueue", \
                                 ['11311', '11312', '11313', '11314', '11315', '11328'])
    queue_list = get_filter_list(filter_obj)
    df = df.where(col("queueid").isin(queue_list))
    df = queue_name_lookup(df, lookup_file)
    return get_date_from_timestamp(df, "midnightstartdate")
  except Exception:
    traceback.print_exc()
    raise Exception("Error while performing queue_performance_by_period_stats_transformation")

# COMMAND ----------

def partscallcentre_popup_data_transformation(df):
  '''
  Performs transformation on table partscallcentre_popup_data.
  Transformation: New Column Contribution creation and date transform
  '''
  try:
    df = df.withColumn("contribution", when(col("objective") == "Sale with a part number", "Direct") \
                                      .when(col("objective") == "Sale without part number", "Direct") \
                                      .when(col("objective") == "Parts Sale", "Direct") \
                                      .otherwise(lit("Indirect")))
    return get_date_from_timestamp(df, "callstart")
  except Exception:
    traceback.print_exc()
    raise Exception("Error while performing tblconfig_queuegroup_transformation")

# COMMAND ----------

def apply_transformation(df, table_name, lookup_file):
  '''
  Based on table name, performs table specific transformation.
  '''
  if table_name == "customeraccount":
    return customer_account_transformation(df)
  elif table_name == "global_branch_address":
    return global_branch_address_transformation(df, lookup_file)
  elif table_name == "agenteventstats":
    return agent_event_stats_transformation(df, lookup_file)
  elif table_name == "agentperformancebyperiodstats":
    return agent_performance_by_period_stats_transformation(df, lookup_file)
  elif table_name == "queueperformancebyperiodstats":
    return queue_performance_by_period_stats_transformation(df, lookup_file)
  elif table_name == "tblconfig_queuegroup":
    return tblconfig_queuegroup_transformation(df, lookup_file)
  elif table_name == "tbldata_extensionperformancebyperiod":
    return tbldata_extension_performance_by_period_transformation(df)
  elif table_name == "partscallcentre_popup_data":
    return partscallcentre_popup_data_transformation(df)
  else:
    return df

# COMMAND ----------

def read_records_from_silver_layer(db_params):
  '''
  Read records from delta table in silver layer and returns as dataframe
  '''
  try:
    return table_reader_delta(db_params.get_database(), db_params.get_table())
  except Exception:
    traceback.print_exc()
    raise Exception("Error while reading from Delta Table " + db_params.get_database() + "." \
                                                            + db_params.get_table())

# COMMAND ----------

def write_records_to_gold_layer(df, db_params):
  '''
  Write records to SQL Server in gold layer
  '''
  password = dbutils.secrets.get(scope = "toromont-kv-secret", key = "gold-layer-db-password")
  try:
    return table_writer_jdbc(df, Constant.GOLD_LAYER_SERVER, Constant.GOLD_LAYER_DB, \
                                 Constant.GOLD_LAYER_SCHEMA + "." + db_params.get_table(), \
                                 "datascienceazure", password)
  except Exception:
    traceback.print_exc()
    raise Exception("Error while writing to " + Constant.GOLD_LAYER_DB + "." \
                                              + db_params.get_table())

# COMMAND ----------

def handle_gold_layer_transformation(table, lookup_file):
  '''
  Performs gold layer transformations.
  '''
  db_params = get_database_parameters("", table, Constant.EXPECTED_TABLE_NAMESPACE_PARTS, \
                                                "datascienceazure", "")
  df = read_records_from_silver_layer(db_params)
  transformed_df = apply_transformation(df, db_params.get_table(), lookup_file)
  write_records_to_gold_layer(transformed_df, db_params)

# COMMAND ----------

def main():
  # Setting spark properties. Partition specifies no. of partition to create after shuffle operation.
  # This also indicates the no of parallel tasks that spark run to perform operation.
  set_spark_properties(Constant.SPARK_SHUFFLE_PARTITION)
  config_file = get_stripped_param("config_file")
  lookup_file = get_stripped_param("lookup_file")
  
  try:
    tables = read_config(config_file, "gold_layer_tables", "table_list")
    for table in tables:
      handle_gold_layer_transformation(table, lookup_file)
      
  except FileNotFoundError:
    traceback.print_exc()
    raise Exception("Config file " + config_file + " does not exist.")
  except NoSectionError:
    traceback.print_exc()
    raise Exception("Section full_load does not exist in " + config_file)

# COMMAND ----------

main()
