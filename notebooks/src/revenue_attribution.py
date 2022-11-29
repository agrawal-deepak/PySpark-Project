# Databricks notebook source
from pyspark.sql.functions import sum, min, max, col, when, concat_ws, collect_set, regexp_extract
import traceback

# COMMAND ----------

# MAGIC %run ../utility/util

# COMMAND ----------

def aggregate_invoice(df):
  '''
  Aggregate part sales based on root document number in invoice
  '''
  # Expression which derives onContract column value to be 1 if OpenDate is between ContractStart 
  # and ContractEnd, 0 otherwise
  on_contract = when(col('OpenDate').between(col('ContractStart'), col('ContractEnd')), 1).otherwise(0)
  df = df.groupBy('RootDocumentNumber') \
         .agg(sum('PartSales').alias('PartSales'), \
              min('OpenDate').alias('OpenDate'), \
              min('ContractStart').alias('ContractStart'), \
              max('ContractEnd').alias('ContractEnd')) \
         .withColumn('OnContract', on_contract)
  return df

# COMMAND ----------

def attribute_revenue(df):
  '''
  Merge document numbers with invoice data and calculate total part sales by meeting id
  '''
  # Expression to replace nulls with zero
  on_contract_null_to_zero = when(col('OnContract').isNull(), 0).otherwise(col('OnContract'))
  adf = aggregate_invoice(df)
  
  # specify the columns on which the tables are joined to avoid duplicate columns
  # spark collect_set method will remove nulls and duplicates
  df =  df.join(adf, ['RootDocumentNumber', 'PartSales', 'OpenDate', 'ContractStart', 'ContractEnd'], how='left') \
          .groupBy('MeetingId') \
          .agg(concat_ws(', ', collect_set('RootDocumentNumber')).alias('RootDocumentNumber'), \
               sum('PartSales').alias('PartSales'), \
               sum('OnContract').alias('OnContract')) \
          .withColumn('_OnContract', on_contract_null_to_zero).drop(col('OnContract')) \
          .withColumn('SalesType', regexp_extract(col('RootDocumentNumber'), '([CS])', 0))
  return df

# COMMAND ----------

def main():
  
  # Read data from Delta table
  try:
    parseddocumentnumber_df = table_reader_delta("appdb_equiplink", "partscallcentre_parseddocumentnumber")
    invoice_data_df = table_reader_delta("appdb_equiplink", "partscallcentre_invoice_data")
    joined_df = parseddocumentnumber_df.join(invoice_data_df, "RootDocumentNumber", how='left')
  except Exception as e:
    traceback.print_exc()
    raise Exception("Error while reading from Delta Table.")

  # Attributing revenue to calls
  try:
    result_df = attribute_revenue(df)
  except Exception as e:
    traceback.print_exc()
    print("Error while Attributing revenue to calls")
    return  
  
  # Store data to Delta table
  try:
    table_writer_delta(result_df, "appdb_equiplink", "partscallcentre_revenue", "overwrite")
    spark.catalog.refreshTable("appdb_equiplink.partscallcentre_revenue")
  except Exception as e:
    traceback.print_exc()
    raise Exception("Error while writing to Delta table appdb_equiplink.partscallcentre_revenue")

# COMMAND ----------

main()

# COMMAND ----------


