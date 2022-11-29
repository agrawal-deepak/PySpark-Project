# Databricks notebook source
from pyspark.sql.functions import col, upper, length, split, explode, trim, lit
import traceback

# COMMAND ----------

# MAGIC %run ../udf/pyspark_udf

# COMMAND ----------

# MAGIC %run ../utility/util

# COMMAND ----------

def convert_to_upper(df):
  '''
  This will convert all the data to upper case.
  '''
  for col_name in df.columns:
    df = df.withColumn(col_name, upper(col_name))
  return df

# COMMAND ----------

# extract full and partial document numbers from PartsDocumentNumber
def extract_doc_num_from_pdn(df):
  '''
  This will extract the document numbers from partsdocumentnumber column. 
  This will also fix the partial document numbers.
  '''
  doc_num_df = df.filter(col("partsdocumentnumber").rlike("\d{2}[A-Z]\d{6}")) \
                 .withColumn("extracted_pdn", split(trim("partsdocumentnumber"), "\W+")) \
                 .drop("partsdocumentnumber")
  doc_num_extract_df = doc_num_df.withColumn("rootdocumentnumber", \
                       explode(document_number_parser("extracted_pdn", lit("partsdocumentnumber")))) \
                       .drop("extracted_pdn").filter(length("rootdocumentnumber") != 0)
  return doc_num_extract_df

# COMMAND ----------

def extract_doc_num_from_notes(df):
  '''
  This will extract the document numbers from notes column.
  '''
  doc_num_df = df.filter(col("notes").rlike("\d{2}[A-Z]\d{6}")) \
                 .withColumn("extracted_pdn", split(trim("notes"), "\W+")).drop("notes")
  doc_num_extract_df = doc_num_df.withColumn("rootdocumentnumber", \
                       explode(document_number_parser("extracted_pdn", lit("notes")))) \
                       .drop("extracted_pdn").filter(length("rootdocumentnumber") != 0)
  return doc_num_extract_df

# COMMAND ----------

def main():
  
  # Read data from Delta table
  try:
    raw_df = table_reader_delta("appdb_equiplink", "partscallcentre_popup_data") \
                          .select("meetingid", "partsdocumentnumber", "notes")
  except Exception:
    traceback.print_exc()
    print("Error while reading from Delta Table appdb_equiplink.partscallcentre_popup_data")
    return

  # Extract the document numbers from partsdocumentnumber and notes column
  try:
    df = convert_to_upper(raw_df)
    doc_num_extract_df = extract_doc_num_from_pdn(df.drop("notes"))
    notes_extract_df = extract_doc_num_from_notes(df.drop("partsdocumentnumber"))
    pdn_df = doc_num_extract_df.union(notes_extract_df)
  except Exception:
    traceback.print_exc()
    print("Error while extracting parsed document numbers")
    return  
  
  # Store data to Delta table
  try:
    table_writer_delta(pdn_df, "appdb_equiplink", "partscallcentre_partsdocumentnumber", "overwrite")
    spark.catalog.refreshTable("appdb_equiplink.partscallcentre_partsdocumentnumber")
  except Exception:
    traceback.print_exc()
    print("Error while writing to Delta table appdb_equiplink.partscallcentre_partsdocumentnumber")
    return

# COMMAND ----------

main()
