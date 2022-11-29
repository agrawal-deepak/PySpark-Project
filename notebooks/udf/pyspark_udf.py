# Databricks notebook source
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType, ArrayType
import re
import pandas as pd
import traceback

# COMMAND ----------

@pandas_udf(ArrayType(StringType()))
def document_number_parser(rows, column):
  '''
  This UDF will parse the document numbers and fix the partial document numbers.
  '''
  def parse_doc_num(row, column):
    res= []
    try:
      # To handle the fixing of partial document numbers, we require this for loop.
      # because partial document numbers should only be fixed with their preceding 
      # full document number. We will loop over the document numbers and keep track
      # of the last document number.
      for item in row:
        doc_num = re.search("\d{2}[A-Z]\d{6}", item)
        if doc_num and ((len(item) == 9) or (len(item) > 9 and not item[9].isdigit())):
          res.append(doc_num.group(0))
        elif "partsdocumentnumber" in column and len(item) <= 6 and item.isdigit() and len(res) != 0:
          res.append(res[-1][:len(res[-1]) - len(item)] + "" + item)
      return res
    except Exception:
      traceback.print_exc()
      raise Exception("Error in parsing document number " + row)

  return pd.Series(map(lambda row, column : parse_doc_num(row, column), rows, column))
