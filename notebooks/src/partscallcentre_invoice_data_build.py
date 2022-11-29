# Databricks notebook source
from pyspark.sql.functions import to_date
import traceback

# COMMAND ----------

# MAGIC %run "../utility/util"

# COMMAND ----------

def partscallcentre_invoice_data_build_sp():
  '''
  This method returns dataframe returned by
  executing spark sql query
  '''
  return spark.sql(""" 
    SELECT LEFT(s.DocumentNumber, 9)               AS RootDocumentNumber,
           to_date(s.OpenDate)                     AS OpenDate,
           s.TotalParts                            AS PartSales,
           to_date(c.ContractStart)                AS ContractStart,
           to_date(c.ContractEnd)                  AS ContractEnd
    FROM   dwdb.ms_f_invoiceheader_history s
           LEFT JOIN appdb_equiplink.equipment e
                  ON s.Make = e.OriginalMakeCode 
                     AND s.SerialNumber = e.OriginalSerialNumber
           LEFT JOIN appdb_equiplink.conditionsummary c
                  ON e.EquipmentId = c.EquipmentId
    WHERE s.InvoiceType IN ('P', 'C')
          AND (s.DocumentNumber like '__C%' or s.DocumentNumber like '__R%' )

    UNION ALL

    SELECT LEFT(p.RefDocumentNo, 9)            AS RootDocumentNumber,
           h.OpenDate,
           p.UnitSell * p.InvoiceQty           AS PartSales,
           to_date(c.ContractStart)            AS ContractStart,
           to_date(c.ContractEnd)              AS ContractEnd
    FROM dwdb.wo_f_workorder_partdetail p
         INNER JOIN dwdb.wo_f_workorder_header h
                 ON p.WONo = h.WONo
         LEFT JOIN appdb_equiplink.equipment e
                ON h.EquipManufCode = e.OriginalMakeCode
                   AND h.SerialNo = e.OriginalSerialNumber
         LEFT JOIN appdb_equiplink.conditionsummary c
                ON e.EquipmentId = c.EquipmentId                
    WHERE p.HdrSegInvoiceIndicator IS NULL
          AND p.RefDocumentNo LIKE '__S%'  """)

# COMMAND ----------

def main():
  '''
  main method executes the query and write the result in delta table format
  '''
  try:
    df = partscallcentre_invoice_data_build_sp()
  except Exception as e:
    print("Error while executing the stored procedure partscallcentre_invoice_data_build_sp.")
    traceback.print_exc()
    return 
    
  try:
    table_writer_delta(df, "appdb_equiplink", "partscallcentre_invoice_data", "overwrite")
  except Exception as e:
    print("Error while writing to Delta Table appdb_equiplink.partscallcentre_invoice_data")
    traceback.print_exc()
    return

# COMMAND ----------

main()
