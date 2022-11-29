# Databricks notebook source
from pyspark.sql.types import DateType, FloatType, ShortType, IntegerType
from pyspark.sql.functions import ltrim, rtrim, trim, year, month
import traceback

# COMMAND ----------

# MAGIC %run "../utility/util"

# COMMAND ----------

def partscallcentre_popup_data_build_sp():
  return spark.sql(""" SELECT w.MeetingId,
       TRIM(IFNULL(w.CustomerNo, ''))         AS CustomerNo,
       w.CustomerName,
       COALESCE(pced.PhoneNumber, w.CallerId)         AS CallerId,
       w.ContactName                                  AS Influencer,
       e.FirstName || ' ' || e.LastName                 AS Contact,
       c.PersSubText                                  AS Branch,
       w.Objective,
       w.PartsDocumentNumber,
       IFNULL(pced.Notes, '')                         AS Notes,
       w.DialedNumber,
       COALESCE(p.BranchNo, pced.BranchNo, '')        AS DialedBranchNo,
       COALESCE(p.BranchName, v.BranchName, '') AS DialedBranchName,
       CASE
            WHEN w.ActivityTypeId = 2 THEN 'Walk In'
            WHEN pced.IsAfterhours = 1 THEN 'Afterhours'
            WHEN pced.IsService = 1 THEN 'Service'
            WHEN pced.IsUsedRestored = 1 THEN 'Used/Restored'
            WHEN w.DialedNumber IS NULL THEN 'Direct Call'
            ELSE 'Queue Call'
       END                                            AS CallType,
       w.CallStart,
       w.EnterDateTime                                AS CallSaved
FROM   appdb_saleslink.saleslink_wap_meetings w
       LEFT OUTER JOIN xdb.x_user x
                    ON w.EnterXUId = x.UserId
       LEFT OUTER JOIN appdb_equiplink.ad_user_all e
                    ON x.LoginName = e.LoginName
                       AND x.DomainId = e.DomainId
       LEFT OUTER JOIN corporatedb.hr_corpdir c
                    ON c.EmployeeNumber = e.SAPID
       LEFT OUTER JOIN toromontcatdb.global_branch_phone p
                    ON w.DialedNumber = p.PhoneNumber
       LEFT OUTER JOIN (SELECT MeetingId,
                               IsAfterhours,
                               BranchNo,
                               IsService,
                               IsUsedRestored,
                               Notes,
                               PhoneNumber
                        FROM   appdb_equiplink.phonecall_extdetails
                        WHERE  IsAgwest = 0) pced
                    ON w.MeetingId = pced.MeetingId
       LEFT OUTER JOIN (SELECT * FROM 
                            (SELECT BranchNo,
                                    BranchName,
                                    ROW_NUMBER() OVER (PARTITION BY BranchNo ORDER BY BranchName) AS row
                             FROM   toromontcatdb.global_branch_phone) AS b
                        WHERE row = 1) v 
                    ON v.BranchNo = pced.BranchNo
WHERE  PartsCallId IS NOT NULL AND YEAR(w.CallStart) >= '2017' """)

# COMMAND ----------

def main():
  try:
    df = partscallcentre_popup_data_build_sp()
  except Exception as e:
    print("Error while executing the stored procedure partscallcentre_popup_data_build_sp.")
    traceback.print_exc()
    return 
  
  try:
    table_writer_delta(df, "appdb_equiplink", "partscallcentre_popup_data", "overwrite")
  except Exception as e:
    print("Error while writing to Delta Table appdb_equiplink.partscallcentre_popup_data")
    traceback.print_exc()
    return

# COMMAND ----------

main()
