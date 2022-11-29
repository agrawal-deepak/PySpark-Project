-- AUGUSTO'S COMMENT
-- The new views are good but processing of invoices that way will take a long time.
-- As suggestions:
-- 1)	Need to change the proc part to do 2 separate inserts instead of 1 insert based on a UNION ALL.
-- 2)	This should be done incrementally. Reprocessing the entire dataset every time consumes a lot of resources. Once an Invoice was persisted to your [datascience].[PartsCallCentre_invoice_data] table it doesn’t need to be reprocessed.


USE [DWDB]
GO

CREATE OR ALTER VIEW dbo.[MS_F_InvoiceHeader_PartsDocument]
AS

SELECT LEFT(s.DocumentNumber, 9)  AS RootDocumentNumber,
       s.OpenDate,
       s.TotalParts               AS PartSales,
       c.ContractStart,
       c.ContractEnd
FROM   dbo.MS_F_InvoiceHeader_History s
       LEFT JOIN Appdb_EquipLink.AI.Equipment e
              ON s.Make = e.OriginalMakeCode
                 AND s.SerialNumber = e.OriginalSerialNumber
       LEFT JOIN Appdb_EquipLink.AI.ConditionSummary c
              ON e.EquipmentId = c.EquipmentId
WHERE  s.InvoiceType IN ('P', 'C')
       AND s.DocumentNumber LIKE '__[CR]%'

GO

CREATE OR ALTER VIEW dbo.[WO_F_WorkOrder_PartsDocument]
AS

SELECT LEFT(p.RefDocumentNo, 9)        AS RootDocumentNumber,
       h.OpenDate,
       p.UnitSell * p.InvoiceQty       AS PartSales,
       c.ContractStart,
       c.ContractEnd
FROM   DWDB.dbo.WO_F_WorkOrder_PartDetail p
       INNER JOIN DWDB.dbo.WO_F_WorkOrder_Header h
               ON p.WONo = h.WONo
       LEFT JOIN Appdb_EquipLink.AI.Equipment e
              ON h.EquipManufCode = e.OriginalMakeCode
                 AND h.SerialNo = e.OriginalSerialNumber
       LEFT JOIN Appdb_EquipLink.AI.ConditionSummary c
              ON e.EquipmentId = c.EquipmentId
WHERE  p.HdrSegInvoiceIndicator IS NULL
       AND p.RefDocumentNo LIKE '__S%'

GO

-- This is the build

USE [Appdb_EquipLink]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [datascience].[PartsCallCentre_Invoice_Data_Build]

DROP TABLE
IF EXISTS [datascience].[PartsCallCentre_Invoice_Data]
    ;

WITH Invoice_Table
     AS (
         SELECT RootDocumentNumber,
                OpenDate,
                PartSales,
                ContractStart,
                ContractEnd
         FROM   DWDB.dbo.MS_F_InvoiceHeader_PartsDocument
         UNION ALL
         SELECT RootDocumentNumber,
                OpenDate,
                PartSales,
                ContractStart,
                ContractEnd
         FROM   DWDB.dbo.WO_F_WorkOrder_PartsDocument
     )

SELECT i.RootDocumentNumber,
       CAST(i.OpenDate AS DATE) [OpenDate],
       i.PartSales,
       i.ContractStart,
       i.ContractEnd
INTO   [datascience].[PartsCallCentre_Invoice_Data]
FROM   Invoice_Table i
GO
