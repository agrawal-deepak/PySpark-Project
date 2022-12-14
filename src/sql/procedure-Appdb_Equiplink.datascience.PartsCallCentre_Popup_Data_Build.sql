USE [AppDb_Equiplink]
GO
/****** Object:  StoredProcedure [datascience].[PartsCallCentre_Popup_Data_Build]    Script Date: 2020-07-05 12:12:48 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER   PROCEDURE [datascience].[PartsCallCentre_Popup_Data_Build]
AS
BEGIN

DELETE FROM datascience.PartsCallCentre_Popup_Data

INSERT INTO datascience.PartsCallCentre_Popup_Data

SELECT a.* FROM 

(

SELECT w.MeetingId,
       LTRIM(RTRIM(ISNULL(w.CustomerNo, '')))         AS CustomerNo,
       w.CustomerName,
       COALESCE(pced.PhoneNumber, w.CallerId)         AS CallerId,
       w.ContactName                                  AS Influencer,
       e.FirstName + ' ' + e.LastName                 AS Contact,
       c.PersSubText                                  AS Branch,
       w.Objective,
       w.PartsDocumentNumber,
       ISNULL(pced.Notes, '')                         AS Notes,
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
FROM   AppDb_SalesLink.dbo.SalesLink_WAP_Meetings w
       LEFT OUTER JOIN Xdb.dbo.X_User x
                    ON w.EnterXUId = x.UserId
       LEFT OUTER JOIN Appdb_Equiplink.dbo.AD_User_All e
                    ON x.LoginName = e.LoginName
                       AND x.DomainId = e.DomainId
       LEFT OUTER JOIN CorporateDB.dbo.Hr_CorpDir c
                    ON c.EmployeeNumber = e.SAPID
       LEFT OUTER JOIN ToromontCatDb.dbo.Global_Branch_Phone p
                    ON w.DialedNumber = p.PhoneNumber
       LEFT OUTER JOIN (SELECT MeetingId,
                               IsAfterhours,
                               BranchNo,
                               IsService,
                               IsUsedRestored,
                               Notes,
                               PhoneNumber
                        FROM   Appdb_Equiplink.dbo.PhoneCall_ExtDetails
                        WHERE  IsAgwest = 0) pced
                    ON w.MeetingId = pced.MeetingId
       LEFT OUTER JOIN (SELECT * FROM 
                            (SELECT BranchNo,
                                    BranchName,
                                    ROW_NUMBER() OVER (PARTITION BY BranchNo ORDER BY BranchName) AS row
                             FROM   ToromontCatDb.dbo.Global_Branch_Phone) AS b
                        WHERE row = 1) v 
                    ON v.BranchNo = pced.BranchNo
WHERE  PartsCallId IS NOT NULL
       AND YEAR(w.CallStart) >= '2017'

) a

END
