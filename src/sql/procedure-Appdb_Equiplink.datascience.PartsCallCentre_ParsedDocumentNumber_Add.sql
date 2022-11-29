-- OLD SP, NOT VALID ANYMORE, FOR REFERENCE PURPOSE
USE [Appdb_Equiplink]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [datascience].[PartsCallCentre_ParsedDocumentNumber_Add]
             @MeetingId INT
            ,@RootDocumentNumber VARCHAR(10)
AS
BEGIN
	IF NOT EXISTS (SELECT 1 FROM [datascience].[PartsCallCentre_ParsedDocumentNumber]
					WHERE	MeetingId = @MeetingId
					        AND RootDocumentNumber = @RootDocumentNumber
	BEGIN
		INSERT INTO [datascience].[PartsCallCentre_ParsedDocumentNumber]
			   ( [MeetingId]
				,[RootDocumentNumber])
		VALUES
			   ( @MeetingId
				,@RootDocumentNumber)

	END
END
GO
