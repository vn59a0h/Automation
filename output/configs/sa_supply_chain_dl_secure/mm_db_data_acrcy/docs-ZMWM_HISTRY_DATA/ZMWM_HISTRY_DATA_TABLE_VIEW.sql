CREATE VIEW  [dbo].[ZMWM_HISTRY_DATA_VW] AS
(
SELECT 
	[MANDT],
	[HUIDENT] ,
	[PICKER] ,
	[CHECKER] ,
	[STG_LANE],
	[CREATED] ,
	[MATID],
	[EAN11] ,
	[TIME] ,
	[LINE_CAT],
	[LGNUM] ,
	[SYSTEM_QTY],
	[SUOM],
	[COUNTED_QTY],
	[CUOM],
	[DIFFERENCE] ,
	[DUOM] ,
	CAST(CONCAT(
		SUBSTRING( cast([SLTTIME] as varchar(255)),1,4),'-',
		SUBSTRING( cast([SLTTIME] as varchar(255)),5,2),'-',
		SUBSTRING( cast([SLTTIME] as varchar(255)),7,2),' ',
		SUBSTRING( cast([SLTTIME] as varchar(255)),9,2),':',
		SUBSTRING( cast([SLTTIME] as varchar(255)),11,2),':',
		SUBSTRING( cast([SLTTIME] as varchar(255)),13,2)
		) AS VARCHAR(255)) DS_LOAD_START_TS
FROM [dbo].[ZMWM_HISTRY_DATA]
)



