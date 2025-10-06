SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
OBJECTIVE:
Weekly Report, on Mondays:
    * Spend: weekly data (debit + credit), mon to sun, saved as friday
    * Inventory: data at the EOW (sunday) saved as friday.

WARNING: At the time of this development, there was no category available to group MainAccount,
and since this is also a variable value, it is included here as a hardcoded list.
If a DIM table were to be created for this, it would still need to be maintained by the user for changes.
The same situation applies to plantValue.

VERSION CONTROL:
Jul/2025: V1 Federico Juiz (based on ticket #7769 and TrialBalanceLog table)

*/

CREATE PROCEDURE [dbo].[usp_Populate_FactKPIWeeklyTrialBalance]
AS
BEGIN

    TRUNCATE TABLE [FACT].[KPIWeeklyTrialBalance];

    -- increment SQL query performance
    SET NOCOUNT ON;
    
    -- PROCESS:

    -- note:
    -- It is not possible to use DIM.Date because it uses a different week number logic;
    -- the week starts on Sunday, not Monday as required for this project.

    -- monday = 1
    SET DATEFIRST 1; 

    -- List of Plants for Location 18 when working with Inventory KPIs
    -- (!) this can be replaced by temp table or table VAR when available in Fabric
    WITH PlantAndBodegas18 AS ( 
        SELECT 'M01' AS Plants 
        UNION ALL SELECT 'M02' UNION ALL SELECT 'M03' UNION ALL SELECT 'M04' UNION ALL
        SELECT 'M05' UNION ALL SELECT 'M06' UNION ALL SELECT 'M07' UNION ALL 
        SELECT 'M08' UNION ALL SELECT 'M09' UNION ALL SELECT 'M10' UNION ALL
        SELECT 'M11' UNION ALL SELECT 'M12' UNION ALL SELECT 'M13' UNION ALL 
        SELECT 'M14' UNION ALL SELECT 'M15' UNION ALL SELECT 'M16' UNION ALL
        SELECT 'M17' UNION ALL SELECT 'M18'
    ),

    -- CTE Spend START
    FriWeekNum AS (
    SELECT DISTINCT
        DateID AS DateID_FridayTag
        ,DATEPART(WEEK, CAST(CONVERT(char(8), DateID) AS date)) AS WeekNumber
        ,YEAR(CAST(CONVERT(char(8), DateID) AS date)) AS YearNumber
    FROM [FACT].[TrialBalanceLog]
    WHERE
        FORMAT(CAST(CONVERT(char(8), DateID) AS date), 'dddd') = 'Friday'
    ),
    TBL_tagged AS (
        SELECT
            DateID
            ,Plant
            ,MainAccount
            ,DebitReportingCurrency
            ,CreditReportingCurrency
            ,DateID_FridayTag
        FROM [FACT].[TrialBalanceLog] AS T
        INNER JOIN FriWeekNum AS F
            ON DATEPART(WEEK, CAST(CONVERT(char(8), T.DateID) AS date)) = F.WeekNumber
            AND YEAR(CAST(CONVERT(char(8), T.DateID) AS date)) = F.YearNumber
    )
    -- CTE Spend END    

    INSERT INTO [FACT].[KPIWeeklyTrialBalance] (KPIDateID, ConceptID, [Value])

    -- KPI STATEMENTS:

    -- KPI: Facility - Plant Spend ($000)
    SELECT
        DateID_FridayTag AS KPIDateID
        ,CASE
            WHEN Plant = 'M97' THEN 'Spend_97'
            WHEN Plant = 'M21' THEN 'Spend_20'
            WHEN Plant = 'M18' THEN 'Spend_18'
        END AS ConceptID
        ,SUM(DebitReportingCurrency + CreditReportingCurrency)/1000 AS [Value] -- usd k[.000]
    FROM TBL_tagged
    WHERE 
        Plant IN ('M97','M21','M18')
        AND MainAccount BETWEEN 600000 AND 699999
        AND MainAccount NOT IN (
            691810, 691820, 691830, 627140, 627143, 627161, 
            627170, 627171, 627190, 627350, 627120, 627160
        )
    GROUP BY
        DateID_FridayTag
        ,CASE
            WHEN Plant = 'M97' THEN 'Spend_97'
            WHEN Plant = 'M21' THEN 'Spend_20'
            WHEN Plant = 'M18' THEN 'Spend_18'
        END

    -- KPI: Facility - Inventory ($000)
    UNION ALL
    SELECT 
        KPIDateID
        ,ConceptID
        ,SUM(EndingBalanceReportingCurrency)/1000 AS [Value] -- usd k[.000]
    FROM (
        SELECT
            CAST(FORMAT(DATEADD(DAY, -2, CAST(CAST(DateID AS CHAR(8)) AS DATE)), 'yyyyMMdd') AS INT) AS KPIDateID-- saved as Friday 
            ,CASE
                WHEN Plant = 'M97' THEN 'InventoryUSD_97'
                WHEN Plant = 'M21' THEN 'InventoryUSD_20'
                WHEN Plant IN (SELECT Plants FROM PlantAndBodegas18) THEN 'InventoryUSD_18'
            END AS ConceptID
            ,EndingBalanceReportingCurrency
        FROM [FACT].[TrialBalanceLog]
        WHERE 
            DATEPART(weekday, CAST(CONVERT(char(8), DateID) AS date)) = 7 -- Sunday only
            AND (Plant IN ('M97','M21') OR Plant IN (SELECT Plants FROM PlantAndBodegas18))
            AND MainAccount IN (
                121301, 121430, 121470, 122201, 122202, 
                122207, 122208, 122256, 122257, 122258, 
                122261, 122295, 129300, 129400
            )
        ) AS sub
    GROUP BY 
        KPIDateID
        ,ConceptID

    -- KPI: Facility - Inventory Raw & WIP ($000)
    UNION ALL
    SELECT 
        KPIDateID
        ,ConceptID
        ,SUM(EndingBalanceReportingCurrency)/1000 AS [Value] -- usd k[.000]
    FROM (
        SELECT
            CAST(FORMAT(DATEADD(DAY, -2, CAST(CAST(DateID AS CHAR(8)) AS DATE)), 'yyyyMMdd') AS INT) AS KPIDateID-- saved as Friday 
            ,CASE
                WHEN Plant = 'M97' THEN 'InventoryRawAndWIP_97'
                WHEN Plant = 'M21' THEN 'InventoryRawAndWIP_20'
                WHEN Plant IN (SELECT Plants FROM PlantAndBodegas18) THEN 'InventoryRawAndWIP_18'
            END AS ConceptID
            ,EndingBalanceReportingCurrency
        FROM [FACT].[TrialBalanceLog]
        WHERE 
            DATEPART(weekday, CAST(CONVERT(char(8), DateID) AS date)) = 7 -- Sunday only
            AND (Plant IN ('M97','M21') OR Plant IN (SELECT Plants FROM PlantAndBodegas18))
            AND MainAccount IN (
                121301, 122201,122202, 122207, 
                122256, 122257, 122258, 122261, 
                122295, 129300, 129400
            )
        ) AS sub
    GROUP BY 
        KPIDateID
        ,ConceptID

    -- KPI: Facility - Inventory in Transit ($000)
    UNION ALL
    SELECT 
        KPIDateID
        ,ConceptID
        ,SUM(EndingBalanceReportingCurrency)/1000 AS [Value] -- usd k[.000]
    FROM (
        SELECT
            CAST(FORMAT(DATEADD(DAY, -2, CAST(CAST(DateID AS CHAR(8)) AS DATE)), 'yyyyMMdd') AS INT) AS KPIDateID-- saved as Friday 
            ,CASE
                WHEN Plant = 'M97' THEN 'InventoryInTransit_97'
                WHEN Plant = 'M21' THEN 'InventoryInTransit_20'
                WHEN Plant IN (SELECT Plants FROM PlantAndBodegas18) THEN 'InventoryInTransit_18'
            END AS ConceptID
            ,EndingBalanceReportingCurrency
        FROM [FACT].[TrialBalanceLog]
        WHERE 
            DATEPART(weekday, CAST(CONVERT(char(8), DateID) AS date)) = 7 -- Sunday only
            AND (Plant IN ('M97','M21') OR Plant IN (SELECT Plants FROM PlantAndBodegas18))
            AND MainAccount = 122208
        ) AS sub
    GROUP BY 
        KPIDateID
        ,ConceptID

    -- KPI: Facility - Inventory Finished Goods ($000)
    UNION ALL
    SELECT 
        KPIDateID
        ,ConceptID
        ,SUM(EndingBalanceReportingCurrency)/1000 AS [Value] -- usd k[.000]
    FROM (
        SELECT
            CAST(FORMAT(DATEADD(DAY, -2, CAST(CAST(DateID AS CHAR(8)) AS DATE)), 'yyyyMMdd') AS INT) AS KPIDateID-- saved as Friday 
            ,CASE
                WHEN Plant = 'M97' THEN 'InventoryFinishedGoodsUSD_97'
                WHEN Plant = 'M21' THEN 'InventoryFinishedGoodsUSD_20'
                WHEN Plant IN (SELECT Plants FROM PlantAndBodegas18) THEN 'InventoryFinishedGoodsUSD_18'
            END AS ConceptID
            ,EndingBalanceReportingCurrency
        FROM [FACT].[TrialBalanceLog]
        WHERE 
            DATEPART(weekday, CAST(CONVERT(char(8), DateID) AS date)) = 7 -- Sunday only
            AND (Plant IN ('M97','M21') OR Plant IN (SELECT Plants FROM PlantAndBodegas18))
            AND MainAccount IN (121430, 121470)
        ) AS sub
    GROUP BY 
        KPIDateID
        ,ConceptID

    -- KPI: Other - VAT - AR Related to VAT
    UNION ALL
    SELECT
        CAST(FORMAT(DATEADD(DAY, -2, CAST(CAST(DateID AS CHAR(8)) AS DATE)), 'yyyyMMdd') AS INT) AS KPIDateID-- saved as Friday 
        ,'ArRelated_VAT' AS ConceptID
        ,SUM(EndingBalanceReportingCurrency)/1000 AS [Value] -- usd k[.000]
    FROM [FACT].[TrialBalanceLog]
    WHERE 
        DATEPART(weekday, CAST(CONVERT(char(8), DateID) AS date)) = 7 -- Sunday only
        AND MainAccount IN (118200, 118201, 118202, 118203, 118204, 118207, 118208, 206101, 206102, 206103, 206201, 206202, 206203)
    GROUP BY 
        CAST(FORMAT(DATEADD(DAY, -2, CAST(CAST(DateID AS CHAR(8)) AS DATE)), 'yyyyMMdd') AS INT)

END
GO
