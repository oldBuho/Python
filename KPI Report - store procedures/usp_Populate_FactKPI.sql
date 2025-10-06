SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*
OBJECTIVE:
This stored procedure aims to gather KPIs from different sources 
to be used in the 'KPI Report' Power BI dashboard.
VERSION CONTROL:
Jul-2025: V1, structure by Federico Juiz.
*/
CREATE PROCEDURE [dbo].[usp_Populate_FactKPI]
AS
BEGIN
    
    -- increment SQL query performance
    SET NOCOUNT ON;

    -- PROCESS:

	TRUNCATE TABLE [FACT].[KPI];
   
    -- Setting Monday = 1
    -- Cause: It is not possible to use [Week] or [DOW] from DIM.Date because it uses a different week number logic;
    -- the week starts on Sunday, not Monday as required for this project.
    SET DATEFIRST 1; 

    -- CTE BLOCK START
    -- Friday marker
    WITH FridayWeekNumber AS (
        SELECT DISTINCT
            DateID AS DateID_FridayTag
            ,DATEPART(WEEK, CAST(CONVERT(CHAR(8), DateID) AS DATE)) AS WeekNumber
            ,[Year] AS YearNumber
        FROM DIM.[Date]
        WHERE 
            DateID <> -1 AND DateID IS NOT NULL -- to avoid CAST error
            AND FORMAT(CAST(CONVERT(CHAR(8), DateID) AS DATE), 'dddd') = 'Friday' -- there is not day name in Dim.Date and DOW cannot be used as it applies different WeekNumber logic
            AND [Year] >= 2023 -- this project started in 2024
    ),
    -- CTE for KPI Facility - WG Finished
    -- D365 form: Inventory Transaction
    -- first data point from Dynamics is from 2024-07-12
    -- https://Company.operations.dynamics.com/?cmp=TMMG&mi=InventTrans 
    Facility_WGFinished_FridayTag AS (
        SELECT
            --T1.InventoryDateID, 
            T1.SiteID, F.DateID_FridayTag
            ,SUM(T1.Qty * T2.unitvolume) AS WGFinished
        FROM
            [FACT].[InventoryTransaction] AS T1
        INNER JOIN 
            [CompanyLakehouse].[dbo].[inventtable] AS T2
            ON LOWER(T1.CompanyID) = LOWER(T2.dataareaid)
            AND T1.ItemID = T2.itemid
        INNER JOIN FridayWeekNumber AS F
            ON DATEPART(WEEK, CAST(CONVERT(CHAR(8), T1.InventoryDateID) AS DATE)) = F.WeekNumber
            AND YEAR(CAST(CONVERT(CHAR(8), T1.InventoryDateID) AS DATE)) = F.YearNumber
        WHERE 1=1
            AND LOWER(CompanyID) = 'tmmg'
            AND ReferenceCategory = 2 -- = 'Production' as defined in VIEW: [FACT].[FactInventoryTransaction]
            AND SiteID IN ('97','20','18')
        GROUP BY 
            --T1.InventoryDateID, 
            T1.SiteID, F.DateID_FridayTag 
    ),
    -- CTE for KPI Facility - WG Shipped
    -- D365 form: Shipment Deviation
    -- first data point from Dynamics is from 2024-07-12
    -- https://Company.operations.dynamics.com/?cmp=TMMG&mi=SalesShipmentDeviation
    Facility_WGShipped_FridayTag AS (
        SELECT
            --S.deliverydate -- ship date, timestamp formatted
            F.DateID_FridayTag -- friday grouping
            --,D.inventsiteid -- Site ID
            ,SI.SiteGroup
            --,S.[itemid]
            ,SUM(S.[qty] * I.unitvolume) AS WGShipped -- delivered qty x volume/WG | NOTE: qty contains negative values
        FROM [CompanyLakehouse].[dbo].[custpackingsliptrans] AS S 
        INNER JOIN  [CompanyLakehouse].[dbo].[inventdim] AS D -- required to convert site code/id into site description
            ON S.[inventdimid] = D.inventdimid -- site
            AND LOWER(S.dataareaid) = LOWER(D.dataareaid) -- co. ID
        INNER JOIN DIM.Site AS SI-- for SiteGroup inlusion
            ON D.inventsiteid = SI.SiteCode 
        INNER JOIN 
            [CompanyLakehouse].[dbo].[inventtable] AS I -- for volume
            ON LOWER(S.dataareaid) = LOWER(I.dataareaid)
            AND S.[itemid] = I.itemid
        INNER JOIN FridayWeekNumber AS F
            ON DATEPART(WEEK, S.deliverydate) = F.WeekNumber
            AND YEAR(S.deliverydate) = F.YearNumber
        WHERE 
            LOWER(S.dataareaid) = 'tmmg'
            AND SI.SiteGroup IN ('Location 97','Location 20','Location 18', 'Location 18 Warehouse')
        group by 
            F.DateID_FridayTag
            ,D.inventsiteid
            ,SI.SiteGroup
    )
    -- CTE BLOCK END

	INSERT INTO [FACT].[KPI]

    -- KPI: Facility - WG Finished
    SELECT
        DateID_FridayTag AS KPIDateID
        ,CASE
            WHEN SiteID = '20' THEN 'WGFinished_20'
            WHEN SiteID = '18' THEN 'WGFinished_18'
            WHEN SiteID = '97' THEN 'WGFinished_97'
        END AS ConceptID
        ,SUM(WGFinished) AS [Value] 
    FROM Facility_WGFinished_FridayTag
    GROUP BY
        DateID_FridayTag
        ,CASE
            WHEN SiteID = '20' THEN 'WGFinished_20'
            WHEN SiteID = '18' THEN 'WGFinished_18'
            WHEN SiteID = '97' THEN 'WGFinished_97'
        END

    -- KPI: Facility - WG Shipped
    UNION ALL
    SELECT
        DateID_FridayTag AS KPIDateID
        ,CASE
            WHEN SiteGroup = 'Location 20' THEN 'WGShipped_20'
            WHEN SiteGroup = 'Location 18' OR SiteGroup = 'Location 18 Warehouse' THEN 'WGShipped_18'
            WHEN SiteGroup = 'Location 97' THEN 'WGShipped_97'
        END AS ConceptID
        ,SUM(WGShipped) AS [Value] 
    FROM Facility_WGShipped_FridayTag
    GROUP BY
        DateID_FridayTag
        ,CASE
            WHEN SiteGroup = 'Location 20' THEN 'WGShipped_20'
            WHEN SiteGroup = 'Location 18' OR SiteGroup = 'Location 18 Warehouse' THEN 'WGShipped_18'
            WHEN SiteGroup = 'Location 97' THEN 'WGShipped_97'
        END

    -- Manual input from Sharepoint (Dataflow)
    UNION ALL
    SELECT KPIDateID, ConceptID, [Value]
    FROM [SharepointLakehouse].[dbo].[KPIManualInput]

    -- KPI from WeeklyTrialBalance table; data saved on Fridays as required in BI dashboard
    -- * Facility - Spend_$ and Inventory_$ 
    -- * VAT
    UNION ALL
    SELECT [KPIDateID], [ConceptID], [Value]
    FROM [FACT].[KPIWeeklyTrialBalance]

    -- KPI: Facility - Inventory Steel & WG from InventoryLog table
    -- Note (!): Only use Friday data (one day per week); otherwise, it could cause issues 
    -- on accumulated calculations (eg. MTD, YTD), since inventory values are daily "totals", 
    -- not daily in/out transactions (partial data).
    UNION ALL
    SELECT
        i.DateID AS KPIDateID
        ,COALESCE(
            CASE s2.SiteGroup 
                WHEN 'Location 20' THEN 'InventoryFinishedGoodsWG_20'
                WHEN 'Location 97' THEN 'InventoryFinishedGoodsWG_97'
                WHEN 'Location 18 Warehouse' THEN 'InventoryFinishedGoodsWGWarehouse_18'
                WHEN 'Location 18' THEN 'InventoryFinishedGoodsWGPlant_18'
                ELSE NULL
            END,
            CASE s1.SiteCode
                WHEN '20' THEN 'InventorySteel_20'
                WHEN '18' THEN 'InventorySteel_18'
                WHEN '97' THEN 'InventorySteel_97'
                ELSE NULL
            END
        ) AS ConceptID
        ,i.[Value]
    FROM [FACT].[InventoryLog] AS i
    INNER JOIN DIM.Site AS s1
        ON i.SiteID = s1.SiteID
    INNER JOIN (SELECT DISTINCT SiteGroup, SiteGroupID FROM DIM.Site) AS s2
        ON i.SiteGroupID = s2.SiteGroupID
    INNER JOIN DIM.DimKPIDate d
        ON i.DateID = d.KPIDateID
    WHERE DayName = 'Friday'


END
GO
