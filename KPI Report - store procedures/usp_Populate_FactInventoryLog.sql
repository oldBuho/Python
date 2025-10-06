SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
OBJECTIVE:
This process generates a log of daily stock not available in Dynamics.
The first version only considers WG and Steel, but it was designed to 
potentially include other cases (e.g., an expensive raw material 
to be tracked for control, a specific steel quality to be studied, etc.).

VERSION CONTROL:
Apr-2025: V1, structure by Federico Juiz and code edited by Otman Herrera
*/

ALTER PROCEDURE [dbo].[usp_Populate_FactInventoryLog]
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

    -- today variable
    -- includes CAST as internally it brings a timestamp (save memory and avoid error)
    DECLARE @Today DATE = CAST(GETDATE() AS DATE);
    DECLARE @DateID int = CONVERT(varchar, @Today, 112);

    -- Evaluate if "today" already has data in FACT.InventoryLog
    -- OPTION A: only saves the first value of the day
    -- IF NOT EXISTS (
    --     SELECT 1 
    --     FROM [FACT].[InventoryLog]
    --     WHERE [Date] = @Today
    -- )
    -- OPTION B: deletes today's values and saves updated ones
    DELETE FROM [FACT].[InventoryLog]
    WHERE [Date] = @Today;

    -- INSERT only today's value 
    WITH gallon_source AS (
    SELECT
        s.itemid,
        LOWER(s.dataareaid) AS dataareaid,
        s.inventsiteid,
        s.wmslocationid,
        s.physicalinvent,
        t.unitvolume
    FROM
        CompanyLakehouse.dbo.inventsum s
            INNER JOIN
        CompanyLakehouse.dbo.inventtable t
            ON LOWER(s.dataareaid) = LOWER(t.dataareaid)
            AND s.itemid = t.itemid
    WHERE
        LOWER(s.dataareaid) = 'tmmg'
        AND s.physicalinvent <> 0
        AND unitvolume > 0
    ),
    gallon_total AS (
    SELECT
        @DateID AS DateID,
        @Today AS [Date],
        SUM(gs.unitvolume*gs.physicalinvent) AS [Value],
        'WG' AS ValueType,
        'gallon' AS ValueUnit,
        si.SiteGroupID,
        -1 AS SiteID
    FROM
        gallon_source gs
            INNER JOIN
        DIM.Site si
            ON gs.dataareaid = si.CompanyID AND gs.inventsiteid = si.SiteCode
    WHERE
        si.SiteGroup IN ('Location 20', 'Location 18', 'Location 18 Warehouse', 'Location 97')
        AND (
            si.SiteGroup = 'Location 20'
            OR si.SiteGroup = 'Location 18'
            OR si.SiteGroup = 'Location 18 Warehouse'
            OR (si.SiteGroup = 'Location 97' AND gs.wmslocationid IN ('VTALPG', 'VTAUSA'))
        )
    GROUP BY
        si.SiteGroupID
    )
    -- once the WG table is created 
    -- it's inserted alongside the Tons table
    INSERT INTO [FACT].[InventoryLog]
    SELECT * FROM gallon_total
    UNION ALL
    SELECT
        @DateID AS DateID,
        @Today AS [Date],
        SUM(it.Tons) AS [Value],
        'Steel' AS ValueType,
        'ton' AS ValueUnit,
        -1 AS SiteGroupID,
        si.SiteID
    FROM
        FACT.InventoryTons it
            INNER JOIN
        DIM.Site si
            ON it.SiteID = si.SiteID
    WHERE
        si.SiteCode IN ('97', '20', '18')
    GROUP BY
        si.SiteID;

END
GO
