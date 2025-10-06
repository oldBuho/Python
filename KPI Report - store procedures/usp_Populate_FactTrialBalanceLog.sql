SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
This process emulates the behavior in D365: General Ledger → Inquiries and Reports → Trial Balance;
"https://Company.operations.dynamics.com/?cmp=TMMG&mi=LedgerTrialBalanceListPage"
DevOps ticket #7769 

Notes:
    ~ YTD es required to calculate closing_balance for a given date
    ~ Transactions, as debit or credit, can be calculated either over a date range or by individual date. 
      For this log, they are pulled daily in the second segment of the CTEs.
    ~ Reprocessing previous days must be considered to ensure that the modifications are reflected in the table.
        In addition, extracting data for the current day often returns NULL values from CompanyLakehouse (Dynamics Link).

QA/Reference: page Fact.TrialBalanceLog from the following document
    https://Company.sharepoint.com/:x:/s/IT/EdzIG1y3M-RDiDPpSxQnlHEBp2x5HYoVTYwiXv5Zkja7Cw?e=g70HzP
*/

CREATE PROCEDURE [dbo].[usp_Populate_FactTrialBalanceLog]
AS
BEGIN
    
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON;

    -- variables, as presented on the Trial Balance form in Dynamics
    DECLARE @dataareaid NVARCHAR(10) = 'TMMG';
    DECLARE @postinglayer AS INT = 0; 
    DECLARE @dimension BIGINT = (  
        SELECT [recid]
        FROM [CompanyLakehouse].[dbo].[dimensionhierarchy]
        WHERE [description] = 'MA+PLA+DEPT+PROD'
        );

    -- Variables to be used in the while loop. This are then use in the "enddate" variable for the Trial Balance query.
    -- last date
    DECLARE @ToDate_loop DATE = CAST(GETDATE() AS DATE); 
    -- first date, 60 days before the current date declared in ToDate_loop
    DECLARE @FromDate_loop DATE = DATEADD(DAY, -60, @ToDate_loop); 

    -- *** LOOP _ START *** 
    WHILE @FromDate_loop <= @ToDate_loop
    BEGIN

        -- dates to be used in the query
        DECLARE @enddate AS DATE = @FromDate_loop;    
        DECLARE @startdate AS DATE = DATEFROMPARTS(YEAR(@enddate), 1, 1);
        DECLARE @enddateID INT = CONVERT(VARCHAR, @enddate, 112);

        -- deletion before writing
        DELETE FROM [FACT].[TrialBalanceLog]
        WHERE [DateID] =  @enddateID;

        --- *** START - YTD for CLOSING BALANCE ***
        -- focus balance base grouping
        WITH FB_YTD AS (
            SELECT
                dfb.[fiscalcalendarperiodtype],
                davc.[mainaccountvalue],
                davc.[plantvalue],
                davc.[departmentsvalue],
                davc.[productcodesvalue],
                SUM(dfb.[debitaccountingcurrencyamount]) AS debitaccountingcurrencyamount,
                SUM(dfb.[creditaccountingcurrencyamount]) AS creditaccountingcurrencyamount,
                SUM(dfb.[debitreportingcurrencyamount]) AS debitreportingcurrencyamount,
                SUM(dfb.[creditreportingcurrencyamount]) AS creditreportingcurrencyamount
            FROM [CompanyLakehouse].[dbo].[dimensionfocusbalance] dfb
            INNER JOIN [CompanyLakehouse].[dbo].[dimensionattributevaluecombination] davc
                ON dfb.[focusledgerdimension] = davc.[recid]
            INNER JOIN [CompanyLakehouse].[dbo].[ledger] l
                ON dfb.[ledger] = l.[recid]
            WHERE
                dfb.[postinglayer] = @postinglayer
                AND dfb.[accountingdate] BETWEEN @startdate AND @enddate
                AND l.[name] = @dataareaid
                AND dfb.[focusdimensionhierarchy] = @dimension
            GROUP BY
                dfb.[fiscalcalendarperiodtype],
                davc.[mainaccountvalue],
                davc.[plantvalue],
                davc.[departmentsvalue],
                davc.[productcodesvalue]
        ),

        -- opening balance
        opening_YTD AS (
            SELECT
                mainaccountvalue,
                plantvalue,
                departmentsvalue,
                productcodesvalue,
                (debitaccountingcurrencyamount + creditaccountingcurrencyamount) AS beginningbalanceaccountingcurrencyamount,
                0 AS debitaccountingcurrencyamount,
                0 AS creditaccountingcurrencyamount,
                (debitreportingcurrencyamount + creditreportingcurrencyamount) AS beginningbalanceportingcurrencyamount,
                0 AS debitreportingcurrencyamount,
                0 AS creditreportingcurrencyamount
            FROM FB_YTD
            WHERE fiscalcalendarperiodtype = 0  -- ! IMPORTANT
        ),

        -- transactions balance
        transactions_YTD AS (
            SELECT
                mainaccountvalue,
                plantvalue,
                departmentsvalue,
                productcodesvalue,
                0 AS beginningbalanceaccountingcurrencyamount,
                debitaccountingcurrencyamount,
                creditaccountingcurrencyamount,
                0 AS beginningbalanceportingcurrencyamount,
                debitreportingcurrencyamount,
                creditreportingcurrencyamount
            FROM FB_YTD
            WHERE fiscalcalendarperiodtype = 1     -- ! IMPORTANT
        ),

        -- opening + transactions
        FBS_YTD AS (
            SELECT * FROM opening_YTD
            UNION ALL
            SELECT * FROM transactions_YTD
        ),
        result_YTD as (
        -- opening + transactions + closing
        SELECT
            mainaccountvalue,
            plantvalue,
            departmentsvalue,
            productcodesvalue,

            --SUM(beginningbalanceaccountingcurrencyamount) AS beginningbalanceaccountingcurrencyamount,
            -- SUM(debitaccountingcurrencyamount) AS debitaccountingcurrencyamount,
            -- SUM(creditaccountingcurrencyamount) AS creditaccountingcurrencyamount,
            SUM(beginningbalanceaccountingcurrencyamount + debitaccountingcurrencyamount + creditaccountingcurrencyamount) AS endingbalanceaccountingcurrencyamount,

            -- SUM(beginningbalanceportingcurrencyamount) AS beginningbalanceportingcurrencyamount,
            -- SUM(debitreportingcurrencyamount) AS debitreportingcurrencyamount,
            -- SUM(creditreportingcurrencyamount) AS creditreportingcurrencyamount,
            SUM(beginningbalanceportingcurrencyamount + debitreportingcurrencyamount + creditreportingcurrencyamount) AS endingbalanceportingcurrencyamount
        FROM FBS_YTD
        GROUP BY
            mainaccountvalue,
            plantvalue,
            departmentsvalue,
            productcodesvalue
        ),
        --- *** END  - YTD for CLOSING BALANCE ***

        --- *** START  - filtered by DATE for DEBIT AND CREDIT DAILY DATA POINT ***
        FB_daily AS (
            SELECT
                davc.[mainaccountvalue],
                davc.[plantvalue],
                davc.[departmentsvalue],
                davc.[productcodesvalue],
                SUM(dfb.[debitaccountingcurrencyamount]) AS debitaccountingcurrencyamount,
                SUM(dfb.[creditaccountingcurrencyamount]) AS creditaccountingcurrencyamount,
                SUM(dfb.[debitreportingcurrencyamount]) AS debitreportingcurrencyamount,
                SUM(dfb.[creditreportingcurrencyamount]) AS creditreportingcurrencyamount
            FROM [CompanyLakehouse].[dbo].[dimensionfocusbalance] dfb
            INNER JOIN [CompanyLakehouse].[dbo].[dimensionattributevaluecombination] davc
                ON dfb.[focusledgerdimension] = davc.[recid]
            INNER JOIN [CompanyLakehouse].[dbo].[ledger] l
                ON dfb.[ledger] = l.[recid]
            WHERE
                dfb.[postinglayer] = @postinglayer
                AND dfb.[accountingdate] = @enddate
                AND l.[name] = @dataareaid
                AND dfb.[focusdimensionhierarchy] = @dimension
            
                AND fiscalcalendarperiodtype = 1      -- ! IMPORTANT
        
            GROUP BY
                davc.[mainaccountvalue],
                davc.[plantvalue],
                davc.[departmentsvalue],
                davc.[productcodesvalue]
        )
        --- *** END  - filtered by DATE for DEBIT AND CREDIT DAILY DATA POINT ***

        INSERT INTO [FACT].[TrialBalanceLog] (
            [DateID],
            [MainAccount],
            [Plant],
            [Departments],
            [ProductCodes],
            [Debit],
            [Credit],
            [ClosingBalance],
            [DebitReportingCurrency],
            [CreditReportingCurrency],
            [EndingBalanceReportingCurrency]
            )
        SELECT
            -- dim
            @enddateID,
            r.mainaccountvalue,
            r.plantvalue,
            r.departmentsvalue,
            r.productcodesvalue,

            -- MEX currency
            f.debitaccountingcurrencyamount,
            f.creditaccountingcurrencyamount,
            r.endingbalanceaccountingcurrencyamount,

            -- USD currency
            f.debitreportingcurrencyamount,
            f.creditreportingcurrencyamount,
            r.endingbalanceportingcurrencyamount
        FROM
            result_YTD AS r
        LEFT JOIN FB_daily AS f -- LEFT to include accounts without a daily transaction
            ON r.mainaccountvalue = f.mainaccountvalue
            AND r.plantvalue = f.plantvalue

            -- departmentsvalue and productcodesvalue are currently mostly nulls (Sep 25) >>> NULL = NULL is FALSE in SQL
            -- nulls handling
            -- V2:          
            AND (
                (r.departmentsvalue = f.departmentsvalue)
                OR (r.departmentsvalue IS NULL AND f.departmentsvalue IS NULL)
            )
            AND (
                (r.productcodesvalue = f.productcodesvalue)
                OR (r.productcodesvalue IS NULL AND f.productcodesvalue IS NULL)
            )
            -- V1:
            --AND COALESCE(r.departmentsvalue, '') = COALESCE(f.departmentsvalue, '')
            --AND COALESCE(r.productcodesvalue,'') = COALESCE(f.productcodesvalue, '')

        -- Change to the next day in the while loop
        SET @FromDate_loop = DATEADD(DAY, 1, @FromDate_loop);

    END
    -- *** LOOP _ END *** 

END 

-- SP v1 without the while loop (2025-09-18)
/*
ALTER PROCEDURE [dbo].[usp_Populate_FactTrialBalanceLog]
AS
BEGIN
    
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON;
    

    -- variables, as presented on the Trial Balance form
    DECLARE @dataareaid NVARCHAR(10) = 'TMMG';                          -- company
    DECLARE @postinglayer AS INT = 0;                                   -- posting layer = 'current'
    DECLARE @enddate AS DATE = CAST(GETDATE() AS DATE);                 -- date    
    DECLARE @startdate AS DATE = DATEFROMPARTS(YEAR(@enddate), 1, 1);   -- required for closing balance (YTD)
    DECLARE @dimension BIGINT = (                                       -- financial dimension set = 'MA+PLA+DEPT+PROD'
        SELECT [recid]
        FROM [CompanyLakehouse].[dbo].[dimensionhierarchy]
        WHERE [description] = 'MA+PLA+DEPT+PROD'
    );
    DECLARE @enddateID INT = CONVERT(VARCHAR, @enddate, 112);               -- date to be used as ID in TrialBalanceLog

    -- avoids duplicate insertions
    DELETE FROM [FACT].[TrialBalanceLog]
    WHERE [DateID] =  @enddateID;

    --- *** START - YTD for CLOSING BALANCE ***
    -- focus balance base grouping
    WITH FB_YTD AS (
        SELECT
            dfb.[fiscalcalendarperiodtype],
            davc.[mainaccountvalue],
            davc.[plantvalue],
            davc.[departmentsvalue],
            davc.[productcodesvalue],
            SUM(dfb.[debitaccountingcurrencyamount]) AS debitaccountingcurrencyamount,
            SUM(dfb.[creditaccountingcurrencyamount]) AS creditaccountingcurrencyamount,
            SUM(dfb.[debitreportingcurrencyamount]) AS debitreportingcurrencyamount,
            SUM(dfb.[creditreportingcurrencyamount]) AS creditreportingcurrencyamount
        FROM [CompanyLakehouse].[dbo].[dimensionfocusbalance] dfb
        INNER JOIN [CompanyLakehouse].[dbo].[dimensionattributevaluecombination] davc
            ON dfb.[focusledgerdimension] = davc.[recid]
        INNER JOIN [CompanyLakehouse].[dbo].[ledger] l
            ON dfb.[ledger] = l.[recid]
        WHERE
            dfb.[postinglayer] = @postinglayer
            AND dfb.[accountingdate] BETWEEN @startdate AND @enddate
            AND l.[name] = @dataareaid
            AND dfb.[focusdimensionhierarchy] = @dimension
        GROUP BY
            dfb.[fiscalcalendarperiodtype],
            davc.[mainaccountvalue],
            davc.[plantvalue],
            davc.[departmentsvalue],
            davc.[productcodesvalue]
    ),

    -- opening balance
    opening_YTD AS (
        SELECT
            mainaccountvalue,
            plantvalue,
            departmentsvalue,
            productcodesvalue,
            (debitaccountingcurrencyamount + creditaccountingcurrencyamount) AS beginningbalanceaccountingcurrencyamount,
            0 AS debitaccountingcurrencyamount,
            0 AS creditaccountingcurrencyamount,
            (debitreportingcurrencyamount + creditreportingcurrencyamount) AS beginningbalanceportingcurrencyamount,
            0 AS debitreportingcurrencyamount,
            0 AS creditreportingcurrencyamount
        FROM FB_YTD
        WHERE fiscalcalendarperiodtype = 0  -- ! IMPORTANT
    ),

    -- transactions balance
    transactions_YTD AS (
        SELECT
            mainaccountvalue,
            plantvalue,
            departmentsvalue,
            productcodesvalue,
            0 AS beginningbalanceaccountingcurrencyamount,
            debitaccountingcurrencyamount,
            creditaccountingcurrencyamount,
            0 AS beginningbalanceportingcurrencyamount,
            debitreportingcurrencyamount,
            creditreportingcurrencyamount
        FROM FB_YTD
        WHERE fiscalcalendarperiodtype = 1     -- ! IMPORTANT
    ),

    -- opening + transactions
    FBS_YTD AS (
        SELECT * FROM opening_YTD
        UNION ALL
        SELECT * FROM transactions_YTD
    ),
    result_YTD as (
    -- opening + transactions + closing
    SELECT
        mainaccountvalue,
        plantvalue,
        departmentsvalue,
        productcodesvalue,

        --SUM(beginningbalanceaccountingcurrencyamount) AS beginningbalanceaccountingcurrencyamount,
        -- SUM(debitaccountingcurrencyamount) AS debitaccountingcurrencyamount,
        -- SUM(creditaccountingcurrencyamount) AS creditaccountingcurrencyamount,
        SUM(beginningbalanceaccountingcurrencyamount + debitaccountingcurrencyamount + creditaccountingcurrencyamount) AS endingbalanceaccountingcurrencyamount,

        -- SUM(beginningbalanceportingcurrencyamount) AS beginningbalanceportingcurrencyamount,
        -- SUM(debitreportingcurrencyamount) AS debitreportingcurrencyamount,
        -- SUM(creditreportingcurrencyamount) AS creditreportingcurrencyamount,
        SUM(beginningbalanceportingcurrencyamount + debitreportingcurrencyamount + creditreportingcurrencyamount) AS endingbalanceportingcurrencyamount
    FROM FBS_YTD
    GROUP BY
        mainaccountvalue,
        plantvalue,
        departmentsvalue,
        productcodesvalue
    ),
    --- *** END  - YTD for CLOSING BALANCE ***
    --- *** START  - filtered by DATE for DEBIT AND CREDIT DAILY DATA POINT ***
    FB_daily AS (
        SELECT
            davc.[mainaccountvalue],
            davc.[plantvalue],
            davc.[departmentsvalue],
            davc.[productcodesvalue],
            SUM(dfb.[debitaccountingcurrencyamount]) AS debitaccountingcurrencyamount,
            SUM(dfb.[creditaccountingcurrencyamount]) AS creditaccountingcurrencyamount,
            SUM(dfb.[debitreportingcurrencyamount]) AS debitreportingcurrencyamount,
            SUM(dfb.[creditreportingcurrencyamount]) AS creditreportingcurrencyamount
        FROM [CompanyLakehouse].[dbo].[dimensionfocusbalance] dfb
        INNER JOIN [CompanyLakehouse].[dbo].[dimensionattributevaluecombination] davc
            ON dfb.[focusledgerdimension] = davc.[recid]
        INNER JOIN [CompanyLakehouse].[dbo].[ledger] l
            ON dfb.[ledger] = l.[recid]
        WHERE
            dfb.[postinglayer] = @postinglayer
            AND dfb.[accountingdate] = @enddate
            AND l.[name] = @dataareaid
            AND dfb.[focusdimensionhierarchy] = @dimension
        
            AND fiscalcalendarperiodtype = 1      -- ! IMPORTANT
    
        GROUP BY
            davc.[mainaccountvalue],
            davc.[plantvalue],
            davc.[departmentsvalue],
            davc.[productcodesvalue]
    )
    --- *** END  - filtered by DATE for DEBIT AND CREDIT DAILY DATA POINT ***

    INSERT INTO [FACT].[TrialBalanceLog] (
        [DateID],
        [MainAccount],
        [Plant],
        [Departments],
        [ProductCodes],
        [Debit],
        [Credit],
        [ClosingBalance],
        [DebitReportingCurrency],
        [CreditReportingCurrency],
        [EndingBalanceReportingCurrency]
        )
    SELECT
        -- dim
        @enddateID,
        r.mainaccountvalue,
        r.plantvalue,
        r.departmentsvalue,
        r.productcodesvalue,

        -- MEX currency
        f.debitaccountingcurrencyamount,
        f.creditaccountingcurrencyamount,
        r.endingbalanceaccountingcurrencyamount,

        -- USD currency
        f.debitreportingcurrencyamount,
        f.creditreportingcurrencyamount,
        r.endingbalanceportingcurrencyamount
    FROM
        result_YTD AS r
    LEFT JOIN FB_daily AS f
        ON r.mainaccountvalue = f.mainaccountvalue
        AND r.plantvalue = f.plantvalue
        AND COALESCE(r.departmentsvalue, '') = COALESCE(f.departmentsvalue, '')     -- currently mostly nulls (jul 25) >>> NULL = NULL ; FALSE
        AND COALESCE(r.productcodesvalue,'') = COALESCE(f.productcodesvalue, '')    -- currently mostly nulls (jul 25) >>> NULL = NULL ; FALSE

END 
GO
*/

GO
