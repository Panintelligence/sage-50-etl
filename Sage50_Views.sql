
/****** Object:  View [dbo].[vw_Financial_Periods]    Script Date: 09/12/2019 14:43:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE VIEW [dbo].[vw_Financial_Periods]




as

SELECT *
		,CONVERT(DATETIME,(CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' + CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar)),103) as StartFYDate

FROM
(

SELECT [PERIOD] + 1 as Financial_Period
		,MONTH(DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))) as Month_No
        ,DATENAME(mm,DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))) as Financial_Month
	    ,LEFT(DATENAME(mm,DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))),3) as Short_Financial_Month
	   ,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as Financial_Year
	   ,CASE WHEN [PERIOD] + 1 <= MONTH(DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))) 
			 THEN (SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY)
			 ELSE (SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) + 1
		END as Actual_Year
		, 0 as Financial_Years_From_Today
	 ,'Current' as ReportingPeriod
	 ,PI_ID
  FROM [dbo].[PERIOD]
  WHERE [PERIOD] BETWEEN 0 AND 11

UNION ALL

SELECT [PERIOD] + 1 as Financial_Period
		,MONTH(DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))) as Month_No
       ,DATENAME(mm,DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))) as Financial_Month
	    ,LEFT(DATENAME(mm,DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))),3) as Short_Financial_Month
	   ,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) - 1 as Financial_Year
	   ,CASE WHEN [PERIOD] + 1 <= MONTH(DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))) 
			 THEN (SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY)
			 ELSE (SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) + 1
		END - 1 as Actual_Year
		, -1 as Financial_Years_From_Today
	 ,'Previous' as ReportingPeriod
	 	 ,PI_ID
  FROM [dbo].[PERIOD]
  WHERE [PERIOD] BETWEEN 0 AND 11

UNION ALL

SELECT [PERIOD] + 1 as Financial_Period
		,MONTH(DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))) as Month_No
       ,DATENAME(mm,DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))) as Financial_Month
	   ,LEFT(DATENAME(mm,DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))),3) as Short_Financial_Month
	   ,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) - 2 as Financial_Year
	   ,CASE WHEN [PERIOD] + 1 <= MONTH(DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))) 
			 THEN (SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY)
			 ELSE (SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) + 1
		END - 2 as Actual_Year
		 ,-2 as Financial_Years_From_Today
		 , NULL
		 	 ,PI_ID
  FROM [dbo].[PERIOD]
  WHERE [PERIOD] BETWEEN 0 AND 11

  
UNION ALL

SELECT [PERIOD] + 1 as Financial_Period
		,MONTH(DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))) as Month_No
       ,DATENAME(mm,DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))) as Financial_Month
	   ,LEFT(DATENAME(mm,DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))),3) as Short_Financial_Month
	   ,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) - 3 as Financial_Year
	   ,CASE WHEN [PERIOD] + 1 <= MONTH(DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))) 
			 THEN (SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY)
			 ELSE (SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) + 1
		END - 3 as Actual_Year
		,-3 as Financial_Years_From_Today
		, NULL
			 ,PI_ID
  FROM [dbo].[PERIOD]
  WHERE [PERIOD] BETWEEN 0 AND 11


UNION ALL

SELECT [PERIOD] + 1 as Financial_Period
		,MONTH(DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))) as Month_No
       ,DATENAME(mm,DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))) as Financial_Month
	    ,LEFT(DATENAME(mm,DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))),3) as Short_Financial_Month
	   ,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) - 4 as Financial_Year
	   ,CASE WHEN [PERIOD] + 1 <= MONTH(DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))) 
			 THEN (SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY)
			 ELSE (SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) + 1
		END - 4 as Actual_Year
		,-4 as Financial_Years_From_Today
		, NULL
			 ,PI_ID
  FROM [dbo].[PERIOD]
  WHERE [PERIOD] BETWEEN 0 AND 11

UNION ALL


SELECT [PERIOD] + 1 as Financial_Period
		,MONTH(DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))) as Month_No
       ,DATENAME(mm,DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))) as Financial_Month
	    ,LEFT(DATENAME(mm,DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))),3) as Short_Financial_Month
	   ,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) + 1 as Financial_Year
	   ,CASE WHEN [PERIOD] + 1 <= MONTH(DATEADD(mm,[Period],CONVERT(DATETIME,CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' +  CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar),103))) 
			 THEN (SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY)
			 ELSE (SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) + 1
		END + 1 as Actual_Year
		, 1 as Financial_Years_From_Today
		, NULL
			 ,PI_ID
  FROM [dbo].[PERIOD]
  WHERE [PERIOD] BETWEEN 0 AND 11

)sub
GO
/****** Object:  View [dbo].[vw_CategoryandPeriods]    Script Date: 09/12/2019 14:43:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE VIEW [dbo].[vw_CategoryandPeriods]

as

SELECT DISTINCT 
fp.Financial_Period
		,fp.Financial_Year
		,fp.Month_No
		,fp.Actual_Year
		,cat.chart
		,cat.category
		,cat.NAME
		,cat.LOW
		,cat.HIGH
		,cat.SORT_ORDER
		,cat.PI_ID
		,cat.FLAG_ASSET_LIABILITY

FROM [dbo].[vw_Financial_Periods] fp
		,[dbo].[CATEGORY] cat


GO
/****** Object:  View [dbo].[vw_Balance_Sheet]    Script Date: 09/12/2019 14:43:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO







CREATE VIEW [dbo].[vw_Balance_Sheet] AS




SELECT cat.NAME	as Display_Name
		,CAST(CAST(cat.CATEGORY as varchar) + '.' + cast(cat.SORT_ORDER as varchar) as float) as Sort_Order
		,cat.CATEGORY
		,ctit.TITLE		as Category_Title
		,cat.CHART
		,MONTH(aj.DATE)													as Transaction_Month
		,dep.NAME		as Department
		,ISNULL(SUM(aj.Amount * CASE WHEN FLAG_ASSET_LIABILITY = 1 THEN -1 ELSE 1 END),0)	as Actual_Amount
		,CASE WHEN FLAG_ASSET_LIABILITY = 1 THEN 'Liability Line' ELSE 'Display Line' END		as Row_Type
		,AJ.NOMINAL_CODE
		,YEAR(aj.DATE)													as Tran_Year
		,cat.Financial_Period
		,cat.Financial_Year
		,cat.PI_ID




  FROM dbo.vw_CategoryandPeriods cat

		LEFT JOIN  [dbo].[NOMINAL_LEDGER] Nom
			ON nom.ACCOUNT_REF >= cat.LOW
			AND nom.ACCOUNT_REF <= cat.HIGH
			AND nom.PI_ID = cat.PI_ID
			
		LEFT JOIN [dbo].[AUDIT_JOURNAL] AJ
			ON nom.ACCOUNT_REF = AJ.NOMINAL_CODE
				AND  MONTH(aj.DATE) = cat.Month_No
				AND YEAR(aj.DATE) = cat.Actual_Year
				AND AJ.DELETED_FLAG = 0
				AND aj.PI_ID = cat.PI_ID

		LEFT JOIN [dbo].[CAT_TITLE] ctit
			ON cat.CHART = ctit.CHART
				AND cat.CATEGORY = ctit.CATEGORY
				AND cat.PI_ID = ctit.PI_ID

		--LEFT JOIN [dbo].[CHART_LIST] clist
		--	ON cat.CHART = clist.CHART
		--		AND cat.PI_ID = clist.PI_ID

		LEFT JOIN [dbo].[DEPARTMENT] dep
			ON dep.NUMBER = aj.DEPT_NUMBER
				AND dep.PI_ID = aj.PI_ID

		--LEFT JOIN [dbo].[vw_Financial_Periods] fp
		--	ON MONTH(aj.DATE) = fp.Month_No
		--		AND YEAR(aj.DATE) = fp.Actual_Year

		
	
WHERE cat.CATEGORY NOT IN (1,2,3,4,10) --AND FLAG_ASSET_LIABILITY = 0



	GROUP BY cat.NAME	
		,CAST(CAST(cat.CATEGORY as varchar) + '.' + cast(cat.SORT_ORDER as varchar) as float)
		,cat.SORT_ORDER
		,cat.CATEGORY
		,ctit.TITLE	
		,cat.CHART
		,MONTH(aj.DATE)
		,dep.NAME
		,CASE WHEN FLAG_ASSET_LIABILITY = 1 THEN 'Liability Line' ELSE 'Display Line' END
		,AJ.NOMINAL_CODE
		,YEAR(aj.DATE)
		,cat.Financial_Period
		,cat.Financial_Year
		,cat.PI_ID

		UNION ALL

-----liabilities


--SELECT cat.NAME	as Display_Name
--		,CAST(CAST(cat.CATEGORY as varchar) + '.' + cast(cat.SORT_ORDER as varchar) as float) as Sort_Order
--		,100
--		,ctit.TITLE		as Category_Title
--		,cat.CHART
--		,MONTH(aj.DATE)													as Transaction_Month
--		,dep.NAME		as Department
--		,ISNULL(SUM(aj.Amount * CASE WHEN cat.CATEGORY >= 7 THEN -1 ELSE 1 END),0)	as Actual_Amount
--		,'Display Line'		as Row_Type
--		,AJ.NOMINAL_CODE
--		,YEAR(aj.DATE)													as Tran_Year
--		,cat.Financial_Period
--		,cat.Financial_Year
--		,cat.PI_ID




--  FROM dbo.vw_CategoryandPeriods cat

--		LEFT JOIN  [dbo].[NOMINAL_LEDGER] Nom
--			ON nom.ACCOUNT_REF >= cat.LOW
--			AND nom.ACCOUNT_REF <= cat.HIGH
--			AND nom.PI_ID = cat.PI_ID
			
--		LEFT JOIN [dbo].[AUDIT_JOURNAL] AJ
--			ON nom.ACCOUNT_REF = AJ.NOMINAL_CODE
--				AND  MONTH(aj.DATE) = cat.Month_No
--				AND YEAR(aj.DATE) = cat.Actual_Year
--				AND AJ.DELETED_FLAG = 0
--				AND aj.PI_ID = cat.PI_ID

--		LEFT JOIN [dbo].[CAT_TITLE] ctit
--			ON cat.CHART = ctit.CHART
--				AND cat.CATEGORY = ctit.CATEGORY
--				AND cat.PI_ID = ctit.PI_ID

--		--LEFT JOIN [dbo].[CHART_LIST] clist
--		--	ON cat.CHART = clist.CHART
--		--		AND cat.PI_ID = clist.PI_ID

--		LEFT JOIN [dbo].[DEPARTMENT] dep
--			ON dep.NUMBER = aj.DEPT_NUMBER
--				AND dep.PI_ID = aj.PI_ID

--		--LEFT JOIN [dbo].[vw_Financial_Periods] fp
--		--	ON MONTH(aj.DATE) = fp.Month_No
--		--		AND YEAR(aj.DATE) = fp.Actual_Year

		
	
--WHERE FLAG_ASSET_LIABILITY = 1



--	GROUP BY cat.NAME	
--		,CAST(CAST(cat.CATEGORY as varchar) + '.' + cast(cat.SORT_ORDER as varchar) as float)
--		,cat.SORT_ORDER
--		,cat.CATEGORY
--		,ctit.TITLE	
--		,cat.CHART
--		,MONTH(aj.DATE)
--		,dep.NAME
--		,AJ.NOMINAL_CODE
--		,YEAR(aj.DATE)
--		,cat.Financial_Period
--		,cat.Financial_Year
--		,cat.PI_ID

--		UNION ALL


--- HEADER

SELECT --'<b>' + ctit.TITLE + '</b>'		as Display_Name
 '<b>' + UPPER(ctit.TITLE)	+ '</b>' as Display_Name
		,cat.CATEGORY   as Sort_Order
		,cat.CATEGORY
		,ctit.TITLE		as Category_Title
		,cat.CHART
		,Month(aj.DATE)														as Transaction_Month
		,dep.NAME		as Department
		,0
		,'Header Line'		as Row_Type
		,AJ.NOMINAL_CODE
		,YEAR(aj.DATE)													as Tran_Year
		,Financial_Period
		,Financial_Year
		,cat.PI_ID

FROM dbo.vw_CategoryandPeriods cat

		LEFT JOIN  [dbo].[NOMINAL_LEDGER] Nom
			ON nom.ACCOUNT_REF >= cat.LOW
			AND nom.ACCOUNT_REF <= cat.HIGH
			AND nom.PI_ID = cat.PI_ID
			
		LEFT JOIN [dbo].[AUDIT_JOURNAL] AJ
			ON nom.ACCOUNT_REF = AJ.NOMINAL_CODE
				AND  MONTH(aj.DATE) = cat.Month_No
				AND YEAR(aj.DATE) = cat.Actual_Year
				AND AJ.DELETED_FLAG = 0
				AND aj.PI_ID = cat.PI_ID

		LEFT JOIN [dbo].[CAT_TITLE] ctit
			ON cat.CHART = ctit.CHART
				AND cat.CATEGORY = ctit.CATEGORY
				AND cat.PI_ID = ctit.PI_ID

		--LEFT JOIN [dbo].[CHART_LIST] clist
		--	ON cat.CHART = clist.CHART
		--		AND cat.PI_ID = clist.PI_ID

		LEFT JOIN [dbo].[DEPARTMENT] dep
			ON dep.NUMBER = aj.DEPT_NUMBER
				AND dep.PI_ID = aj.PI_ID

		
WHERE cat.CATEGORY NOT IN (1,2,3,4,10)


	GROUP BY ctit.TITLE		
		,cat.CATEGORY
		,ctit.TITLE	
		,cat.CHART
		,Month(aj.DATE)
		,dep.NAME   
			,AJ.NOMINAL_CODE
		,YEAR(aj.DATE)
		,Financial_Period
		,Financial_Year
		,cat.PI_ID

		UNION ALL



----- sub total

SELECT '<b>TOTAL ' + UPPER(ctit.TITLE) +'</b>'		as Display_Name
		,(cat.CATEGORY + 1) - 0.000002   as Sort_Order
		,cat.CATEGORY
		,ctit.TITLE	as Category_Title
		,cat.CHART
		,MONTH(aj.DATE)														as Transaction_Month
		,dep.NAME		as Department
		,ISNULL(SUM(aj.Amount * CASE WHEN cat.CATEGORY >= 7 THEN -1 ELSE 1 END),0)	as Actual_Amount
		,'Subtotal Line'		as Row_Type
			,AJ.NOMINAL_CODE
			,YEAR(aj.DATE)													as Tran_Year
		,Financial_Period
		,Financial_Year
		,cat.PI_ID


FROM dbo.vw_CategoryandPeriods cat

		LEFT JOIN  [dbo].[NOMINAL_LEDGER] Nom
			ON nom.ACCOUNT_REF >= cat.LOW
			AND nom.ACCOUNT_REF <= cat.HIGH
			AND nom.PI_ID = cat.PI_ID
			
		LEFT JOIN [dbo].[AUDIT_JOURNAL] AJ
			ON nom.ACCOUNT_REF = AJ.NOMINAL_CODE
				AND  MONTH(aj.DATE) = cat.Month_No
				AND YEAR(aj.DATE) = cat.Actual_Year
				AND AJ.DELETED_FLAG = 0
				AND aj.PI_ID = cat.PI_ID

		LEFT JOIN [dbo].[CAT_TITLE] ctit
			ON cat.CHART = ctit.CHART
				AND cat.CATEGORY = ctit.CATEGORY
				AND cat.PI_ID = ctit.PI_ID

		--LEFT JOIN [dbo].[CHART_LIST] clist
		--	ON cat.CHART = clist.CHART
		--		AND cat.PI_ID = clist.PI_ID

		LEFT JOIN [dbo].[DEPARTMENT] dep
			ON dep.NUMBER = aj.DEPT_NUMBER
				AND dep.PI_ID = aj.PI_ID

		
WHERE cat.CATEGORY NOT IN (1,2,3,4,10) --AND FLAG_ASSET_LIABILITY = 0


	GROUP BY ctit.TITLE		
		,cat.CATEGORY   
		,ctit.TITLE	
		,cat.CHART
		,Month(aj.DATE)													
		,dep.NAME
		,AJ.NOMINAL_CODE
		,YEAR(aj.DATE)
		,Financial_Period
		,Financial_Year
		,cat.PI_ID

		UNION ALL

--- Build the line splits

SELECT ' '	as Display_Name
		,(cat.CATEGORY + 1) - 0.000001		as Sort_Order
		,cat.CATEGORY
		,ctit.TITLE							as Category_Title
		,cat.CHART
		,MONTH(aj.DATE)							as Transaction_Month
		,dep.NAME							as Department
		,NULL									as Actual_Amount
		,'Line Split'						as Row_Type
			,AJ.NOMINAL_CODE
			,YEAR(aj.DATE)													as Tran_Year
		,Financial_Period
		,Financial_Year
		,cat.PI_ID


FROM dbo.vw_CategoryandPeriods cat

		LEFT JOIN  [dbo].[NOMINAL_LEDGER] Nom
			ON nom.ACCOUNT_REF >= cat.LOW
			AND nom.ACCOUNT_REF <= cat.HIGH
			AND nom.PI_ID = cat.PI_ID
			
		LEFT JOIN [dbo].[AUDIT_JOURNAL] AJ
			ON nom.ACCOUNT_REF = AJ.NOMINAL_CODE
				AND  MONTH(aj.DATE) = cat.Month_No
				AND YEAR(aj.DATE) = cat.Actual_Year
				AND AJ.DELETED_FLAG = 0
				AND aj.PI_ID = cat.PI_ID

		LEFT JOIN [dbo].[CAT_TITLE] ctit
			ON cat.CHART = ctit.CHART
				AND cat.CATEGORY = ctit.CATEGORY
				AND cat.PI_ID = ctit.PI_ID

		--LEFT JOIN [dbo].[CHART_LIST] clist
		--	ON cat.CHART = clist.CHART
		--		AND cat.PI_ID = clist.PI_ID

		LEFT JOIN [dbo].[DEPARTMENT] dep
			ON dep.NUMBER = aj.DEPT_NUMBER
				AND dep.PI_ID = aj.PI_ID

		
WHERE cat.CATEGORY NOT IN (1,2,3,4,10)


GROUP BY ctit.TITLE		
		,cat.CATEGORY   
		,ctit.TITLE	
		,cat.CHART
		,Month(aj.DATE)													
		,dep.NAME
		,AJ.NOMINAL_CODE
		,YEAR(aj.DATE)
		,Financial_Period
		,Financial_Year
		,cat.PI_ID

--				UNION ALL

--- Build the line break

--SELECT ' '	as Display_Name
--		,2.9999999999		as Sort_Order
--		,cat.CATEGORY
--		,ctit.TITLE							as Category_Title
--		,cat.CHART
--		,MONTH(aj.DATE)							as Transaction_Month
--		,dep.NAME							as Department
--		,NULL									as Actual_Amount
--		,'Line Split'						as Row_Type
--			,AJ.NOMINAL_CODE
--			,YEAR(aj.DATE)													as Tran_Year
--		,Financial_Period
--		,Financial_Year
--		,cat.PI_ID


--FROM dbo.vw_CategoryandPeriods cat

--		LEFT JOIN  [dbo].[NOMINAL_LEDGER] Nom
--			ON nom.ACCOUNT_REF >= cat.LOW
--			AND nom.ACCOUNT_REF <= cat.HIGH
--			AND nom.PI_ID = cat.PI_ID
			
--		LEFT JOIN [dbo].[AUDIT_JOURNAL] AJ
--			ON nom.ACCOUNT_REF = AJ.NOMINAL_CODE
--				AND  MONTH(aj.DATE) = cat.Month_No
--				AND YEAR(aj.DATE) = cat.Actual_Year
--				AND AJ.DELETED_FLAG = 0
--				AND aj.PI_ID = cat.PI_ID

--		LEFT JOIN [dbo].[CAT_TITLE] ctit
--			ON cat.CHART = ctit.CHART
--				AND cat.CATEGORY = ctit.CATEGORY
--				AND cat.PI_ID = ctit.PI_ID

--		--LEFT JOIN [dbo].[CHART_LIST] clist
--		--	ON cat.CHART = clist.CHART
--		--		AND cat.PI_ID = clist.PI_ID

--		LEFT JOIN [dbo].[DEPARTMENT] dep
--			ON dep.NUMBER = aj.DEPT_NUMBER
--				AND dep.PI_ID = aj.PI_ID

		
--WHERE cat.CATEGORY NOT IN (1,2,3,4)


--GROUP BY ctit.TITLE		
--		,cat.CATEGORY   
--		,ctit.TITLE	
--		,cat.CHART
--		,Month(aj.DATE)													
--		,dep.NAME
--		,AJ.NOMINAL_CODE
--		,YEAR(aj.DATE)
--		,Financial_Period
--		,Financial_Year
--		,cat.PI_ID


--		UNION ALL



------- Gross Profit

--SELECT '<b>GROSS PROFIT/LOSS </b>'		as Display_Name
--		,(3) - 0.000001   as Sort_Order
--		,cat.CATEGORY
--		,ctit.TITLE	as Category_Title
--		,cat.CHART
--		,MONTH(aj.DATE)														as Transaction_Month
--		,dep.NAME		as Department
--		,ISNULL(SUM(CASE WHEN cat.CATEGORY = 1 THEN aj.Amount * -1 ELSE 0 END)
--			- 
--		 SUM(CASE WHEN cat.CATEGORY = 2 THEN aj.Amount ELSE 0 END),0)		as Actual_Amount
--		,'GROSS PROFIT Line'		as Row_Type
--			,AJ.NOMINAL_CODE
--		,YEAR(aj.DATE)													as Tran_Year
--		,Financial_Period
--		,Financial_Year
--		,cat.PI_ID


--FROM dbo.vw_CategoryandPeriods cat

--		LEFT JOIN  [dbo].[NOMINAL_LEDGER] Nom
--			ON nom.ACCOUNT_REF >= cat.LOW
--			AND nom.ACCOUNT_REF <= cat.HIGH
--			AND nom.PI_ID = cat.PI_ID
			
--		LEFT JOIN [dbo].[AUDIT_JOURNAL] AJ
--			ON nom.ACCOUNT_REF = AJ.NOMINAL_CODE
--				AND  MONTH(aj.DATE) = cat.Month_No
--				AND YEAR(aj.DATE) = cat.Actual_Year
--				AND AJ.DELETED_FLAG = 0
--				AND aj.PI_ID = cat.PI_ID

--		LEFT JOIN [dbo].[CAT_TITLE] ctit
--			ON cat.CHART = ctit.CHART
--				AND cat.CATEGORY = ctit.CATEGORY
--				AND cat.PI_ID = ctit.PI_ID

--		--LEFT JOIN [dbo].[CHART_LIST] clist
--		--	ON cat.CHART = clist.CHART
--		--		AND cat.PI_ID = clist.PI_ID

--		LEFT JOIN [dbo].[DEPARTMENT] dep
--			ON dep.NUMBER = aj.DEPT_NUMBER
--				AND dep.PI_ID = aj.PI_ID
		
--WHERE cat.CATEGORY NOT IN (1,2)
		

--	GROUP BY ctit.TITLE		
--		,cat.CATEGORY   
--		,ctit.TITLE	
--		,cat.CHART
--		,Month(aj.DATE)													
--		,dep.NAME
--		,AJ.NOMINAL_CODE
--		,YEAR(aj.DATE)
--		,Financial_Period
--		,Financial_Year
--		,cat.PI_ID

--	UNION ALL



------- NET Profit Before Tax

--SELECT '<b>NET PROFIT/LOSS BEFORE TAX </b>'		as Display_Name
--		,9   as Sort_Order
--		,9
--		,ctit.TITLE	as Category_Title
--		,cat.CHART
--		,MONTH(aj.DATE)														as Transaction_Month
--		,dep.NAME		as Department
--		,ISNULL(SUM(CASE WHEN cat.CATEGORY = 1 THEN aj.Amount * -1 ELSE 0 END)
--			- 
--		 SUM(CASE WHEN cat.CATEGORY <> 1 THEN aj.Amount ELSE 0 END),0)		as Actual_Amount
--		,'NET PROFIT Line'		as Row_Type
--			,AJ.NOMINAL_CODE
--		,YEAR(aj.DATE)													as Tran_Year
--		,Financial_Period
--		,Financial_Year
--		,cat.PI_ID


--FROM dbo.vw_CategoryandPeriods cat

--		LEFT JOIN  [dbo].[NOMINAL_LEDGER] Nom
--			ON nom.ACCOUNT_REF >= cat.LOW
--			AND nom.ACCOUNT_REF <= cat.HIGH
--			AND nom.PI_ID = cat.PI_ID
			
--		LEFT JOIN [dbo].[AUDIT_JOURNAL] AJ
--			ON nom.ACCOUNT_REF = AJ.NOMINAL_CODE
--				AND  MONTH(aj.DATE) = cat.Month_No
--				AND YEAR(aj.DATE) = cat.Actual_Year
--				AND AJ.DELETED_FLAG = 0
--				AND aj.PI_ID = cat.PI_ID

--		LEFT JOIN [dbo].[CAT_TITLE] ctit
--			ON cat.CHART = ctit.CHART
--				AND cat.CATEGORY = ctit.CATEGORY
--				AND cat.PI_ID = ctit.PI_ID

--		LEFT JOIN [dbo].[CHART_LIST] clist
--			ON cat.CHART = clist.CHART
--				AND cat.PI_ID = clist.PI_ID

--		LEFT JOIN [dbo].[DEPARTMENT] dep
--			ON dep.NUMBER = aj.DEPT_NUMBER
--				AND dep.PI_ID = aj.PI_ID
		
--WHERE cat.CATEGORY NOT IN (1,2,3,4)


--	GROUP BY ctit.TITLE		
--		--,cat.CATEGORY   
--		--,cat.CATEGORY
--		,ctit.TITLE	
--		,cat.CHART
--		,Month(aj.DATE)													
--		,dep.NAME
--		,AJ.NOMINAL_CODE
--		,YEAR(aj.DATE)
--		,Financial_Period
--		,Financial_Year
--		,cat.PI_ID

--	UNION ALL



------- NET Profit

--SELECT '<b>NET PROFIT/LOSS AFTER TAX  </b>'		as Display_Name
--		,11   as Sort_Order
--		,11
--		,ctit.TITLE	as Category_Title
--		,cat.CHART
--		,MONTH(aj.DATE)														as Transaction_Month
--		,dep.NAME		as Department
--		,ISNULL(SUM(CASE WHEN cat.CATEGORY = 1 THEN aj.Amount * -1 ELSE 0 END)
--			- 
--		 SUM(CASE WHEN cat.CATEGORY <> 1 THEN aj.Amount ELSE 0 END),0)		as Actual_Amount
--		,'NET PROFIT Line'		as Row_Type
--			,AJ.NOMINAL_CODE
--		,YEAR(aj.DATE)													as Tran_Year
--		,Financial_Period
--		,Financial_Year
--		,cat.PI_ID


--FROM dbo.vw_CategoryandPeriods cat

--		LEFT JOIN  [dbo].[NOMINAL_LEDGER] Nom
--			ON nom.ACCOUNT_REF >= cat.LOW
--			AND nom.ACCOUNT_REF <= cat.HIGH
--			AND nom.PI_ID = cat.PI_ID
			
--		LEFT JOIN [dbo].[AUDIT_JOURNAL] AJ
--			ON nom.ACCOUNT_REF = AJ.NOMINAL_CODE
--				AND  MONTH(aj.DATE) = cat.Month_No
--				AND YEAR(aj.DATE) = cat.Actual_Year
--				AND AJ.DELETED_FLAG = 0
--				AND aj.PI_ID = cat.PI_ID

--		LEFT JOIN [dbo].[CAT_TITLE] ctit
--			ON cat.CHART = ctit.CHART
--				AND cat.CATEGORY = ctit.CATEGORY
--				AND cat.PI_ID = ctit.PI_ID

--		LEFT JOIN [dbo].[CHART_LIST] clist
--			ON cat.CHART = clist.CHART
--				AND cat.PI_ID = clist.PI_ID

--		LEFT JOIN [dbo].[DEPARTMENT] dep
--			ON dep.NUMBER = aj.DEPT_NUMBER
--				AND dep.PI_ID = aj.PI_ID
		
--WHERE cat.CATEGORY NOT IN (1,2,3,4,10)


--	GROUP BY ctit.TITLE		
--		--,cat.CATEGORY   
--		--,cat.CATEGORY
--		,ctit.TITLE	
--		,cat.CHART		
--		,Month(aj.DATE)													
--		,dep.NAME
--		,AJ.NOMINAL_CODE
--		,YEAR(aj.DATE)
--		,Financial_Period
--		,Financial_Year
--		,cat.PI_ID



--GO


GO
/****** Object:  View [dbo].[vw_Project_Activity]    Script Date: 09/12/2019 14:43:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


 CREATE VIEW [dbo].[vw_Project_Activity]
 
 AS 
 
 SELECT	pt.PROJECT_TRAN_ID
		,ausp.TRAN_NUMBER as Audit_Trail_Number
		,pt.PROJECT_ID
		,pt.COST_CODE_ID
		,pct.COST_TYPE_ID
		,pt.DATE as Project_Tran_Date
		,[TYPE]
		,[ACCOUNT_REF]  as Account_Ref
		,[INV_REF] as Ref   
		,[DETAILS]
		 ,[NOMINAL_CODE] 
		 ,''	as Resource
		 ,0.0	as Quantity
		 ,0.0	as Rate
        ,[NET_AMOUNT]
		,[DEPT_NUMBER]
		,pt.RECORD_CREATE_DATE
		,pt.RECORD_MODIFY_DATE
		,pt.PI_ID
     

     
  FROM [dbo].[AUDIT_SPLIT] ausp
		INNER JOIN [dbo].[PROJECT_TRAN] pt
			ON ausp.TRAN_NUMBER = pt.AUDIT_TRAIL_ID
				AND ausp.PI_ID = pt.PI_ID
		INNER JOIN dbo.PROJECT_COST_CODE pct
			ON pt.COST_CODE_ID = pct.COST_CODE_ID
				AND pt.PI_ID = pct.PI_ID


UNION ALL



SELECT   
		pot.[PROJECT_TRAN_ID]
		,''
		,pt.PROJECT_ID
		,pt.COST_CODE_ID
		,pct.COST_TYPE_ID
		,pt.DATE as Project_Tran_Date
		,[TYPE]
		,pot.ACCOUNT_REF  as Customer_Ref
      ,pot.[REFERENCE]
      ,[DETAILS]
	  ,[NOMINAL_CODE]
      ,pr.[NAME]	as Resource
      ,[QUANTITY] * -1
      ,pot.[RATE]
	  ,[QUANTITY] * pot.[RATE] AS NET_AMOUNT
	  ,DEPT_NUMBER
	  	,pt.RECORD_CREATE_DATE
		,pt.RECORD_MODIFY_DATE
		,pt.PI_ID
  

  FROM [dbo].[PROJECT_ONLY_TRAN] pot
		INNER JOIN [dbo].[PROJECT_TRAN] pt
			ON pot.PROJECT_TRAN_ID = pt.PROJECT_TRAN_ID
				AND pot.PI_ID = pt.PI_ID
		
		INNER JOIN dbo.PROJECT_COST_CODE pct
			ON pt.COST_CODE_ID = pct.COST_CODE_ID
				AND pt.PI_ID = pct.PI_ID

			LEFT JOIN [dbo].[PROJECT_RESOURCE] pr
			ON pot.RESOURCE_ID = pr.RESOURCE_ID
				AND pot.PI_ID = pr.PI_ID
GO
/****** Object:  View [dbo].[vw_Project_Cost_Codes]    Script Date: 09/12/2019 14:43:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



 CREATE VIEW [dbo].[vw_Project_Cost_Codes]
 
 AS 
 

 SELECT PROJECT_ID
		,COST_CODE_ID   
		,COST_TYPE_ID
		,COST_CODE_DESCRIPTION
		,COST_TYPE
		,SUM(Budget) as Budget
		,SUM(Actual) as Actual
		,PI_ID


FROM (	
 SELECT pt.PROJECT_ID
		,pt.COST_CODE_ID   
		,cc.COST_TYPE_ID
		,cc.DESCRIPTION			as COST_CODE_DESCRIPTION
		,pct.NAME				as COST_TYPE
		,0.0					as Budget
		,(ac.NET_AMOUNT * -1)  as Actual
		,pt.PI_ID
		

     
  FROM [dbo].[PROJECT_TRAN] pt
		INNER JOIN [dbo].[PROJECT_COST_CODE] cc
			ON pt.COST_CODE_ID = cc.COST_CODE_ID
				AND pt.PI_ID = cc.PI_ID
		INNER JOIN [dbo].[PROJECT_COST_TYPE] pct
			ON cc.COST_TYPE_ID = pct.COST_TYPE_ID
				AND cc.PI_ID = pct.PI_ID
		INNER JOIN [dbo].[vw_Project_Activity] ac
			ON pt.PROJECT_TRAN_ID = ac.PROJECT_TRAN_ID
				AND pt.PI_ID = ac.PI_ID





UNION ALL


SELECT  [PROJECT_ID]
      ,pb.[COST_CODE_ID]
	  ,cc.COST_TYPE_ID
	  ,cc.DESCRIPTION	as COST_CODE_DESCRIPTION
      ,pct.NAME			as COST_TYPE
	  ,pb.AMOUNT		as Budget
	,0.0			as Actual
	,pb.PI_ID

  FROM [dbo].[PROJECT_BUDGET] pb
		INNER JOIN [dbo].[PROJECT_COST_CODE] cc
			ON pb.COST_CODE_ID = cc.COST_CODE_ID
				AND pb.PI_ID = cc.PI_ID
		INNER JOIN [dbo].[PROJECT_COST_TYPE] pct
			ON cc.COST_TYPE_ID = pct.COST_TYPE_ID
				AND cc.PI_ID = pct.PI_ID

)sub


GROUP BY PROJECT_ID
		,COST_CODE_ID   
		,COST_TYPE_ID
		,COST_CODE_DESCRIPTION
		,COST_TYPE
		,PI_ID
GO
/****** Object:  View [dbo].[vw_PandL]    Script Date: 09/12/2019 14:43:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





CREATE VIEW [dbo].[vw_PandL] AS




SELECT cat.NAME	as Display_Name
		,CAST(CAST(cat.CATEGORY as varchar) + '.' + cast(cat.SORT_ORDER as varchar) as float) as Sort_Order
		,cat.CATEGORY
		,ctit.TITLE		as Category_Title
		,cat.CHART
		,MONTH(aj.DATE)													as Transaction_Month
		,dep.NAME		as Department
		,ISNULL(SUM(aj.Amount * CASE WHEN cat.CATEGORY = 1 THEN -1 ELSE 1 END),0)	as Actual_Amount
		,'Display Line'		as Row_Type
		,AJ.NOMINAL_CODE
		,YEAR(aj.DATE)													as Tran_Year
		,cat.Financial_Period
		,cat.Financial_Year
		,cat.PI_ID



  FROM dbo.vw_CategoryandPeriods cat

		LEFT JOIN  [dbo].[NOMINAL_LEDGER] Nom
			ON nom.ACCOUNT_REF >= cat.LOW
			AND nom.ACCOUNT_REF <= cat.HIGH
			AND nom.PI_ID = cat.PI_ID
			
		LEFT JOIN [dbo].[AUDIT_JOURNAL] AJ
			ON nom.ACCOUNT_REF = AJ.NOMINAL_CODE
				AND  MONTH(aj.DATE) = cat.Month_No
				AND YEAR(aj.DATE) = cat.Actual_Year
				AND AJ.DELETED_FLAG = 0
				AND aj.PI_ID = cat.PI_ID

		LEFT JOIN [dbo].[CAT_TITLE] ctit
			ON cat.CHART = ctit.CHART
				AND cat.CATEGORY = ctit.CATEGORY
				AND cat.PI_ID = ctit.PI_ID

		--LEFT JOIN [dbo].[CHART_LIST] clist
		--	ON cat.CHART = clist.CHART
		--		AND cat.PI_ID = clist.PI_ID

		LEFT JOIN [dbo].[DEPARTMENT] dep
			ON dep.NUMBER = aj.DEPT_NUMBER
				AND dep.PI_ID = aj.PI_ID

		--LEFT JOIN [dbo].[vw_Financial_Periods] fp
		--	ON MONTH(aj.DATE) = fp.Month_No
		--		AND YEAR(aj.DATE) = fp.Actual_Year

		
	
WHERE cat.CATEGORY IN (1,2,3,4,10)



	GROUP BY cat.NAME	
		,CAST(CAST(cat.CATEGORY as varchar) + '.' + cast(cat.SORT_ORDER as varchar) as float)
		,cat.SORT_ORDER
		,cat.CATEGORY
		,ctit.TITLE	
		,cat.CHART
		,MONTH(aj.DATE)
		,dep.NAME
		,AJ.NOMINAL_CODE
		,YEAR(aj.DATE)
		,cat.Financial_Period
		,cat.Financial_Year
		,cat.PI_ID

		UNION ALL

--- HEADER

SELECT --'<b>' + ctit.TITLE + '</b>'		as Display_Name
 '<b>' + UPPER(ctit.TITLE)	+ '</b>' as Display_Name
		,cat.CATEGORY   as Sort_Order
		,cat.CATEGORY
		,ctit.TITLE		as Category_Title
		,cat.CHART
		,Month(aj.DATE)														as Transaction_Month
		,dep.NAME		as Department
		,0
		,'Header Line'		as Row_Type
		,AJ.NOMINAL_CODE
		,YEAR(aj.DATE)													as Tran_Year
		,Financial_Period
		,Financial_Year
		,cat.PI_ID

FROM dbo.vw_CategoryandPeriods cat

		LEFT JOIN  [dbo].[NOMINAL_LEDGER] Nom
			ON nom.ACCOUNT_REF >= cat.LOW
			AND nom.ACCOUNT_REF <= cat.HIGH
			AND nom.PI_ID = cat.PI_ID
			
		LEFT JOIN [dbo].[AUDIT_JOURNAL] AJ
			ON nom.ACCOUNT_REF = AJ.NOMINAL_CODE
				AND  MONTH(aj.DATE) = cat.Month_No
				AND YEAR(aj.DATE) = cat.Actual_Year
				AND AJ.DELETED_FLAG = 0
				AND aj.PI_ID = cat.PI_ID

		LEFT JOIN [dbo].[CAT_TITLE] ctit
			ON cat.CHART = ctit.CHART
				AND cat.CATEGORY = ctit.CATEGORY
				AND cat.PI_ID = ctit.PI_ID

		--LEFT JOIN [dbo].[CHART_LIST] clist
		--	ON cat.CHART = clist.CHART
		--		AND cat.PI_ID = clist.PI_ID

		LEFT JOIN [dbo].[DEPARTMENT] dep
			ON dep.NUMBER = aj.DEPT_NUMBER
				AND dep.PI_ID = aj.PI_ID

		
WHERE cat.CATEGORY IN (1,2,3,4)


	GROUP BY ctit.TITLE		
		,cat.CATEGORY
		,ctit.TITLE	
		,cat.CHART
		,Month(aj.DATE)
		,dep.NAME   
			,AJ.NOMINAL_CODE
		,YEAR(aj.DATE)
		,Financial_Period
		,Financial_Year
		,cat.PI_ID

		UNION ALL



----- sub total

SELECT '<b>TOTAL ' + UPPER(ctit.TITLE) +'</b>'		as Display_Name
		,(cat.CATEGORY + 1) - 0.000002   as Sort_Order
		,cat.CATEGORY
		,ctit.TITLE	as Category_Title
		,cat.CHART
		,MONTH(aj.DATE)														as Transaction_Month
		,dep.NAME		as Department
		,ISNULL(SUM(aj.Amount * CASE WHEN cat.CATEGORY = 1 THEN -1 ELSE 1 END),0)	as Actual_Amount
		,'Subtotal Line'		as Row_Type
			,AJ.NOMINAL_CODE
			,YEAR(aj.DATE)													as Tran_Year
		,Financial_Period
		,Financial_Year
		,cat.PI_ID


FROM dbo.vw_CategoryandPeriods cat

		LEFT JOIN  [dbo].[NOMINAL_LEDGER] Nom
			ON nom.ACCOUNT_REF >= cat.LOW
			AND nom.ACCOUNT_REF <= cat.HIGH
			AND nom.PI_ID = cat.PI_ID
			
		LEFT JOIN [dbo].[AUDIT_JOURNAL] AJ
			ON nom.ACCOUNT_REF = AJ.NOMINAL_CODE
				AND  MONTH(aj.DATE) = cat.Month_No
				AND YEAR(aj.DATE) = cat.Actual_Year
				AND AJ.DELETED_FLAG = 0
				AND aj.PI_ID = cat.PI_ID

		LEFT JOIN [dbo].[CAT_TITLE] ctit
			ON cat.CHART = ctit.CHART
				AND cat.CATEGORY = ctit.CATEGORY
				AND cat.PI_ID = ctit.PI_ID

		--LEFT JOIN [dbo].[CHART_LIST] clist
		--	ON cat.CHART = clist.CHART
		--		AND cat.PI_ID = clist.PI_ID

		LEFT JOIN [dbo].[DEPARTMENT] dep
			ON dep.NUMBER = aj.DEPT_NUMBER
				AND dep.PI_ID = aj.PI_ID

		
WHERE cat.CATEGORY IN (1,2,3,4)


	GROUP BY ctit.TITLE		
		,cat.CATEGORY   
		,ctit.TITLE	
		,cat.CHART
		,Month(aj.DATE)													
		,dep.NAME
		,AJ.NOMINAL_CODE
		,YEAR(aj.DATE)
		,Financial_Period
		,Financial_Year
		,cat.PI_ID

		UNION ALL

--- Build the line splits

SELECT ' '	as Display_Name
		,(cat.CATEGORY + 1) - 0.000001		as Sort_Order
		,cat.CATEGORY
		,ctit.TITLE							as Category_Title
		,cat.CHART
		,MONTH(aj.DATE)							as Transaction_Month
		,dep.NAME							as Department
		,NULL									as Actual_Amount
		,'Line Split'						as Row_Type
			,AJ.NOMINAL_CODE
			,YEAR(aj.DATE)													as Tran_Year
		,Financial_Period
		,Financial_Year
		,cat.PI_ID


FROM dbo.vw_CategoryandPeriods cat

		LEFT JOIN  [dbo].[NOMINAL_LEDGER] Nom
			ON nom.ACCOUNT_REF >= cat.LOW
			AND nom.ACCOUNT_REF <= cat.HIGH
			AND nom.PI_ID = cat.PI_ID
			
		LEFT JOIN [dbo].[AUDIT_JOURNAL] AJ
			ON nom.ACCOUNT_REF = AJ.NOMINAL_CODE
				AND  MONTH(aj.DATE) = cat.Month_No
				AND YEAR(aj.DATE) = cat.Actual_Year
				AND AJ.DELETED_FLAG = 0
				AND aj.PI_ID = cat.PI_ID

		LEFT JOIN [dbo].[CAT_TITLE] ctit
			ON cat.CHART = ctit.CHART
				AND cat.CATEGORY = ctit.CATEGORY
				AND cat.PI_ID = ctit.PI_ID

		--LEFT JOIN [dbo].[CHART_LIST] clist
		--	ON cat.CHART = clist.CHART
		--		AND cat.PI_ID = clist.PI_ID

		LEFT JOIN [dbo].[DEPARTMENT] dep
			ON dep.NUMBER = aj.DEPT_NUMBER
				AND dep.PI_ID = aj.PI_ID

		
WHERE cat.CATEGORY IN (1,2,3,4)


GROUP BY ctit.TITLE		
		,cat.CATEGORY   
		,ctit.TITLE	
		,cat.CHART
		,Month(aj.DATE)													
		,dep.NAME
		,AJ.NOMINAL_CODE
		,YEAR(aj.DATE)
		,Financial_Period
		,Financial_Year
		,cat.PI_ID

				UNION ALL

--- Build the line break

SELECT ' '	as Display_Name
		,2.9999999999		as Sort_Order
		,cat.CATEGORY
		,ctit.TITLE							as Category_Title
		,cat.CHART
		,MONTH(aj.DATE)							as Transaction_Month
		,dep.NAME							as Department
		,NULL									as Actual_Amount
		,'Line Split'						as Row_Type
			,AJ.NOMINAL_CODE
			,YEAR(aj.DATE)													as Tran_Year
		,Financial_Period
		,Financial_Year
		,cat.PI_ID


FROM dbo.vw_CategoryandPeriods cat

		LEFT JOIN  [dbo].[NOMINAL_LEDGER] Nom
			ON nom.ACCOUNT_REF >= cat.LOW
			AND nom.ACCOUNT_REF <= cat.HIGH
			AND nom.PI_ID = cat.PI_ID
			
		LEFT JOIN [dbo].[AUDIT_JOURNAL] AJ
			ON nom.ACCOUNT_REF = AJ.NOMINAL_CODE
				AND  MONTH(aj.DATE) = cat.Month_No
				AND YEAR(aj.DATE) = cat.Actual_Year
				AND AJ.DELETED_FLAG = 0
				AND aj.PI_ID = cat.PI_ID

		LEFT JOIN [dbo].[CAT_TITLE] ctit
			ON cat.CHART = ctit.CHART
				AND cat.CATEGORY = ctit.CATEGORY
				AND cat.PI_ID = ctit.PI_ID

		--LEFT JOIN [dbo].[CHART_LIST] clist
		--	ON cat.CHART = clist.CHART
		--		AND cat.PI_ID = clist.PI_ID

		LEFT JOIN [dbo].[DEPARTMENT] dep
			ON dep.NUMBER = aj.DEPT_NUMBER
				AND dep.PI_ID = aj.PI_ID

		
WHERE cat.CATEGORY IN (1,2,3,4)


GROUP BY ctit.TITLE		
		,cat.CATEGORY   
		,ctit.TITLE	
		,cat.CHART
		,Month(aj.DATE)													
		,dep.NAME
		,AJ.NOMINAL_CODE
		,YEAR(aj.DATE)
		,Financial_Period
		,Financial_Year
		,cat.PI_ID


		UNION ALL



----- Gross Profit

SELECT '<b>GROSS PROFIT/LOSS </b>'		as Display_Name
		,(3) - 0.000001   as Sort_Order
		,cat.CATEGORY
		,ctit.TITLE	as Category_Title
		,cat.CHART
		,MONTH(aj.DATE)														as Transaction_Month
		,dep.NAME		as Department
		,ISNULL(SUM(CASE WHEN cat.CATEGORY = 1 THEN aj.Amount * -1 ELSE 0 END)
			- 
		 SUM(CASE WHEN cat.CATEGORY = 2 THEN aj.Amount ELSE 0 END),0)		as Actual_Amount
		,'GROSS PROFIT Line'		as Row_Type
			,AJ.NOMINAL_CODE
		,YEAR(aj.DATE)													as Tran_Year
		,Financial_Period
		,Financial_Year
		,cat.PI_ID


FROM dbo.vw_CategoryandPeriods cat

		LEFT JOIN  [dbo].[NOMINAL_LEDGER] Nom
			ON nom.ACCOUNT_REF >= cat.LOW
			AND nom.ACCOUNT_REF <= cat.HIGH
			AND nom.PI_ID = cat.PI_ID
			
		LEFT JOIN [dbo].[AUDIT_JOURNAL] AJ
			ON nom.ACCOUNT_REF = AJ.NOMINAL_CODE
				AND  MONTH(aj.DATE) = cat.Month_No
				AND YEAR(aj.DATE) = cat.Actual_Year
				AND AJ.DELETED_FLAG = 0
				AND aj.PI_ID = cat.PI_ID

		LEFT JOIN [dbo].[CAT_TITLE] ctit
			ON cat.CHART = ctit.CHART
				AND cat.CATEGORY = ctit.CATEGORY
				AND cat.PI_ID = ctit.PI_ID

		--LEFT JOIN [dbo].[CHART_LIST] clist
		--	ON cat.CHART = clist.CHART
		--		AND cat.PI_ID = clist.PI_ID

		LEFT JOIN [dbo].[DEPARTMENT] dep
			ON dep.NUMBER = aj.DEPT_NUMBER
				AND dep.PI_ID = aj.PI_ID
		
WHERE cat.CATEGORY IN (1,2)
		

	GROUP BY ctit.TITLE		
		,cat.CATEGORY   
		,ctit.TITLE	
		,cat.CHART
		,Month(aj.DATE)													
		,dep.NAME
		,AJ.NOMINAL_CODE
		,YEAR(aj.DATE)
		,Financial_Period
		,Financial_Year
		,cat.PI_ID

	UNION ALL



----- NET Profit Before Tax

SELECT '<b>NET PROFIT/LOSS BEFORE TAX </b>'		as Display_Name
		,9   as Sort_Order
		,9
		,ctit.TITLE	as Category_Title
		,cat.CHART
		,MONTH(aj.DATE)														as Transaction_Month
		,dep.NAME		as Department
		,ISNULL(SUM(CASE WHEN cat.CATEGORY = 1 THEN aj.Amount * -1 ELSE 0 END)
			- 
		 SUM(CASE WHEN cat.CATEGORY <> 1 THEN aj.Amount ELSE 0 END),0)		as Actual_Amount
		,'NET PROFIT Line'		as Row_Type
			,AJ.NOMINAL_CODE
		,YEAR(aj.DATE)													as Tran_Year
		,Financial_Period
		,Financial_Year
		,cat.PI_ID


FROM dbo.vw_CategoryandPeriods cat

		LEFT JOIN  [dbo].[NOMINAL_LEDGER] Nom
			ON nom.ACCOUNT_REF >= cat.LOW
			AND nom.ACCOUNT_REF <= cat.HIGH
			AND nom.PI_ID = cat.PI_ID
			
		LEFT JOIN [dbo].[AUDIT_JOURNAL] AJ
			ON nom.ACCOUNT_REF = AJ.NOMINAL_CODE
				AND  MONTH(aj.DATE) = cat.Month_No
				AND YEAR(aj.DATE) = cat.Actual_Year
				AND AJ.DELETED_FLAG = 0
				AND aj.PI_ID = cat.PI_ID

		LEFT JOIN [dbo].[CAT_TITLE] ctit
			ON cat.CHART = ctit.CHART
				AND cat.CATEGORY = ctit.CATEGORY
				AND cat.PI_ID = ctit.PI_ID

		LEFT JOIN [dbo].[CHART_LIST] clist
			ON cat.CHART = clist.CHART
				AND cat.PI_ID = clist.PI_ID

		LEFT JOIN [dbo].[DEPARTMENT] dep
			ON dep.NUMBER = aj.DEPT_NUMBER
				AND dep.PI_ID = aj.PI_ID
		
WHERE cat.CATEGORY IN (1,2,3,4)


	GROUP BY ctit.TITLE		
		--,cat.CATEGORY   
		--,cat.CATEGORY
		,ctit.TITLE	
		,cat.CHART
		,Month(aj.DATE)													
		,dep.NAME
		,AJ.NOMINAL_CODE
		,YEAR(aj.DATE)
		,Financial_Period
		,Financial_Year
		,cat.PI_ID

	UNION ALL



----- NET Profit

SELECT '<b>NET PROFIT/LOSS AFTER TAX  </b>'		as Display_Name
		,11   as Sort_Order
		,11
		,ctit.TITLE	as Category_Title
		,cat.CHART
		,MONTH(aj.DATE)														as Transaction_Month
		,dep.NAME		as Department
		,ISNULL(SUM(CASE WHEN cat.CATEGORY = 1 THEN aj.Amount * -1 ELSE 0 END)
			- 
		 SUM(CASE WHEN cat.CATEGORY <> 1 THEN aj.Amount ELSE 0 END),0)		as Actual_Amount
		,'NET PROFIT Line'		as Row_Type
			,AJ.NOMINAL_CODE
		,YEAR(aj.DATE)													as Tran_Year
		,Financial_Period
		,Financial_Year
		,cat.PI_ID


FROM dbo.vw_CategoryandPeriods cat

		LEFT JOIN  [dbo].[NOMINAL_LEDGER] Nom
			ON nom.ACCOUNT_REF >= cat.LOW
			AND nom.ACCOUNT_REF <= cat.HIGH
			AND nom.PI_ID = cat.PI_ID
			
		LEFT JOIN [dbo].[AUDIT_JOURNAL] AJ
			ON nom.ACCOUNT_REF = AJ.NOMINAL_CODE
				AND  MONTH(aj.DATE) = cat.Month_No
				AND YEAR(aj.DATE) = cat.Actual_Year
				AND AJ.DELETED_FLAG = 0
				AND aj.PI_ID = cat.PI_ID

		LEFT JOIN [dbo].[CAT_TITLE] ctit
			ON cat.CHART = ctit.CHART
				AND cat.CATEGORY = ctit.CATEGORY
				AND cat.PI_ID = ctit.PI_ID

		LEFT JOIN [dbo].[CHART_LIST] clist
			ON cat.CHART = clist.CHART
				AND cat.PI_ID = clist.PI_ID

		LEFT JOIN [dbo].[DEPARTMENT] dep
			ON dep.NUMBER = aj.DEPT_NUMBER
				AND dep.PI_ID = aj.PI_ID
		
WHERE cat.CATEGORY IN (1,2,3,4,10)


	GROUP BY ctit.TITLE		
		--,cat.CATEGORY   
		--,cat.CATEGORY
		,ctit.TITLE	
		,cat.CHART		
		,Month(aj.DATE)													
		,dep.NAME
		,AJ.NOMINAL_CODE
		,YEAR(aj.DATE)
		,Financial_Period
		,Financial_Year
		,cat.PI_ID



GO
/****** Object:  View [dbo].[vw_Customer_Invoice_Dates]    Script Date: 09/12/2019 14:43:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



/****** Script for SelectTopNRows command from SSMS  ******/

CREATE VIEW [dbo].[vw_Customer_Invoice_Dates]

AS

SELECT 
		sl.ACCOUNT_REF
		,sl.NAME
		,sl.PI_ID

		,MAX(asp.DATE) as Last_Order_Date
		,MIN(asp.DATE) as First_Order_Date

  FROM [dbo].[SALES_LEDGER] sl
		INNER JOIN [dbo].[AUDIT_SPLIT] asp
			ON sl.ACCOUNT_REF = asp.ACCOUNT_REF
				AND sl.PI_ID = asp.PI_ID

WHERE asp.TYPE = 'SI'

GROUP BY sl.ACCOUNT_REF
		,sl.NAME
				,sl.PI_ID
		
		
GO
/****** Object:  View [dbo].[vw_Customer_Order_Dates]    Script Date: 09/12/2019 14:43:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


/****** Script for SelectTopNRows command from SSMS  ******/

CREATE VIEW [dbo].[vw_Customer_Order_Dates]

AS

SELECT 
		sl.ACCOUNT_REF
		,sl.NAME
		,sl.PI_ID

		,MAX(so.ORDER_DATE) as Last_Order_Date
		,MIN(so.ORDER_DATE) as First_Order_Date

  FROM [dbo].[SALES_LEDGER] sl
		INNER JOIN [dbo].[SALES_ORDER] so
			ON sl.ACCOUNT_REF = so.ACCOUNT_REF
				AND sl.PI_ID = so.PI_ID

GROUP BY sl.ACCOUNT_REF
		,sl.NAME
		,sl.PI_ID
		
		
GO
/****** Object:  View [dbo].[vw_Last_Stock_Price]    Script Date: 09/12/2019 14:43:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE VIEW [dbo].[vw_Last_Stock_Price]

AS

---- Get the last cost price of stock before sale

	SELECT laststock.ORDER_NUMBER
			,laststock.ORDER_DATE
			,st.STOCK_CODE
			,st.COST_PRICE as COST_PRICE_AT_SALE
			,st.PI_ID

	FROM   [dbo].[STOCK_TRAN] st
			INNER JOIN 
					(SELECT 
							so.ORDER_DATE
							,so.ORDER_NUMBER
							,sop.ITEM_NUMBER
							,st.STOCK_CODE
							,so.PI_ID
							--,st.TYPE
							,MAX(st.DATE) as LastStockDateBeforeSale

					  FROM 
						[dbo].[SALES_ORDER] so
						INNER JOIN 	[dbo].[SOP_ITEM] sop
							ON sop.ORDER_NUMBER = so.ORDER_NUMBER
							AND sop.PI_ID = so.PI_ID
						LEFT JOIN [dbo].[STOCK_TRAN] st
							ON st.STOCK_CODE = sop.STOCK_CODE
								AND st.PI_ID = sop.PI_ID
			
					  WHERE TYPE IN ('GI','AI')
							AND	 st.DATE <= so.ORDER_DATE

					  GROUP BY so.ORDER_DATE
								,so.ORDER_NUMBER
								,sop.ITEM_NUMBER
								,st.STOCK_CODE
								,so.PI_ID
					)laststock

		ON st.[DATE] = laststock.LastStockDateBeforeSale
				AND st.STOCK_CODE = laststock.STOCK_CODE
				AND st.PI_ID = laststock.PI_ID


		WHERE st.TYPE IN ('GI','AI')

UNION ALL

---- Now include any items that aren't in the stock_tran table above

	SELECT SO.ORDER_NUMBER
			,So.ORDER_DATE
			,st.STOCK_CODE
			,st.LAST_PURCHASE_PRICE
			,st.PI_ID

	FROM  [dbo].[SALES_ORDER] so
			INNER JOIN 	[dbo].[SOP_ITEM] sop
				ON sop.ORDER_NUMBER = so.ORDER_NUMBER
					AND sop.PI_ID = so.PI_ID
			LEFT JOIN [dbo].[STOCK] st
				ON st.STOCK_CODE = sop.STOCK_CODE
					AND st.PI_ID = sop.PI_ID

	WHERE	st.STOCK_CODE NOT IN (
									SELECT DISTINCT st.STOCK_CODE
			    
									  FROM 
										[dbo].[SALES_ORDER] so
										INNER JOIN 	[dbo].[SOP_ITEM] sop
											ON sop.ORDER_NUMBER = so.ORDER_NUMBER
												AND sop.PI_ID = so.PI_ID
										LEFT JOIN [dbo].[STOCK_TRAN] st
											ON st.STOCK_CODE = sop.STOCK_CODE
												AND st.PI_ID = sop.PI_ID
			
									  WHERE TYPE IN ('GI','AI')
											AND	 st.DATE <= so.ORDER_DATE
									 )

GO
/****** Object:  View [dbo].[vw_Nominal_Budgets]    Script Date: 09/12/2019 14:43:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





CREATE VIEW [dbo].[vw_Nominal_Budgets]

as

		SELECT  [ACCOUNT_REF]
			  ,[ANALYSIS_ID]
			  ,[PERIOD]
			  ,[YEAR]
			  ,[BUDGET]
			  ,sub.[CATEGORY]
			  ,[Budget_Check]
			  ,[Budget_Method]
				,cat.CATEGORY as Budget_Category
				,cat.CHART
				,CASE WHEN cat.CATEGORY NOT IN (1) THEN  [BUDGET] * - 1 ELSE  [BUDGET] END as Profit_Budget
				,sub.PI_ID

				FROM (	SELECT [ACCOUNT_REF]
						  ,[ANALYSIS_ID]
						  ,[PERIOD]
						  ,[YEAR]
						  ,[BUDGET]
						  ,[CATEGORY]
						  ,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 1 THEN 1 ELSE 0 END as Budget_Check
						  ,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method
						  ,PI_ID

					  FROM [dbo].[FINANCIAL_BUDGET]

					  WHERE [RECORD_DELETED] = 0

					UNION ALL

					SELECT  
						[ACCOUNT_REF]
						,0											as [ANALYSIS_ID]
						,1											as [Period]
						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
						,[BUDGET_MTH1]								as Budget
						,0											as [CATEGORY]
						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetCheck
						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method
						,PI_ID

					FROM [dbo].[NOMINAL_LEDGER]
					WHERE [RECORD_DELETED] = 0

					UNION ALL

					SELECT  
						[ACCOUNT_REF]
						,0											as [ANALYSIS_ID]
						,2											as [Period]
						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
						,[BUDGET_MTH2]								as Budget
						,0											as [CATEGORY]
						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetCheck
						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method
						,PI_ID

					FROM [dbo].[NOMINAL_LEDGER]
					WHERE [RECORD_DELETED] = 0

					UNION ALL

					SELECT  
						[ACCOUNT_REF]
						,0											as [ANALYSIS_ID]
						,3										as [Period]
						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
						,[BUDGET_MTH3]								as Budget
						,0											as [CATEGORY]
						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetToUse
						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method
						,PI_ID

					FROM [dbo].[NOMINAL_LEDGER]
					WHERE [RECORD_DELETED] = 0

					UNION ALL

					SELECT  
						[ACCOUNT_REF]
						,0											as [ANALYSIS_ID]
						,4											as [Period]
						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
						,[BUDGET_MTH4]								as Budget
						,0											as [CATEGORY]
						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetToUse
						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method
						,PI_ID

					FROM [dbo].[NOMINAL_LEDGER]
					WHERE [RECORD_DELETED] = 0
     
					 UNION ALL

					SELECT  
						[ACCOUNT_REF]
						,0											as [ANALYSIS_ID]
						,5											as [Period]
						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
						,[BUDGET_MTH5]								as Budget
						,0											as [CATEGORY]
						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetToUse
						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method
						,PI_ID

					FROM [dbo].[NOMINAL_LEDGER]
					WHERE [RECORD_DELETED] = 0
     
					UNION ALL

					SELECT  
						[ACCOUNT_REF]
						,0											as [ANALYSIS_ID]
						,6											as [Period]
						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
						,[BUDGET_MTH6]								as Budget
						,0											as [CATEGORY]
						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetToUse
						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method
						,PI_ID

					FROM [dbo].[NOMINAL_LEDGER]
					WHERE [RECORD_DELETED] = 0

					UNION ALL

					SELECT  
						[ACCOUNT_REF]
						,0											as [ANALYSIS_ID]
						,7											as [Period]
						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
						,[BUDGET_MTH7]								as Budget
						,0											as [CATEGORY]
						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetToUse
						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method
						,PI_ID

					FROM [dbo].[NOMINAL_LEDGER]
					WHERE [RECORD_DELETED] = 0
    
					UNION ALL

					SELECT  
						[ACCOUNT_REF]
						,0											as [ANALYSIS_ID]
						,8											as [Period]
						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
						,[BUDGET_MTH8]								as Budget
						,0											as [CATEGORY]
						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetToUse
						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method
						,PI_ID

					FROM [dbo].[NOMINAL_LEDGER]
					WHERE [RECORD_DELETED] = 0

					UNION ALL

					SELECT  
						[ACCOUNT_REF]
						,0											as [ANALYSIS_ID]
						,9											as [Period]
						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
						,[BUDGET_MTH9]								as Budget
						,0											as [CATEGORY]
						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetToUse
						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method
						,PI_ID

					FROM [dbo].[NOMINAL_LEDGER]
					WHERE [RECORD_DELETED] = 0

					UNION ALL

					SELECT  
						[ACCOUNT_REF]
						,0											as [ANALYSIS_ID]
						,10											as [Period]
						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
						,[BUDGET_MTH10]								as Budget
						,0											as [CATEGORY]
						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetToUse
						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method
						,PI_ID

					FROM [dbo].[NOMINAL_LEDGER]
					WHERE [RECORD_DELETED] = 0


					UNION ALL

					SELECT  
						[ACCOUNT_REF]
						,0											as [ANALYSIS_ID]
						,11										as [Period]
						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
						,[BUDGET_MTH11]								as Budget
						,0											as [CATEGORY]
						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetToUse
						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method
						,PI_ID

					FROM [dbo].[NOMINAL_LEDGER]
					WHERE [RECORD_DELETED] = 0


					UNION ALL

					SELECT  
						[ACCOUNT_REF]
						,0											as [ANALYSIS_ID]
						,12										as [Period]
						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
						,[BUDGET_MTH12]								as Budget
						,0											as [CATEGORY]
						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetToUse
						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method
						,PI_ID

					FROM [dbo].[NOMINAL_LEDGER]
					WHERE [RECORD_DELETED] = 0
		)sub

		INNER JOIN [dbo].[CATEGORY] cat
			ON sub.ACCOUNT_REF BETWEEN cat.LOW AND cat.HIGH
				AND sub.PI_ID = cat.PI_ID
		--INNER JOIN [dbo].[CHART_LIST] ch
		--	ON cat.CHART = ch.CHART
		--	AND cat.PI_ID = ch.PI_ID
		--	ON cp.Financial_Period = sub.PERIOD
		--	AND cp.Financial_Year = sub.YEAR

		WHERE Budget_Method IN (0,1)
			AND Budget_Check = 1
     
--UNION ALL

-------- Get the sub totals

--		SELECT  [ACCOUNT_REF]
--			  ,[ANALYSIS_ID]
--			  ,[PERIOD]
--			  ,[YEAR]
--			  ,CASE WHEN [BUDGET]
--			  ,sub.[CATEGORY]
--			  ,[Budget_Check]
--			  ,[Budget_Method]
--				,cat.CATEGORY as Budget_Category
--				,ch.NAME

--				FROM (	SELECT [ACCOUNT_REF]
--						  ,[ANALYSIS_ID]
--						  ,[PERIOD]
--						  ,[YEAR]
--						  ,[BUDGET]
--						  ,[CATEGORY]
--						  ,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 1 THEN 1 ELSE 0 END as Budget_Check
--						  ,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method

--					  FROM [dbo].[FINANCIAL_BUDGET]

--					  WHERE [RECORD_DELETED] = 0

--					UNION ALL

--					SELECT  
--						[ACCOUNT_REF]
--						,0											as [ANALYSIS_ID]
--						,1											as [Period]
--						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
--						,[BUDGET_MTH1]								as Budget
--						,0											as [CATEGORY]
--						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetCheck
--						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method

--					FROM [dbo].[NOMINAL_LEDGER]
--					WHERE [RECORD_DELETED] = 0

--					UNION ALL

--					SELECT  
--						[ACCOUNT_REF]
--						,0											as [ANALYSIS_ID]
--						,2											as [Period]
--						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
--						,[BUDGET_MTH2]								as Budget
--						,0											as [CATEGORY]
--						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetCheck
--						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method

--					FROM [dbo].[NOMINAL_LEDGER]
--					WHERE [RECORD_DELETED] = 0

--					UNION ALL

--					SELECT  
--						[ACCOUNT_REF]
--						,0											as [ANALYSIS_ID]
--						,3										as [Period]
--						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
--						,[BUDGET_MTH3]								as Budget
--						,0											as [CATEGORY]
--						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetToUse
--						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method

--					FROM [dbo].[NOMINAL_LEDGER]
--					WHERE [RECORD_DELETED] = 0

--					UNION ALL

--					SELECT  
--						[ACCOUNT_REF]
--						,0											as [ANALYSIS_ID]
--						,4											as [Period]
--						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
--						,[BUDGET_MTH4]								as Budget
--						,0											as [CATEGORY]
--						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetToUse
--						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method

--					FROM [dbo].[NOMINAL_LEDGER]
--					WHERE [RECORD_DELETED] = 0
     
--					 UNION ALL

--					SELECT  
--						[ACCOUNT_REF]
--						,0											as [ANALYSIS_ID]
--						,5											as [Period]
--						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
--						,[BUDGET_MTH5]								as Budget
--						,0											as [CATEGORY]
--						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetToUse
--						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method

--					FROM [dbo].[NOMINAL_LEDGER]
--					WHERE [RECORD_DELETED] = 0
     
--					UNION ALL

--					SELECT  
--						[ACCOUNT_REF]
--						,0											as [ANALYSIS_ID]
--						,6											as [Period]
--						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
--						,[BUDGET_MTH6]								as Budget
--						,0											as [CATEGORY]
--						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetToUse
--						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method

--					FROM [dbo].[NOMINAL_LEDGER]
--					WHERE [RECORD_DELETED] = 0

--					UNION ALL

--					SELECT  
--						[ACCOUNT_REF]
--						,0											as [ANALYSIS_ID]
--						,7											as [Period]
--						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
--						,[BUDGET_MTH7]								as Budget
--						,0											as [CATEGORY]
--						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetToUse
--						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method

--					FROM [dbo].[NOMINAL_LEDGER]
--					WHERE [RECORD_DELETED] = 0
    
--					UNION ALL

--					SELECT  
--						[ACCOUNT_REF]
--						,0											as [ANALYSIS_ID]
--						,8											as [Period]
--						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
--						,[BUDGET_MTH8]								as Budget
--						,0											as [CATEGORY]
--						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetToUse
--						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method

--					FROM [dbo].[NOMINAL_LEDGER]
--					WHERE [RECORD_DELETED] = 0

--					UNION ALL

--					SELECT  
--						[ACCOUNT_REF]
--						,0											as [ANALYSIS_ID]
--						,9											as [Period]
--						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
--						,[BUDGET_MTH9]								as Budget
--						,0											as [CATEGORY]
--						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetToUse
--						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method

--					FROM [dbo].[NOMINAL_LEDGER]
--					WHERE [RECORD_DELETED] = 0

--					UNION ALL

--					SELECT  
--						[ACCOUNT_REF]
--						,0											as [ANALYSIS_ID]
--						,10											as [Period]
--						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
--						,[BUDGET_MTH10]								as Budget
--						,0											as [CATEGORY]
--						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetToUse
--						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method

--					FROM [dbo].[NOMINAL_LEDGER]
--					WHERE [RECORD_DELETED] = 0


--					UNION ALL

--					SELECT  
--						[ACCOUNT_REF]
--						,0											as [ANALYSIS_ID]
--						,11										as [Period]
--						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
--						,[BUDGET_MTH11]								as Budget
--						,0											as [CATEGORY]
--						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetToUse
--						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method

--					FROM [dbo].[NOMINAL_LEDGER]
--					WHERE [RECORD_DELETED] = 0


--					UNION ALL

--					SELECT  
--						[ACCOUNT_REF]
--						,0											as [ANALYSIS_ID]
--						,12										as [Period]
--						,(SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as [YEAR]
--						,[BUDGET_MTH12]								as Budget
--						,0											as [CATEGORY]
--						,CASE WHEN (SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) = 0 THEN 1 ELSE 0 END as BudgetToUse
--						,(SELECT TOP 1 [BUDGETING_METHOD] FROM COMPANY) as Budget_Method

--					FROM [dbo].[NOMINAL_LEDGER]
--					WHERE [RECORD_DELETED] = 0
--		)sub

--		INNER JOIN [dbo].[CATEGORY] cat
--			ON sub.ACCOUNT_REF BETWEEN cat.LOW AND cat.HIGH
--		INNER JOIN [dbo].[CHART_LIST] ch
--			ON cat.CHART = ch.CHART
--		--	ON cp.Financial_Period = sub.PERIOD
--		--	AND cp.Financial_Year = sub.YEAR

--		WHERE Budget_Method IN (0,1)
--			AND Budget_Check = 1
  

  
--		GO


GO
/****** Object:  View [dbo].[vw_pandash_PreCannedDates]    Script Date: 09/12/2019 14:43:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[vw_pandash_PreCannedDates] AS
SELECT  
[ROWKEY]      = 1, 
[DATEKEY] = (YEAR(DATEADD(d, 0, GETDATE())) * 10000) + (MONTH(DATEADD(d, 0, GETDATE())) * 100) + DAY(DATEADD(d, 0, GETDATE())),
[DATE_PERIOD_KEY] = (YEAR(GETDATE()) * 1000) + MONTH(GETDATE()),
[DATE] = Convert(DateTime, DATEDIFF(DAY, 0, GETDATE())),
[DATE_DATE] = CONVERT(DATE, GETDATE()),
[SHORT_DATE_NAME] = CONVERT(VARCHAR(50), 'Today'),
[LONG_DATE_NAME] = CONVERT(VARCHAR(50), 'Today'),
[DATE_FROM] = Convert(DateTime, DATEDIFF(DAY, 0, GETDATE())),
[DATE_TO] = Convert(DateTime, DATEADD(ms,-2,DATEDIFF(DAY, -1, GETDATE()))),
[YEAR] = YEAR(GETDATE()),
[QUARTER] = floor((month(GETDATE())+2)/3),
[YEAR_AND_QUARTER] = CAST(Year(GETDATE())AS VARCHAR(4))+' Q'+CAST(floor((month(GETDATE())+2)/3) AS VARCHAR(1)), -- Year and quarter
[MONTH_NUMBER] = Month(GETDATE()), -- The month number of the current date
[MONTH_NAME] = DateName(m,GETDATE()), -- the month name of the current month (January, February, etc.)
[MONTH_YEAR] = DateName(m,GETDATE()) + ' ' + CAST(Year(GETDATE()) AS VARCHAR(4)), -- The month and year of the current date
[MONTH_YEAR_ABBREVIATED] = SUBSTRING(DateName(m,GETDATE()),1,3) +' '+SUBSTRING(CAST(Year(GETDATE()) AS VARCHAR(4)),3,2), -- Month and year ABBREVIATED (Jan 09, Jan 10, etc.)
[MONTH_ABBREVIATED] = SUBSTRING(DateName(m,GETDATE()),1,3), -- Month name ABBREVIATED (Jan, Feb, etc.)
[MONTH_NAME_SORTED] = CASE WHEN month(GETDATE()) < 10 THEN '0' ELSE '' END + CAST(month(GETDATE()) AS varchar(2))+' '+DateName(m,GETDATE()), --[MONTH_NAME_SORTED]
[WEEK_NUMBER] = DATEPART(wk, GETDATE()), -- the week number of the current date
[WEEK_STARTING] = CASE WHEN DateName(weekday,GETDATE()) = 'Sunday' THEN datepart(day,(dateadd(week, datediff(week, 0, GETDATE()), -7))) ELSE datepart(day,(dateadd(week, datediff(week, 0, GETDATE()), 0))) END, -- The day of the week that the week starts on
[DAY_NUMBER] = Day(GETDATE()), -- The day number of the current date
[DAY_NAME] = DateName(weekday,GETDATE()), -- The day name of the current date (Monday, Tuesday, etc.)
[DAY_NAME_SORTED] = CASE
       WHEN DateName(weekday,GETDATE()) = 'Monday' THEN '1 Monday'
       WHEN DateName(weekday,GETDATE()) = 'Tuesday' THEN '2 Tuesday'
       WHEN DateName(weekday,GETDATE()) = 'Wednesday' THEN '3 Wednesday'
       WHEN DateName(weekday,GETDATE()) = 'Thursday' THEN '4 Thursday'
       WHEN DateName(weekday,GETDATE()) = 'Friday' THEN '5 Friday'
       WHEN DateName(weekday,GETDATE()) = 'Saturday' THEN '6 Saturday'
       WHEN DateName(weekday,GETDATE()) = 'Sunday' THEN '7 Sunday'
       ELSE ''
END,   
[DAY_NUMBER_ORDINAL] = CASE
       WHEN DAY(GETDATE()) IN(1,21,31) then CAST(DAY(GETDATE()) AS VARCHAR(4))+'st'
       WHEN DAY(GETDATE()) IN(2,22) then CAST(DAY(GETDATE()) AS VARCHAR(4))+'nd'
       WHEN DAY(GETDATE()) IN (3,23) then CAST(DAY(GETDATE()) AS VARCHAR(4))+'rd'
       WHEN DAY(GETDATE()) IN(4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,24,25,26,27,28,29,30) then CAST(DAY(GETDATE()) AS VARCHAR(4))+'th'
       ELSE CAST(DAY(GETDATE()) AS VARCHAR(4))
END,
[DAY_AND_NAME] = CAST(Day(GETDATE()) AS VARCHAR(2)) +' '+ CAST(DateName(weekday,GETDATE()) AS VARCHAR(10)),
[DAY_AND_NAME_ABBREVATED] = CAST(Day(GETDATE()) AS VARCHAR(2)) +' '+ CAST(SUBSTRING(DateName(weekday,GETDATE()),0,3) AS VARCHAR(4)),
[DD_MMM_YYYY] = CONVERT(VARCHAR(11), GETDATE(),113),
[DD_MMM_YY] = CONVERT(VARCHAR(11),GETDATE(),6),
[MM_DD_YYYY] = CONVERT(VARCHAR(11), GETDATE(),110)
UNION
-- YESTERDAY
SELECT  
[ROWKEY]      = 2, 
[DATEKEY] = (YEAR(DATEADD(d, -1, GETDATE())) * 10000) + (MONTH(DATEADD(d, -1, GETDATE())) * 100) + DAY(DATEADD(d, -1, GETDATE())),
[DATE_PERIOD_KEY] = (YEAR(DATEADD(d,-1,GETDATE())) * 1000) + MONTH(DATEADD(d,-1,GETDATE())),
[DATE] = DATEADD(d,-1, Convert(DateTime, DATEDIFF(DAY, 0, GETDATE()))),
[DATE_DATE] = CONVERT(DATE, DATEADD(d,-1,GETDATE())),
[SHORT_DATE_NAME] = CONVERT(VARCHAR(50), 'Yesterday'),
[LONG_DATE_NAME] = CONVERT(VARCHAR(50), 'Yesterday'),
[DATE_FROM] = DATEADD(d,-1, Convert(DateTime, DATEDIFF(DAY, 0, GETDATE()))),
[DATE_TO] = Convert(DateTime, DATEADD(ms,-2,DATEDIFF(DAY, 0, GETDATE()))),
[YEAR] = YEAR(DATEADD(d,-1, Convert(DateTime, DATEDIFF(DAY, 0, GETDATE())))),
[QUARTER] = floor((month(DATEADD(d,-1,GETDATE()))+2)/3),
[YEAR_AND_QUARTER] = CAST(Year(DATEADD(d,-1,GETDATE()))AS VARCHAR(4))+' Q'+CAST(floor((month(DATEADD(d,-1,GETDATE()))+2)/3) AS VARCHAR(1)), -- Year and quarter
[MONTH_NUMBER] = Month(DATEADD(d,-1,GETDATE())), -- The month number of the current date
[MONTH_NAME] = DateName(m,DATEADD(d,-1,GETDATE())), -- the month name of the current month (January, February, etc.)
[MONTH_YEAR] = DateName(m,DATEADD(d,-1,GETDATE())) + ' ' + CAST(Year(DATEADD(d,-1,GETDATE())) AS VARCHAR(4)), -- The month and year of the current date
[MONTH_YEAR_ABBREVIATED] = SUBSTRING(DateName(m,DATEADD(d,-1,GETDATE())),1,3) +' '+SUBSTRING(CAST(Year(DATEADD(d,-1,GETDATE())) AS VARCHAR(4)),3,2), -- Month and year ABBREVIATED (Jan 09, Jan 10, etc.)
[MONTH_ABBRIVEATED] = SUBSTRING(DateName(m,DATEADD(d,-1,GETDATE())),1,3), -- Month name ABBREVIATED (Jan, Feb, etc.)
[MONTH_NAME_SORTED] = CASE WHEN month(DATEADD(d,-1,GETDATE())) < 10 THEN '0' ELSE '' END + CAST(month(DATEADD(d,-1,GETDATE())) AS varchar(2))+' '+DateName(m,DATEADD(d,-1,GETDATE())), --[MONTH_NAME_SORTED]
[WEEK_NUMBER] = DATEPART(wk, DATEADD(d,-1,GETDATE())), -- the week number of the current date
[WEEK_STARTING] = CASE WHEN DateName(weekday,DATEADD(d,-1,GETDATE())) = 'Sunday' THEN datepart(day,(dateadd(week, datediff(week, 0, DATEADD(d,-1,GETDATE())), -7))) ELSE datepart(day,(dateadd(week, datediff(week, 0, DATEADD(d,-1,GETDATE())), 0))) END, -- The day of the week that the week starts on
[DAY_NUMBER] = Day(DATEADD(d,-1,GETDATE())), -- The day number of the current date
[DAY_NAME] = DateName(weekday,DATEADD(d,-1,GETDATE())), -- The day name of the current date (Monday, Tuesday, etc.)
[DAY_NAME_SORTED] = CASE
       WHEN DateName(weekday,DATEADD(d,-1,GETDATE())) = 'Monday' THEN '1 Monday'
       WHEN DateName(weekday,DATEADD(d,-1,GETDATE()))= 'Tuesday' THEN '2 Tuesday'
       WHEN DateName(weekday,DATEADD(d,-1,GETDATE())) = 'Wednesday' THEN '3 Wednesday'
       WHEN DateName(weekday,DATEADD(d,-1,GETDATE())) = 'Thursday' THEN '4 Thursday'
       WHEN DateName(weekday,DATEADD(d,-1,GETDATE())) = 'Friday' THEN '5 Friday'
       WHEN DateName(weekday,DATEADD(d,-1,GETDATE())) = 'Saturday' THEN '6 Saturday'
       WHEN DateName(weekday,DATEADD(d,-1,GETDATE())) = 'Sunday' THEN '7 Sunday'
       ELSE ''
END,   
[DAY_NUMBER_ORDINAL] = CASE
       WHEN DAY(DATEADD(d,-1,GETDATE())) IN(1,21,31) then CAST(DAY(DATEADD(d,-1,GETDATE())) AS VARCHAR(4))+'st'
       WHEN DAY(DATEADD(d,-1,GETDATE())) IN(2,22) then CAST(DAY(DATEADD(d,-1,GETDATE())) AS VARCHAR(4))+'nd'
       WHEN DAY(DATEADD(d,-1,GETDATE())) IN (3,23) then CAST(DAY(DATEADD(d,-1,GETDATE())) AS VARCHAR(4))+'rd'
       WHEN DAY(GETDATE()) IN(4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,24,25,26,27,28,29,30) then CAST(DAY(DATEADD(d,-1,GETDATE())) AS VARCHAR(4))+'th'
       ELSE CAST(DAY(DATEADD(d,-1,GETDATE())) AS VARCHAR(4))
END,
[DAY_AND_NAME] = CAST(Day(DATEADD(d,-1,GETDATE())) AS VARCHAR(2)) +' '+ CAST(DateName(weekday,DATEADD(d,-1,GETDATE())) AS VARCHAR(10)),
[DAY_AND_NAME_ABBREVIATED] = CAST(Day(DATEADD(d,-1,GETDATE())) AS VARCHAR(2)) +' '+ CAST(SUBSTRING(DateName(weekday,DATEADD(d,-1,GETDATE())),0,3) AS VARCHAR(4)),
[DD_MMM_YYYY] = CONVERT(VARCHAR(11), DATEADD(d,-1,GETDATE()),113),
[DD_MMM_YY] = CONVERT(VARCHAR(11),DATEADD(d,-1,GETDATE()),6),
[MM_DD_YYYY] = CONVERT(VARCHAR(11), DATEADD(d,-1,GETDATE()),110)
UNION
-- Last 7 days
SELECT  
[ROWKEY]      = 3, 
[DATEKEY] = NULL, 
[DATE_PERIOD_KEY] = NULL,
[DATE] = NULL, 
[DATE_DATE] = NULL, 
[SHORT_DATE_NAME] = CONVERT(VARCHAR(50), 'Last 7 Days'),
[LONG_DATE_NAME] = CONVERT(VARCHAR(50), 'Last 7 Days ('
                             + RIGHT('0' + DATENAME(DD, DATEADD(d, -6, GETDATE())), 2) + ' ' 
                             + LEFT(DATENAME(MM, DATEADD(d, -6, GETDATE())), 3) + ' '
                             + DATENAME(YY, DATEADD(d, -6, GETDATE()))
                             + ' - '
                             + RIGHT('0' + DATENAME(DD, GETDATE()), 2) + ' ' 
                             + LEFT(DATENAME(MM, GETDATE()), 3) + ' '
                             + DATENAME(YY, GETDATE())
                             + ')'
                             ),
[DATE_FROM] = DATEADD(d,-6, Convert(DateTime, DATEDIFF(DAY, 0, GETDATE()))),
[DATE_TO] =  Convert(DateTime, DATEADD(ms,-2,DATEDIFF(DAY, -1, GETDATE()))),
[YEAR] = NULL, 
[QUARTER] = NULL, 
[YEAR_AND_QUARTER] = NULL,
[MONTH_NUMBER] = NULL, 
[MONTH_NAME] = NULL, 
[MONTH_YEAR] = NULL,
[MONTH_YEAR_ABBREVIATED] = NULL,
[MONTH_ABBREVIATED] = NULL,
[MONTH_NAME_SORTED] = NULL,
[WEEK_NUMBER] = NULL, 
[WEEK_STARTING] = NULL,
[DAY_NUMBER] = NULL,
[DAY_NAME] = NULL, 
[DAY_NAME_SORTED] = NULL, 
[DAY_NUMBER_ORDINAL] = NULL,
[DAY_AND_NAME] = NULL,
[DAY_AND_NAME_ABBREVIATED] = NULL,
[DD_MMM_YYYY] = NULL, 
[DD_MMM_YY] = NULL, 
[MM_DD_YYYY] = NULL 
UNION
-- Last 14 days
SELECT  
[ROWKEY]      = 4, 
[DATEKEY] = NULL,
[DATE_PERIOD_KEY] = NULL,
[DATE] = NULL, 
[DATE_DATE] = NULL, 
[SHORT_DATE_NAME] = CONVERT(VARCHAR(50), 'Last 14 Days'),
[LONG_DATE_NAME] = CONVERT(VARCHAR(50), 'Last 14 Days ('
                             + RIGHT('0' + DATENAME(DD, DATEADD(d, -13, GETDATE())), 2) + ' ' 
                             + LEFT(DATENAME(MM, DATEADD(d, -13, GETDATE())), 3) + ' '
                             + DATENAME(YY, DATEADD(d, -13, GETDATE()))
                             + ' - '
                             + RIGHT('0' + DATENAME(DD, GETDATE()), 2) + ' ' 
                             + LEFT(DATENAME(MM, GETDATE()), 3) + ' '
                             + DATENAME(YY, GETDATE())
                             + ')'
                             ),
[DATE_FROM] = DATEADD(d,-13, Convert(DateTime, DATEDIFF(DAY, 0, GETDATE()))),
[DATE_TO] =  Convert(DateTime, DATEADD(ms,-2,DATEDIFF(DAY, -1, GETDATE()))),
[YEAR] = NULL, 
[QUARTER] = NULL, 
[YEAR_AND_QUARTER] = NULL,
[MONTH_NUMBER] = NULL, 
[MONTH_NAME] = NULL, 
[MONTH_YEAR] = NULL,
[MONTH_YEAR_ABBREVIATED] = NULL,
[MONTH_ABBREVIATED] = NULL,
[MONTH_NAME_SORTED] = NULL,
[WEEK_NUMBER] = NULL, 
[WEEK_STARTING] = NULL,
[DAY_NUMBER] = NULL,
[DAY_NAME] = NULL, 
[DAY_NAME_SORTED] = NULL, 
[DAY_NUMBER_ORDINAL] = NULL,
[DAY_AND_NAME] = NULL,
[DAY_AND_NAME_ABBREVIATED] = NULL,
[DD_MMM_YYYY] = NULL, 
[DD_MMM_YY] = NULL, 
[MM_DD_YYYY] = NULL 
UNION
-- Last 30 days
SELECT  
[ROWKEY]      = 5, 
[DATEKEY] = NULL,
[DATE_PERIOD_KEY] = NULL,
[DATE] = NULL, 
[DATE_DATE] = NULL, 
[SHORT_DATE_NAME] = CONVERT(VARCHAR(50), 'Last 30 Days'),
[LONG_DATE_NAME] =  CONVERT(VARCHAR(50), 'Last 30 Days ('
                             + RIGHT('0' + DATENAME(DD, DATEADD(d, -29, GETDATE())), 2) + ' ' 
                             + LEFT(DATENAME(MM, DATEADD(d, -29, GETDATE())), 3) + ' '
                             + DATENAME(YY, DATEADD(d, -29, GETDATE()))
                             + ' - '
                             + RIGHT('0' + DATENAME(DD, GETDATE()), 2) + ' ' 
                             + LEFT(DATENAME(MM, GETDATE()), 3) + ' '
                             + DATENAME(YY, GETDATE())
                             + ')'
                             ),
[DATE_FROM] = DATEADD(d,-29, Convert(DateTime, DATEDIFF(DAY, 0, GETDATE()))),
[DATE_TO] =  Convert(DateTime, DATEADD(ms,-2,DATEDIFF(DAY, -1, GETDATE()))),
[YEAR] = NULL, 
[QUARTER] = NULL, 
[YEAR_AND_QUARTER] = NULL,
[MONTH_NUMBER] = NULL, 
[MONTH_NAME] = NULL, 
[MONTH_YEAR] = NULL,
[MONTH_YEAR_ABBREVIATED] = NULL,
[MONTH_ABBREVIATED] = NULL,
[MONTH_NAME_SORTED] = NULL,
[WEEK_NUMBER] = NULL, 
[WEEK_STARTING] = NULL,
[DAY_NUMBER] = NULL,
[DAY_NAME] = NULL, 
[DAY_NAME_SORTED] = NULL, 
[DAY_NUMBER_ORDINAL] = NULL,
[DAY_AND_NAME] = NULL,
[DAY_AND_NAME_ABBREVIATED] = NULL,
[DD_MMM_YYYY] = NULL, 
[DD_MMM_YY] = NULL, 
[MM_DD_YYYY] = NULL 
UNION
-- Last 60 days
SELECT  
[ROWKEY]      = 6, 
[DATEKEY] = NULL,
[DATE_PERIOD_KEY] = NULL,
[DATE] = NULL, 
[DATE_DATE] = NULL, 
[SHORT_DATE_NAME] = CONVERT(VARCHAR(50), 'Last 60 Days'),
[LONG_DATE_NAME] = CONVERT(VARCHAR(50), 'Last 60 Days ('
                             + RIGHT('0' + DATENAME(DD, DATEADD(d, -59, GETDATE())), 2) + ' ' 
                             + LEFT(DATENAME(MM, DATEADD(d, -59, GETDATE())), 3) + ' '
                             + DATENAME(YY, DATEADD(d, -59, GETDATE()))
                             + ' - '
                             + RIGHT('0' + DATENAME(DD, GETDATE()), 2) + ' ' 
                             + LEFT(DATENAME(MM, GETDATE()), 3) + ' '
                             + DATENAME(YY, GETDATE())
                             + ')'
                             ),
[DATE_FROM] = DATEADD(d,-59, Convert(DateTime, DATEDIFF(DAY, 0, GETDATE()))),
[DATE_TO] =  Convert(DateTime, DATEADD(ms,-2,DATEDIFF(DAY, -1, GETDATE()))),
[YEAR] = NULL, 
[QUARTER] = NULL, 
[YEAR_AND_QUARTER] = NULL,
[MONTH_NUMBER] = NULL, 
[MONTH_NAME] = NULL, 
[MONTH_YEAR] = NULL,
[MONTH_YEAR_ABBREVIATED] = NULL,
[MONTH_ABBREVIATED] = NULL,
[MONTH_NAME_SORTED] = NULL,
[WEEK_NUMBER] = NULL, 
[WEEK_STARTING] = NULL,
[DAY_NUMBER] = NULL,
[DAY_NAME] = NULL, 
[DAY_NAME_SORTED] = NULL, 
[DAY_NUMBER_ORDINAL] = NULL,
[DAY_AND_NAME] = NULL,
[DAY_AND_NAME_ABBREVIATED] = NULL,
[DD_MMM_YYYY] = NULL, 
[DD_MMM_YY] = NULL, 
[MM_DD_YYYY] = NULL 
UNION
-- Last 90 days
SELECT  
[ROWKEY]      = 7, 
[DATEKEY] = NULL,
[DATE_PERIOD_KEY] = NULL,
[DATE] = NULL, 
[DATE_DATE] = NULL, 
[SHORT_DATE_NAME] = CONVERT(VARCHAR(50), 'Last 90 Days'),
[LONG_DATE_NAME] = CONVERT(VARCHAR(50), 'Last 90 Days ('
                             + RIGHT('0' + DATENAME(DD, DATEADD(d, -89, GETDATE())), 2) + ' ' 
                             + LEFT(DATENAME(MM, DATEADD(d, -89, GETDATE())), 3) + ' '
                             + DATENAME(YY, DATEADD(d, -89, GETDATE()))
                             + ' - '
                             + RIGHT('0' + DATENAME(DD, GETDATE()), 2) + ' ' 
                             + LEFT(DATENAME(MM, GETDATE()), 3) + ' '
                             + DATENAME(YY, GETDATE())
                             + ')'
                             ),
[DATE_FROM] = DATEADD(d,-89, Convert(DateTime, DATEDIFF(DAY, 0, GETDATE()))),
[DATE_TO] =  Convert(DateTime, DATEADD(ms,-2,DATEDIFF(DAY, -1, GETDATE()))),
[YEAR] = NULL, 
[QUARTER] = NULL, 
[YEAR_AND_QUARTER] = NULL,
[MONTH_NUMBER] = NULL, 
[MONTH_NAME] = NULL, 
[MONTH_YEAR] = NULL,
[MONTH_YEAR_ABBREVIATED] = NULL,
[MONTH_ABBREVIATED] = NULL,
[MONTH_NAME_SORTED] = NULL,
[WEEK_NUMBER] = NULL, 
[WEEK_STARTING] = NULL,
[DAY_NUMBER] = NULL,
[DAY_NAME] = NULL, 
[DAY_NAME_SORTED] = NULL, 
[DAY_NUMBER_ORDINAL] = NULL,
[DAY_AND_NAME] = NULL,
[DAY_AND_NAME_ABBREVIATED] = NULL,
[DD_MMM_YYYY] = NULL, 
[DD_MMM_YY] = NULL, 
[MM_DD_YYYY] = NULL 
UNION
-- Last Calendar Month
SELECT  
[ROWKEY]      = 8, 
[DATEKEY] = NULL,
[DATE_PERIOD_KEY] = NULL,
[DATE] = NULL, 
[DATE_DATE] = NULL, 
[SHORT_DATE_NAME] = CONVERT(VARCHAR(50), 'Last Calendar Month'),
[LONG_DATE_NAME] = CONVERT(VARCHAR(50), 'Last Calendar Month ('
                             + RIGHT('0' + DATENAME(DD,DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0) ), 2) + ' ' 
                             + LEFT(DATENAME(MM, DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0) ), 3) + ' '
                             + DATENAME(YY, DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0) )
                             + ' - '
                             + RIGHT('0' + DATENAME(DD, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-1, -1)), 2) + ' ' 
                             + LEFT(DATENAME(MM, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-1, -1)), 3) + ' '
                             + DATENAME(YY, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-1, -1))
                             + ')'
                             ),
[DATE_FROM] = DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0),
[DATE_TO] =   DATEADD(ms,-2,DATEDIFF(DAY, -1, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-1, -1))),
[YEAR] = NULL, 
[QUARTER] = NULL, 
[YEAR_AND_QUARTER] = NULL,
[MONTH_NUMBER] = MONTH(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0)), 
[MONTH_NAME] = DATENAME(mm,DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0)), 
[MONTH_YEAR] = DATENAME(mm,DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0))+' '+ CAST(YEAR(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0)) AS VARCHAR(4)),
[MONTH_YEAR_ABBREVIATED] = SUBSTRING(DateName(mm,DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0)),1,3) +' '+SUBSTRING(CAST(Year(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0)) AS VARCHAR(4)),3,2), -- Month and year ABBREVIATED (Jan 09, Jan 10, etc.),
[MONTH_ABBREVIATED] = SUBSTRING(DateName(m,DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0)),1,3),
[MONTH_NAME_SORTED] = CASE WHEN month(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0)) < 10 THEN '0' ELSE '' END + CAST(month(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0)) AS varchar(2))+' '+DateName(m,DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0)), --[MONTH_NAME_SORTED],
[WEEK_NUMBER] = NULL, 
[WEEK_STARTING] = NULL,
[DAY_NUMBER] = NULL,
[DAY_NAME] = NULL, 
[DAY_NAME_SORTED] = NULL, 
[DAY_NUMBER_ORDINAL] = NULL,
[DAY_AND_NAME] = NULL,
[DAY_AND_NAME_ABBREVIATED] = NULL,
[DD_MMM_YYYY] = NULL, 
[DD_MMM_YY] = NULL, 
[MM_DD_YYYY] = NULL 
UNION
-- Last 3 Calendar Month
SELECT  
[ROWKEY]      = 9, 
[DATEKEY] = NULL,
[DATE_PERIOD_KEY] = NULL,
[DATE] = NULL, 
[DATE_DATE] = NULL, 
[SHORT_DATE_NAME] = CONVERT(VARCHAR(50), 'Last 3 Calendar Months'),
[LONG_DATE_NAME] = CONVERT(VARCHAR(70), 'Last 3 Calendar Months ('
                             + RIGHT('0' + DATENAME(DD, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-4, 0)), 2) + ' ' 
                             + LEFT(DATENAME(MM,  DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-4, 0)), 3) + ' '
                             + DATENAME(YY,DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-4, 0))
                             + ' - '
                             + RIGHT('0' + DATENAME(DD, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-1, -1)), 2) + ' ' 
                             + LEFT(DATENAME(MM, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-1, -1)), 3) + ' '
                             + DATENAME(YY, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-1, -1))
                             + ')'
                             ),
[DATE_FROM] = DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-4, 0),
[DATE_TO] =  DATEADD(ms,-2,DATEDIFF(DAY, -1, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-1, -1))),
[YEAR] = NULL, 
[QUARTER] = NULL, 
[YEAR_AND_QUARTER] = NULL,
[MONTH_NUMBER] = NULL, 
[MONTH_NAME] = NULL, 
[MONTH_YEAR] = NULL,
[MONTH_YEAR_ABBREVIATED] = NULL,
[MONTH_ABBREVIATED] = NULL,
[MONTH_NAME_SORTED] = NULL,
[WEEK_NUMBER] = NULL, 
[WEEK_STARTING] = NULL,
[DAY_NUMBER] = NULL,
[DAY_NAME] = NULL, 
[DAY_NAME_SORTED] = NULL, 
[DAY_NUMBER_ORDINAL] = NULL,
[DAY_AND_NAME] = NULL,
[DAY_AND_NAME_ABBREVIATED] = NULL,
[DD_MMM_YYYY] = NULL, 
[DD_MMM_YY] = NULL, 
[MM_DD_YYYY] = NULL 
UNION
-- Last 6 calendar months
SELECT  
[ROWKEY]      = 10, 
[DATEKEY] = NULL,
[DATE_PERIOD_KEY] = NULL,
[DATE] = NULL, 
[DATE_DATE] = NULL, 
[SHORT_DATE_NAME] = CONVERT(VARCHAR(50), 'Last 6 Calendar Months'),
[LONG_DATE_NAME] = CONVERT(VARCHAR(50), 'Last 6 Calendar Months ('
                             + RIGHT('0' + DATENAME(DD,DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE()) -7, 0)), 2) + ' ' 
                             + LEFT(DATENAME(MM,  DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE()) -7, 0)), 3) + ' '
                             + DATENAME(YY,DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE()) -7, 0))
                             + ' - '
                             + RIGHT('0' + DATENAME(DD, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-1, -1)), 2) + ' ' 
                             + LEFT(DATENAME(MM, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-1, -1)), 3) + ' '
                             + DATENAME(YY, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-1, -1))
                             + ')'
                             ),
[DATE_FROM] = DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE()) -7, 0),
[DATE_TO] =  DATEADD(ms,-2,DATEDIFF(DAY, -1, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-1, -1))),
[YEAR] = NULL, 
[QUARTER] = NULL, 
[YEAR_AND_QUARTER] = NULL,
[MONTH_NUMBER] = NULL, 
[MONTH_NAME] = NULL, 
[MONTH_YEAR] = NULL,
[MONTH_YEAR_ABBREVIATED] = NULL,
[MONTH_ABBREVIATED] = NULL,
[MONTH_NAME_SORTED] = NULL,
[WEEK_NUMBER] = NULL, 
[WEEK_STARTING] = NULL,
[DAY_NUMBER] = NULL,
[DAY_NAME] = NULL, 
[DAY_NAME_SORTED] = NULL, 
[DAY_NUMBER_ORDINAL] = NULL,
[DAY_AND_NAME] = NULL,
[DAY_AND_NAME_ABBREVIATED] = NULL,
[DD_MMM_YYYY] = NULL, 
[DD_MMM_YY] = NULL, 
[MM_DD_YYYY] = NULL 
UNION
-- Last calendar year
SELECT  
[ROWKEY]      = 11, 
[DATEKEY] = NULL,
[DATE_PERIOD_KEY] = NULL,
[DATE] = NULL, 
[DATE_DATE] = NULL, 
[SHORT_DATE_NAME] = CONVERT(VARCHAR(50), 'Last Year'),
[LONG_DATE_NAME] = CONVERT(VARCHAR(50), 'Last Year ('
                             + RIGHT('0' + DATENAME(DD, DATEADD(yy, DATEDIFF(yy, 0, GETDATE()), 0)), 2) + ' ' 
                             + LEFT(DATENAME(MM, DATEADD(yy, DATEDIFF(yy, 0, GETDATE()), 0)), 3) + ' '
                             + DATENAME(YY, DATEADD(YY, -1, GETDATE()))
                             + ' - '
                             + RIGHT('0' + DATENAME(DD, DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-8, -1)), 2) + ' ' 
                             + LEFT(DATENAME(MM, DATEADD(ms,-2,DATEADD(yy, DATEDIFF(yy, 0, GETDATE()) - 0, 0))), 3) + ' '
                             + DATENAME(YY, DATEADD(ms,-2,DATEADD(yy, DATEDIFF(yy, 0, GETDATE()) - 0, 0)))
                             + ')'
                             ),
[DATE_FROM] = DATEADD(yy, DATEDIFF(yy, 0, GETDATE()) - 1, 0),
[DATE_TO] = DATEADD(ms,-2,DATEADD(yy, DATEDIFF(yy, 0, GETDATE()) - 0, 0)),
[YEAR] = NULL, 
[QUARTER] = NULL, 
[YEAR_AND_QUARTER] = NULL,
[MONTH_NUMBER] = NULL, 
[MONTH_NAME] = NULL, 
[MONTH_YEAR] = NULL,
[MONTH_YEAR_ABBREVIATED] = NULL,
[MONTH_ABBREVIATED] = NULL,
[MONTH_NAME_SORTED] = NULL,
[WEEK_NUMBER] = NULL, 
[WEEK_STARTING] = NULL,
[DAY_NUMBER] = NULL,
[DAY_NAME] = NULL, 
[DAY_NAME_SORTED] = NULL, 
[DAY_NUMBER_ORDINAL] = NULL,
[DAY_AND_NAME] = NULL,
[DAY_AND_NAME_ABBREVIATED] = NULL,
[DD_MMM_YYYY] = NULL, 
[DD_MMM_YY] = NULL, 
[MM_DD_YYYY] = NULL 
UNION
-- This Month
SELECT  
[ROWKEY]      = 12, 
[DATEKEY] = NULL,
[DATE_PERIOD_KEY] = NULL,
[DATE] = NULL,
[DATE_DATE] = NULL,
[SHORT_DATE_NAME] = CONVERT(VARCHAR(50), 'This Month'),
[LONG_DATE_NAME] = CONVERT(VARCHAR(50), 'This Month (' 
                                 + RIGHT(Convert(Date, DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)),2)+' '+
                                 + LEFT(DATENAME(mm,DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)),3)+' '+
                                 + DATENAME(yy, DATEADD(month, DATEDIFF(month, 0, GETDATE()),0))+' '+
                                 +' - '+
                                 RIGHT(Convert(Date,DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,0,GETDATE())+1,0))),2)+' '+
                                 + LEFT(DATENAME(mm,DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)),3)+' '+
                                 + DATENAME(yy, DATEADD(month, DATEDIFF(month, 0, GETDATE()),0))+' '+
                                 +')'
                                 ),
[DATE_FROM] = Convert(DateTime, DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)),
[DATE_TO] = Convert(DateTime,DATEADD(ms,-2,DATEADD(mm, DATEDIFF(m,0,GETDATE())+1,0))),
[YEAR] = NULL,
[QUARTER] = NULL,
[YEAR_AND_QUARTER] = NULL, -- Year and quarter
[MONTH_NUMBER] = Month(GETDATE()), -- The month number of the current date
[MONTH_NAME] = DateName(m,GETDATE()), -- the month name of the current month (January, February, etc.)
[MONTH_YEAR] = DateName(m,GETDATE()) + ' ' + CAST(Year(GETDATE()) AS VARCHAR(4)), -- The month and year of the current date
[MONTH_YEAR_ABBREVIATED] = SUBSTRING(DateName(m,GETDATE()),1,3) +' '+SUBSTRING(CAST(Year(GETDATE()) AS VARCHAR(4)),3,2), -- Month and year ABBREVIATED (Jan 09, Jan 10, etc.)
[MONTH_ABBREVIATED] = SUBSTRING(DateName(m,GETDATE()),1,3), -- Month name ABBREVIATED (Jan, Feb, etc.)
[MONTH_NAME_SORTED] = CASE WHEN month(GETDATE()) < 10 THEN '0' ELSE '' END + CAST(month(GETDATE()) AS varchar(2))+' '+DateName(m,GETDATE()), --[MONTH_NAME_SORTED]
[WEEK_NUMBER] = NULL, 
[WEEK_STARTING] = NULL,
[DAY_NUMBER] = NULL,
[DAY_NAME] = NULL,
[DAY_NAME_SORTED] = NULL,  
[DAY_NUMBER_ORDINAL] = NULL,
[DAY_AND_NAME] = NULL,
[DAY_AND_NAME_ABBREVIATED] = NULL,
[DD_MMM_YYYY] = NULL,
[DD_MMM_YY] = NULL,
[MM_DD_YYYY] = NULL
UNION
-- This Year
SELECT  
[ROWKEY]      = 13, 
[DATEKEY] = NULL,
[DATE_PERIOD_KEY] = NULL,
[DATE] = NULL,
[DATE_DATE] = NULL,
[SHORT_DATE_NAME] = CONVERT(VARCHAR(50), 'This Year'),
[LONG_DATE_NAME] = CONVERT(VARCHAR(50), 'This Year (' 
                                 + RIGHT(Convert(Date, DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)),2)+' '+
                                 + LEFT(DATENAME(MM, DATEADD(yy, DATEDIFF(yy, 0, GETDATE()), 0)), 3)+' '+
                                 + DATENAME(yy, GETDATE())+' '+
                                 +' - '+
                                 RIGHT(Convert(Date,Convert(Date,DATEADD(ms,-2,DATEADD(year, DATEDIFF(year, -1, GETDATE()), 0)))),2)+' '+
                                 + LEFT(DATENAME(mm,Convert(Date,DATEADD(ms,-2,DATEADD(year, DATEDIFF(year, -1, GETDATE()), 0)))),3)+' '+
                                 + DATENAME(yy, DATEADD(month, DATEDIFF(month, 0, GETDATE()),0))+' '+
                                 +')'
                                 ),
[DATE_FROM] = Convert(DateTime,  DATEADD(yy, DATEDIFF(yy, 0, GETDATE()), 0)),
[DATE_TO] = Convert(DateTime,DATEADD(ms,-2,DATEADD(year, DATEDIFF(year, -1, GETDATE()), 0))),
[YEAR] = NULL,
[QUARTER] = NULL,
[YEAR_AND_QUARTER] = NULL, 
[MONTH_NUMBER] = NULL,
[MONTH_NAME] = NULL,
[MONTH_YEAR] = NULL,
[MONTH_YEAR_ABBREVIATED] = NULL,
[MONTH_ABBREVIATED] = NULL,
[MONTH_NAME_SORTED] = NULL,
[WEEK_NUMBER] = NULL, 
[WEEK_STARTING] = NULL,
[DAY_NUMBER] = NULL,
[DAY_NAME] = NULL,
[DAY_NAME_SORTED] = NULL,  
[DAY_NUMBER_ORDINAL] = NULL,
[DAY_AND_NAME] = NULL,
[DAY_AND_NAME_ABBREVIATED] = NULL,
[DD_MMM_YYYY] = NULL,
[DD_MMM_YY] = NULL,
[MM_DD_YYYY] = NULL
UNION
-- This Week

SELECT  
[ROWKEY]      = 14, 
[DATEKEY] = NULL,
[DATE_PERIOD_KEY] = NULL,
[DATE] = NULL,
[DATE_DATE] = NULL,
[SHORT_DATE_NAME] = CONVERT(VARCHAR(50), 'This Week'),
[LONG_DATE_NAME] = CONVERT(VARCHAR(50), 'This Week (' 
                                 + RIGHT(Convert(Date, DATEADD(wk, DATEDIFF(wk,0,GETDATE()), 0)),2)+' '+
                                 + LEFT(DATENAME(MM, DATEADD(wk, DATEDIFF(wk,0,GETDATE()), 0)), 3)+' '+
                                 + DATENAME(yy,DATEADD(wk, DATEDIFF(wk,0,GETDATE()), 0))+' '+
                                 +' - '+
                                 RIGHT(Convert(Date,DATEADD(ms,-2,DATEADD(week, DATEDIFF(day, 0, getdate())/7, 7))),2)+' '+
                                 + LEFT(DATENAME(mm,DATEADD(ms,-2,DATEADD(week, DATEDIFF(day, 0, getdate())/7, 5))),3)+' '+
                                 + DATENAME(yy, DATEADD(ms,-2,DATEADD(week, DATEDIFF(day, 0, getdate())/7, 5)))+' '+
                                 +')'
                                 ),
[DATE_FROM] = Convert(DateTime, DATEADD(wk, DATEDIFF(wk,0,GETDATE()), 0)),
[DATE_TO] = Convert(DateTime,DATEADD(ms,-2,DATEADD(week, DATEDIFF(day, 0, getdate())/7, 7))),
[YEAR] = NULL,
[QUARTER] = NULL,
[YEAR_AND_QUARTER] = NULL, 
[MONTH_NUMBER] = NULL,
[MONTH_NAME] = NULL,
[MONTH_YEAR] = NULL,
[MONTH_YEAR_ABBREVIATED] = NULL,
[MONTH_ABBREVIATED] = NULL,
[MONTH_NAME_SORTED] = NULL,
[WEEK_NUMBER] = NULL, 
[WEEK_STARTING] = NULL,
[DAY_NUMBER] = NULL,
[DAY_NAME] = NULL,
[DAY_NAME_SORTED] = NULL,  
[DAY_NUMBER_ORDINAL] = NULL,
[DAY_AND_NAME] = NULL,
[DAY_AND_NAME_ABBREVIATED] = NULL,
[DD_MMM_YYYY] = NULL,
[DD_MMM_YY] = NULL,
[MM_DD_YYYY] = NULL
UNION
-- This Month Last Year
SELECT  
[ROWKEY]      = 15, 
[DATEKEY] = NULL,
[DATE_PERIOD_KEY] = NULL,
[DATE] = NULL,
[DATE_DATE] = NULL,
[SHORT_DATE_NAME] = CONVERT(VARCHAR(50), 'This Month Last Year'),
[LONG_DATE_NAME] = CONVERT(VARCHAR(50), 'This Month Last Year (' 
                                 + RIGHT(Convert(Date, DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)),2)+' '+
                                 + LEFT(DATENAME(MM, Convert(DateTime, DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0))), 3)+' '+
                                 + DATENAME(yy, DATEADD(yy,-1,Convert(DateTime, DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0))))+' '+
                                 +' - '+
                                 RIGHT(Convert(Date,DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,0,GETDATE())+1,0))),2)+' '+
                                 + LEFT(DATENAME(mm,Convert(DateTime, DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0))),3)+' '+
                                 + DATENAME(yy, DATEADD(yy,-1,Convert(DateTime, DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0))))+' '+
                                 +')'
                                 ),
[DATE_FROM] = Convert(DateTime, DATEADD(yy,-1,DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0))),
[DATE_TO] = Convert(DateTime,DATEADD(ms,-2,DATEADD(yy,-1,DATEADD(mm, DATEDIFF(m,0,GETDATE())+1,0)))),
[YEAR] = NULL,
[QUARTER] = NULL,
[YEAR_AND_QUARTER] = NULL, -- Year and quarter
[MONTH_NUMBER] = MONTH(DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)),
[MONTH_NAME] = DATENAME(mm,DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)),
[MONTH_YEAR] = DATENAME(mm,DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0))+' '+CAST(YEAR(DATEADD(yy,-1,Convert(DateTime, DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)))) AS VARCHAR(4)),
[MONTH_YEAR_ABBREVIATED] = SUBSTRING(DateName(m,DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)),1,3) +' '+SUBSTRING(CAST(Year(DATEADD(yy,-1,Convert(DateTime, DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)))) AS VARCHAR(4)),3,2), -- Month and year ABBREVIATED (Jan 09, Jan 10, etc.),
[MONTH_ABBREVIATED] = SUBSTRING(DateName(m,DATEADD(yy,-1,DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0))),1,3), -- Month name ABBREVIATED (Jan, Feb, etc.)
[MONTH_NAME_SORTED] = CASE WHEN month(DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)) < 10 THEN '0' ELSE '' END + CAST(month(DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)) AS varchar(2))+' '+DateName(m,DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)), --[MONTH_NAME_SORTED]
[WEEK_NUMBER] = NULL, 
[WEEK_STARTING] = NULL,
[DAY_NUMBER] = NULL,
[DAY_NAME] = NULL,
[DAY_NAME_SORTED] = NULL,  
[DAY_NUMBER_ORDINAL] = NULL,
[DAY_AND_NAME] = NULL,
[DAY_AND_NAME_ABBREVIATED] = NULL,
[DD_MMM_YYYY] = NULL,
[DD_MMM_YY] = NULL,
[MM_DD_YYYY] = NULL
UNION
-- ‘This Week Last Year’ (tricky ; week numbers ? . Yeuch).
SELECT  
[ROWKEY]      = 16, 
[DATEKEY] = NULL,
[DATE_PERIOD_KEY] = NULL,
[DATE] = NULL,
[DATE_DATE] = NULL,
[SHORT_DATE_NAME] = CONVERT(VARCHAR(50), 'This Week Last Year'),
[LONG_DATE_NAME] = CONVERT(VARCHAR(50), 'This Week Last Year (' 
                                 + RIGHT(Convert(Date, DATEADD(wk,-52,DATEADD(wk, DATEDIFF(wk,0,GETDATE()), 0))),2)+' '+
                                 + LEFT(DATENAME(MM, DATEADD(wk,-52,DATEADD(wk, DATEDIFF(wk,0,GETDATE()), 0))), 3)+' '+
                                 + DATENAME(yy, DATEADD(wk,-52,DATEADD(wk, DATEDIFF(wk,0,GETDATE()), 0)))+' '+
                                 +' - '+
                                 RIGHT(Convert(Date, DATEADD(wk,-52,DATEADD(ms,-2,DATEADD(week, DATEDIFF(day, 0, getdate())/7, 7)))),2)+' '+
                                 + LEFT(DATENAME(mm, DATEADD(wk,-52,DATEADD(ms,-2,DATEADD(week, DATEDIFF(day, 0, getdate())/7, 7)))),3)+' '+
                                 + DATENAME(yy, DATEADD(wk,-52,DATEADD(ms,-2,DATEADD(week, DATEDIFF(day, 0, getdate())/7, 7))))+' '+
                                 +')'
                                 ),
[DATE_FROM] =  Convert(DateTime, DATEADD(wk,-52,DATEADD(wk, DATEDIFF(wk,0,GETDATE()), 0))),
[DATE_TO] = Convert(DateTime, DATEADD(wk,-52,DATEADD(ms,-2,DATEADD(week, DATEDIFF(day, 0, getdate())/7, 7)))),
[YEAR] = NULL,
[QUARTER] = NULL,
[YEAR_AND_QUARTER] = NULL, -- Year and quarter
[MONTH_NUMBER] = NULL,
[MONTH_NAME] = NULL,
[MONTH_YEAR] = NULL,
[MONTH_YEAR_ABBREVIATED] = NULL,
[MONTH_ABBREVIATED] = NULL,
[MONTH_NAME_SORTED] = NULL,
[WEEK_NUMBER] = DATEPART(wk,  DATEADD(wk,-52,DATEADD(wk, DATEDIFF(wk,0,GETDATE()), 0))), 
[WEEK_STARTING] = NULL,
[DAY_NUMBER] = NULL,
[DAY_NAME] = NULL,
[DAY_NAME_SORTED] = NULL,  
[DAY_NUMBER_ORDINAL] = NULL,
[DAY_AND_NAME] = NULL,
[DAY_AND_NAME_ABBREVIATED] = NULL,
[DD_MMM_YYYY] = NULL,
[DD_MMM_YY] = NULL,
[MM_DD_YYYY] = NULL


UNION
-- ‘Current Financial Year’ Had to take this year from Sage as customers vary
SELECT  
[ROWKEY]      = 17, 
[DATEKEY] = NULL,
[DATE_PERIOD_KEY] = NULL,
[DATE] = NULL,
[DATE_DATE] = NULL,
[SHORT_DATE_NAME] = CONVERT(VARCHAR(50), 'Current Financial Year'),
[LONG_DATE_NAME] = CONVERT(VARCHAR(50), 'Current Financial Year (' +
										CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar)
                                 +')'
                                 ),
[DATE_FROM] = CONVERT(DATETIME,(CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' + CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar)),103),
[DATE_TO] = DATEADD(ms,-2,DATEADD(year,1,CONVERT(DATETIME,(CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' + CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar)),103))),
[YEAR] = NULL,
[QUARTER] = NULL,
[YEAR_AND_QUARTER] = NULL, -- Year and quarter
[MONTH_NUMBER] = NULL,
[MONTH_NAME] = NULL,
[MONTH_YEAR] = NULL,
[MONTH_YEAR_ABBREVIATED] = NULL,
[MONTH_ABBREVIATED] = NULL,
[MONTH_NAME_SORTED] = NULL,
[WEEK_NUMBER] = NULL,
[WEEK_STARTING] = NULL,
[DAY_NUMBER] = NULL,
[DAY_NAME] = NULL,
[DAY_NAME_SORTED] = NULL,  
[DAY_NUMBER_ORDINAL] = NULL,
[DAY_AND_NAME] = NULL,
[DAY_AND_NAME_ABBREVIATED] = NULL,
[DD_MMM_YYYY] = NULL,
[DD_MMM_YY] = NULL,
[MM_DD_YYYY] = NULL


UNION
-- ‘Last Financial Year’ Had to take this year from Sage as customers vary
SELECT  
[ROWKEY]      = 18, 
[DATEKEY] = NULL,
[DATE_PERIOD_KEY] = NULL,
[DATE] = NULL,
[DATE_DATE] = NULL,
[SHORT_DATE_NAME] = CONVERT(VARCHAR(50), 'Last Financial Year'),
[LONG_DATE_NAME] = CONVERT(VARCHAR(50), 'Last Financial Year (' +
										CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) - 1 as varchar)
                                 +')'
                                 ),
[DATE_FROM] = DATEADD(YEAR,-1,CONVERT(DATETIME,(CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' + CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar)),103)),
[DATE_TO] = DATEADD(Year,-1,DATEADD(ms,-2,DATEADD(year,1,CONVERT(DATETIME,(CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' + CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar)),103)))),
[YEAR] = NULL,
[QUARTER] = NULL,
[YEAR_AND_QUARTER] = NULL, -- Year and quarter
[MONTH_NUMBER] = NULL,
[MONTH_NAME] = NULL,
[MONTH_YEAR] = NULL,
[MONTH_YEAR_ABBREVIATED] = NULL,
[MONTH_ABBREVIATED] = NULL,
[MONTH_NAME_SORTED] = NULL,
[WEEK_NUMBER] = NULL,
[WEEK_STARTING] = NULL,
[DAY_NUMBER] = NULL,
[DAY_NAME] = NULL,
[DAY_NAME_SORTED] = NULL,  
[DAY_NUMBER_ORDINAL] = NULL,
[DAY_AND_NAME] = NULL,
[DAY_AND_NAME_ABBREVIATED] = NULL,
[DD_MMM_YYYY] = NULL,
[DD_MMM_YY] = NULL,
[MM_DD_YYYY] = NULL


UNION
-- ‘Next Financial Year’ Had to take this year from Sage as customers vary
SELECT  
[ROWKEY]      = 19, 
[DATEKEY] = NULL,
[DATE_PERIOD_KEY] = NULL,
[DATE] = NULL,
[DATE_DATE] = NULL,
[SHORT_DATE_NAME] = CONVERT(VARCHAR(50), 'Next Financial Year'),
[LONG_DATE_NAME] = CONVERT(VARCHAR(50), 'Next Financial Year (' +
										CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) + 1 as varchar)
                                 +')'
                                 ),
[DATE_FROM] = DATEADD(YEAR,1,CONVERT(DATETIME,(CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' + CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar)),103)),
[DATE_TO] = DATEADD(Year,1,DATEADD(ms,-2,DATEADD(year,1,CONVERT(DATETIME,(CAST((SELECT TOP 1 FINANCIAL_YEAR FROM COMPANY) as varchar) + '-01-' + CAST((SELECT TOP 1 START_MONTH FROM COMPANY) as varchar)),103)))),
[YEAR] = NULL,
[QUARTER] = NULL,
[YEAR_AND_QUARTER] = NULL, -- Year and quarter
[MONTH_NUMBER] = NULL,
[MONTH_NAME] = NULL,
[MONTH_YEAR] = NULL,
[MONTH_YEAR_ABBREVIATED] = NULL,
[MONTH_ABBREVIATED] = NULL,
[MONTH_NAME_SORTED] = NULL,
[WEEK_NUMBER] = NULL,
[WEEK_STARTING] = NULL,
[DAY_NUMBER] = NULL,
[DAY_NAME] = NULL,
[DAY_NAME_SORTED] = NULL,  
[DAY_NUMBER_ORDINAL] = NULL,
[DAY_AND_NAME] = NULL,
[DAY_AND_NAME_ABBREVIATED] = NULL,
[DD_MMM_YYYY] = NULL,
[DD_MMM_YY] = NULL,
[MM_DD_YYYY] = NULL
GO
/****** Object:  View [dbo].[vw_Product_Attachment]    Script Date: 09/12/2019 14:43:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE VIEW [dbo].[vw_Product_Attachment]

as

SELECT Prod.STOCK_CODE
		,Prod.Description as Product_Description
		,ATT.STOCK_CODE as Also_Bought_Code
		,Att.DESCRIPTION as Also_Bought_Description
		,tot.Total_Orders
		,ATT.ORDER_NUMBER
		,Prod.PI_ID
		
		--,COUNT(DISTINCT ATT.Order_Number) as Orders_With_Item

FROM

		(SELECT so.ORDER_NUMBER
				,st.STOCK_CODE
				,st.DESCRIPTION
				,st.PI_ID

		FROM 
				[dbo].[SALES_ORDER] so
				INNER JOIN 	[dbo].[SOP_ITEM] sop
					ON sop.ORDER_NUMBER = so.ORDER_NUMBER
						AND sop.PI_ID = so.PI_ID
				LEFT JOIN [dbo].[STOCK] st
					ON st.STOCK_CODE = sop.STOCK_CODE
						AND st.PI_ID = sop.PI_ID
		)Prod

	INNER JOIN 

		(SELECT so.ORDER_NUMBER
				,st.STOCK_CODE
				,st.DESCRIPTION
				,st.PI_ID

		FROM 
				[dbo].[SALES_ORDER] so
				INNER JOIN 	[dbo].[SOP_ITEM] sop
					ON sop.ORDER_NUMBER = so.ORDER_NUMBER
						AND sop.PI_ID = so.PI_ID
				LEFT JOIN [dbo].[STOCK] st
					ON st.STOCK_CODE = sop.STOCK_CODE
						AND st.PI_ID = sop.PI_ID
		)Att

	ON Prod.Order_Number = Att.Order_Number
		AND prod.PI_ID = Att.PI_ID

	INNER JOIN 

		(SELECT COUNT(DISTINCT so.ORDER_NUMBER) as Total_Orders
				,st.STOCK_CODE
				,st.DESCRIPTION
				,st.PI_ID

		FROM 
				[dbo].[SALES_ORDER] so
				INNER JOIN 	[dbo].[SOP_ITEM] sop
					ON sop.ORDER_NUMBER = so.ORDER_NUMBER
						AND sop.PI_ID = so.PI_ID
				LEFT JOIN [dbo].[STOCK] st
					ON st.STOCK_CODE = sop.STOCK_CODE
						AND st.PI_ID = sop.PI_ID

		GROUP BY st.STOCK_CODE
				,st.DESCRIPTION
				,st.PI_ID
		)Tot

	ON Prod.STOCK_CODE = tot.STOCK_CODE
		AND Prod.PI_ID = tot.PI_ID


WHERE Prod.STOCK_CODE <> Att.STOCK_CODE


--GROUP BY Prod.STOCK_CODE
--		,Prod.Description
--		,ATT.STOCK_CODE
--		,Att.DESCRIPTION
--		,tot.Total_Orders
--		,ATT.ORDER_NUMBER



GO
/****** Object:  View [dbo].[vw_Product_Attachment_Invoice]    Script Date: 09/12/2019 14:43:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE VIEW [dbo].[vw_Product_Attachment_Invoice]

as

SELECT Prod.STOCK_CODE
		,Prod.Description as Product_Description
		,ATT.STOCK_CODE as Also_Bought_Code
		,Att.DESCRIPTION as Also_Bought_Description
		,tot.Total_Invoices
		,ATT.INVOICE_NUMBER
		--,COUNT(DISTINCT ATT.Order_Number) as Orders_With_Item
		,prod.PI_ID

FROM

		(SELECT i.INVOICE_NUMBER
				,st.STOCK_CODE
				,st.DESCRIPTION
				,st.PI_ID

		FROM 
				[dbo].[INVOICE] i
				INNER JOIN 	[dbo].[INVOICE_ITEM] ii
					ON i.INVOICE_NUMBER = ii.INVOICE_NUMBER
						AND i.PI_ID = ii.PI_ID
				LEFT JOIN [dbo].[STOCK] st
					ON st.STOCK_CODE = ii.STOCK_CODE
						AND st.PI_ID = ii.PI_ID
		)Prod

	INNER JOIN 

		(SELECT i.INVOICE_NUMBER
				,st.STOCK_CODE
				,st.DESCRIPTION
				,st.PI_ID

		FROM 
				[dbo].[INVOICE] i
				INNER JOIN 	[dbo].[INVOICE_ITEM] ii
					ON i.INVOICE_NUMBER = ii.INVOICE_NUMBER
						AND i.PI_ID = ii.PI_ID
				LEFT JOIN [dbo].[STOCK] st
					ON st.STOCK_CODE = ii.STOCK_CODE
						AND st.PI_ID = ii.PI_ID
		)Att

	ON Prod.INVOICE_Number = Att.INVOICE_Number
		AND prod.PI_ID = att.PI_ID

	INNER JOIN 

		(SELECT COUNT(DISTINCT i.INVOICE_NUMBER) as Total_Invoices
				,st.STOCK_CODE
				,st.DESCRIPTION
				,st.PI_ID

		FROM 
				[dbo].[INVOICE] i
				INNER JOIN 	[dbo].[INVOICE_ITEM] ii
					ON i.INVOICE_NUMBER = ii.INVOICE_NUMBER
						AND i.PI_ID = ii.PI_ID
				LEFT JOIN [dbo].[STOCK] st
					ON st.STOCK_CODE = ii.STOCK_CODE
						AND st.PI_ID = ii.PI_ID

		GROUP BY st.STOCK_CODE
				,st.DESCRIPTION
				,st.PI_ID
		)Tot

	ON Prod.STOCK_CODE = tot.STOCK_CODE
		AND Prod.PI_ID = tot.PI_ID


WHERE Prod.STOCK_CODE <> Att.STOCK_CODE




GO
/****** Object:  View [dbo].[vw_Product_Invoice_Dates]    Script Date: 09/12/2019 14:43:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



/****** Script for SelectTopNRows command from SSMS  ******/

CREATE VIEW [dbo].[vw_Product_Invoice_Dates]

AS

SELECT 
		st.DESCRIPTION
		,st.STOCK_CODE
		,MAX(i.INVOICE_DATE) as Last_Invoice_Date
		,MIN(i.INVOICE_DATE) as First_Invoice_Date
		,st.PI_ID

	FROM 
				[dbo].[INVOICE] i
				INNER JOIN 	[dbo].[INVOICE_ITEM] ii
					ON i.INVOICE_NUMBER = ii.INVOICE_NUMBER
						AND i.PI_ID = ii.PI_ID
				LEFT JOIN [dbo].[STOCK] st
					ON st.STOCK_CODE = ii.STOCK_CODE
						AND st.PI_ID = ii.PI_ID

  WHERE i.INVOICE_OR_CREDIT = 'Invoice'

GROUP BY st.DESCRIPTION
		,st.STOCK_CODE
		,st.PI_ID
		
		
GO
/****** Object:  View [dbo].[vw_Stock_Shortfall]    Script Date: 09/12/2019 14:43:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE View [dbo].[vw_Stock_Shortfall]

as

 SELECT   
	  st.[STOCK_CODE]
	  ,st.DESCRIPTION
      ,MAX(st.QTY_IN_STOCK) 			as QTY_IN_STOCK
      ,SUM(sop.QTY_ORDER)				as QTY_ON_ORDER_SALES
	  ,MAX(st.[QTY_ON_ORDER])			as QTY_ON_ORDER_STOCK
	  ,SUM(sop.[QTY_ORDER]) - (SUM(sop.[QTY_ALLOCATED]) + SUM(sop.[QTY_DESPATCH]) + SUM(sop.[QTY_DELIVERED]))		 as UNALLOCATED
	  ,SUM(sop.[QTY_ALLOCATED])			as ALLOCATED
	  ,SUM(sop.[QTY_DESPATCH])			as DESPATCHED
	  ,SUM(sop.[QTY_DELIVERED])			as DELIVERED
	  ,MAX(st.QTY_REORDER_LEVEL)		as REORDER_LEVEL


	  ,CASE WHEN ((SUM(sop.[QTY_ORDER]) - (SUM(sop.[QTY_ALLOCATED]) + SUM(sop.[QTY_DESPATCH]) + SUM(sop.[QTY_DELIVERED]))) 
					+ SUM(sop.[QTY_ALLOCATED]) 
					- CASE WHEN MAX(st.QTY_IN_STOCK) < 0 THEN 0 ELSE MAX(st.QTY_IN_STOCK) END 
					- MAX(st.[QTY_ON_ORDER])
				 ) < 0 
			THEN 0
			ELSE ((SUM(sop.[QTY_ORDER]) - (SUM(sop.[QTY_ALLOCATED]) + SUM(sop.[QTY_DESPATCH]) + SUM(sop.[QTY_DELIVERED]))) 
					+ SUM(sop.[QTY_ALLOCATED]) 
					- CASE WHEN MAX(st.QTY_IN_STOCK) < 0 THEN 0 ELSE MAX(st.QTY_IN_STOCK) END
					 - MAX(st.[QTY_ON_ORDER])
				 )
		END			as SHORTFALL
		
		,(CASE WHEN MAX(st.QTY_IN_STOCK) < 0 THEN 0 ELSE MAX(st.QTY_IN_STOCK) END  +  MAX(st.[QTY_ON_ORDER])) - MAX(st.QTY_REORDER_LEVEL) as Buffer_Level

			  ,st.PI_ID

	
 FROM [dbo].[STOCK] st
		
		INNER JOIN  [dbo].[SOP_ITEM] sop
			ON st.STOCK_CODE = sop.STOCK_CODE
				AND st.PI_ID = sop.PI_ID
		INNER JOIN [dbo].[SALES_ORDER] so
			ON sop.ORDER_NUMBER = so.ORDER_NUMBER
				AND sop.PI_ID = so.PI_ID
  
 WHERE so.ORDER_OR_QUOTE = 'Sales Order'
		AND st.IGNORE_STK_LVL_FLAG IN (0,1)  ---bring stock only

 GROUP BY st.[STOCK_CODE]
			,st.DESCRIPTION
		,st.PI_ID
GO
