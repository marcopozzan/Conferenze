--------------------------------------------------- DEMO 1 ---------------------------------------------------
--Numero di record 200319766
SELECT count(*)
FROM dbo.[Sales]
---Verificare da profiler che arrivano le interrogazioni



--Creazione della sqlsat921.SalesByYear
--DROP TABLE dbo.SalesByYear
SELECT Date.[CalendarYear],
       SUM(sales.quantity) AS SumOfQuantity
INTO dbo.SalesByYear
FROM dbo.[Sales] AS sales
    LEFT OUTER JOIN dbo.Date AS Date
        ON sales.[DateKey] = Date.[DateKey]
GROUP BY Date.[CalendarYear];

--QUERY in DAX

--Creazione della sqlsat921.SalesByCustomerAndYear
--DROP TABLE sqlsat921.SalesByCustomerAndYear
SELECT Date.[CalendarYear],
	   Sales.CustomerKey,
       SUM(sales.quantity) AS SumOfQuantity
INTO dbo.SalesByCustomerAndYear
FROM dbo.[Sales] AS sales
    LEFT OUTER JOIN dbo.Date AS Date
        ON sales.[DateKey] = Date.[DateKey]
GROUP BY Date.[CalendarYear],
		 Sales.CustomerKey

--QUERY in DAX


--SYNAPSE
--ALTER TABLE Sales SWITCH PARTITION 2 TO  Sales_20070101 PARTITION 2;

--SQL SERVER
--  DELETE FROM [Contoso].[sqlsat921].[Sales]
--  Where YEAR(Sales.[Datekey]) = 2007 

-----Ricarico le righe
--SELECT [DateKey]
--      ,[ProductKey]
--      ,[CustomerKey]
--      ,[Quantity]
--      ,[Amount]
--      ,[Cost]
--  INTO [sqlsat921].[Sales]
--  FROM [Contoso].[sqlsat921].[Sales_2007]
--  Where YEAR(Sales.[Datekey]) = 2007 