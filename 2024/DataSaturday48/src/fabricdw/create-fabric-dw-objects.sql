
CREATE SCHEMA [Gold]
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


DROP TABLE IF EXISTS [Gold].[Calendar];
GO

CREATE TABLE [Gold].[Calendar](
	[Date] [date] NULL,
	[DayNum] [smallint] NULL,
	[DayOfWeek] [varchar](20) NULL,
	[DayOfWeekNum] [smallint] NULL,
	[Month] [varchar](20) NULL,
	[MonthNum] [smallint] NULL,
	[QuarterNum] [smallint] NULL,
	[Quarter] [varchar](2) NULL,
	[Year] [smallint] NULL)
GO


DROP TABLE IF EXISTS [Gold].[Customer];
GO

CREATE TABLE [Gold].[Customer](
	[YearsCustomer] [int] NULL,
	[BuyingGroupID] [int] NULL,
	[BuyingGroupName] [varchar](50) NULL,
	[CreditLimit] [decimal](10, 2) NULL,
	[CustomerCategoryID] [int] NULL,
	[CustomerID] [int] NULL,
	[CustomerName] [varchar](100) NULL,
	[DeliveryCityID] [int] NULL,
	[DeliveryCity] [varchar](50) NULL,
	[DeliveryStateProvince] [varchar](50) NULL,
	[DeliveryCountry] [varchar](60) NULL,
	[Region] [varchar](30) NULL,
	[Subregion] [varchar](30) NULL,
	[Continent] [varchar](30) NULL,
	[SalesTerritory] [varchar](50) NULL,
	[DeliveryPostalCode] [varchar](10) NULL,
	[IsOnCreditHold] [bit] NULL,
	[PaymentDays] [int] NULL,
	[AreaCode] [varchar](3) NULL)
GO

DROP TABLE IF EXISTS [Gold].[InvoicedSales];
GO

CREATE TABLE [Gold].[InvoicedSales](
	[InvoiceID] [int] NULL,
	[InvoiceLineID] [int] NULL,
	[CustomerID] [int] NULL,
	[StockItemID] [int] NULL,
	[SalespersonPersonID] [int] NULL,
	[InvoiceDate] [date] NULL,
	[LastUpdated] [datetime2](6) NULL,
	[Quantity] [int] NULL,
	[ExtendedPrice] [decimal](18, 2) NULL,
	[GrossProfit] [decimal](18, 2) NULL,
	[TaxAmount] [decimal](18, 2) NULL
) 
GO

DROP TABLE IF EXISTS [Gold].[Products];
GO

CREATE TABLE [Gold].[Products](
	[StockItemID] [int] NULL,
	[StockItemName] [varchar](100) NULL,
	[SupplierID] [int] NULL,
	[SupplierName] [varchar](100) NULL,
	[Brand] [varchar](20) NULL,
	[ColorName] [varchar](20) NULL,
	[LeadTimeDays] [int] NULL)
GO

DROP TABLE IF EXISTS [Gold].[SalesOrders];
GO

CREATE TABLE [Gold].[SalesOrders](
	[OrderID] [int] NULL,
	[OrderLineID] [int] NULL,
	[CustomerID] [int] NULL,
	[StockItemID] [int] NULL,
	[SalespersonPersonID] [int] NULL,
	[OrderDate] [date] NULL,
	[Quantity] [int] NULL,
	[ExtendedPrice] [decimal](18, 2) NULL,
	[LastUpdated] [datetime2](6) NULL
) 
GO

DROP TABLE IF EXISTS [Gold].[Salesperson];
GO

CREATE TABLE [Gold].[Salesperson](
	[PersonID] [int] NULL,
	[FullName] [varchar](50) NULL)

GO

-- Find/Replace [MyFTAFabricWarehouse] with your Warehouse Name
CREATE OR ALTER PROC [Gold].[IncrLoadInvoicedSales]
@StartDate DATETIME,
@EndDate DATETIME
AS
BEGIN

SET NOCOUNT ON;

DECLARE @UpdateCount INT, @InsertCount INT
-- exec [Gold].[IncrLoadInvoicedSales] null, null 

IF @StartDate IS NULL
BEGIN
    SELECT @StartDate = isnull(MAX(LastUpdated),'2013-01-01') 
    FROM [MyFTAFabricWarehouse].[Gold].[InvoicedSales]
END;

IF @EndDate IS NULL
BEGIN
    SET @EndDate = '9999-12-31'
END    

UPDATE target
SET target.InvoiceDate = source.InvoiceDate,
            target.CustomerID = source.CustomerID,
            target.StockItemID = source.StockItemID,
            target.SalespersonPersonID = source.SalespersonPersonID,
            target.ExtendedPrice = source.ExtendedPrice,
            target.Quantity = source.Quantity,
            target.GrossProfit = source.GrossProfit,
            target.TaxAmount = source.TaxAmount,
            target.LastUpdated = source.LastUpdated
FROM [MyFTAFabricWarehouse].[Gold].[InvoicedSales] AS target
    INNER JOIN [MyFTAFabricWarehouse].[Silver].[vInvoicedSales] AS source
    ON (target.InvoiceID = source.InvoiceID AND target.InvoiceLineID = source.InvoiceLineID)
    WHERE source.LastUpdated BETWEEN @StartDate and @EndDate;

 SELECT @UpdateCount = @@ROWCOUNT   

INSERT INTO [MyFTAFabricWarehouse].[Gold].[InvoicedSales] (InvoiceID, InvoiceLineID, InvoiceDate, CustomerID, StockItemID, SalespersonPersonID, 
            ExtendedPrice, Quantity,GrossProfit,TaxAmount, LastUpdated)
    SELECT source.InvoiceID, source.InvoiceLineID, source.InvoiceDate, source.CustomerID, source.StockItemID, source.SalespersonPersonID,
            source.ExtendedPrice, source.Quantity, source.GrossProfit, source.TaxAmount,source.LastUpdated
    FROM [MyFTAFabricWarehouse].[Silver].[vInvoicedSales] AS source
    LEFT JOIN [MyFTAFabricWarehouse].[Gold].[InvoicedSales] AS target
    ON (target.InvoiceID = source.InvoiceID AND target.InvoiceLineID = source.InvoiceLineID)
    WHERE target.InvoiceID IS NULL AND target.InvoiceLineID IS NULL AND source.LastUpdated  BETWEEN @StartDate and @EndDate
END

 SELECT @InsertCount = @@ROWCOUNT  

 SELECT @UpdateCount as UpdateCount, @InsertCount as InsertCount, @StartDate as MaxDate
GO
----


CREATE OR ALTER PROC [Gold].[IncrLoadSalesOrders]

@StartDate DATETIME,
@EndDate DATETIME
AS
BEGIN

SET NOCOUNT ON;

DECLARE @UpdateCount INT, @InsertCount INT
-- exec [Gold].[IncrLoadSalesOrders] null, null 

IF @StartDate IS NULL
BEGIN
    SELECT @StartDate = isnull(MAX(LastUpdated),'2013-01-01') 
    FROM [MyFTAFabricWarehouse].[Gold].[SalesOrders]
END;

IF @EndDate IS NULL
BEGIN
    SET @EndDate = '9999-12-31'
END  

UPDATE target
SET target.OrderDate = source.OrderDate,
            target.CustomerID = source.CustomerID,
            target.StockItemID = source.StockItemID,
            target.SalespersonPersonID = source.SalespersonPersonID,
            target.ExtendedPrice = source.ExtendedPrice,
            target.Quantity = source.Quantity,
            target.LastUpdated = source.LastUpdated
FROM [MyFTAFabricWarehouse].[Gold].[SalesOrders] AS target
    INNER JOIN [MyFTAFabricWarehouse].[Silver].[vSalesOrders] AS source
    ON (target.OrderID = source.OrderID AND target.OrderLineID = source.OrderLineID)
    WHERE source.LastUpdated BETWEEN @StartDate and @EndDate;

SELECT @UpdateCount = @@ROWCOUNT   

INSERT INTO [MyFTAFabricWarehouse].[Gold].[SalesOrders] (OrderID, OrderLineID, OrderDate, CustomerID, StockItemID, SalespersonPersonID, 
            ExtendedPrice, Quantity, LastUpdated)
    SELECT source.OrderID, source.OrderLineID, source.OrderDate, source.CustomerID, source.StockItemID, source.SalespersonPersonID,
            source.ExtendedPrice, source.Quantity, source.LastUpdated
    FROM [MyFTAFabricWarehouse].[Silver].[vSalesOrders] AS source
    LEFT JOIN [MyFTAFabricWarehouse].[Gold].[SalesOrders] AS target
    ON (target.OrderID = source.OrderID AND target.OrderLineID = source.OrderLineID)
    WHERE target.OrderID IS NULL AND target.OrderLineID IS NULL AND source.LastUpdated BETWEEN @StartDate and @EndDate;

SELECT @InsertCount = @@ROWCOUNT  

SELECT @UpdateCount as UpdateCount, @InsertCount as InsertCount, @StartDate as MaxDate   
END



