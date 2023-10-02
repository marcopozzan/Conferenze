SELECT YEAR(SO.OrderDate) AS OrderDateYear,
COUNT(SO.OrderDate) AS TotalOrderCount
FROM [DWH].[dbo].[silver_salesorder] SO
GROUP BY YEAR(SO.OrderDate);




SELECT ISNULL(C.ColorName,'No Colour') AS ColourName,
SUM(cast(SOL.Quantity as int)) AS TotalOrderLineQuantity,
SUM(cast(SOL.UnitPrice as numeric)) AS TotalOrderLineUnitPrice
FROM [DWH].[dbo].[silver_salesorderline]  SOL
INNER JOIN [DWH].[dbo].[silver_stockitems] SI ON SI.StockItemID = SOL.StockItemID
LEFT JOIN [DWH].[dbo].[silver_colors] C ON C.ColorID = SI.ColorID
GROUP BY ISNULL(C.ColorName,'No Colour');




SELECT 
    YEAR(SO.OrderDate) AS OrderDateYear,
    SC.SupplierCategoryName,
    SUM(cast(SOL.Quantity as int)) AS TotalOrderLineQuantity,
    SUM(cast(SOL.UnitPrice as numeric)) AS TotalOrderLineUnitPrice
FROM [DWH].[dbo].[silver_salesorderline] SOL
INNER JOIN [DWH].[dbo].[silver_salesorder] SO ON SO.OrderID = SOL.OrderID
INNER JOIN [DWH].[dbo].[silver_stockitems] SI ON SI.StockItemID = SOL.StockItemID
INNER JOIN [DWH].[dbo].[silver_suppliers] S ON SI.SupplierID = S.SupplierID
INNER JOIN [DWH].[dbo].[silver_categories] SC ON SC.SupplierCategoryID = S.SupplierCategoryID
GROUP BY YEAR(SO.OrderDate),
        SC.SupplierCategoryName;
