  --Modified query for salesorderheader (overview, excluding online orderflag)
WITH
  SalesOrders AS (
  SELECT
    sales_order_header.SalesOrderID AS sales_orderID,
    sales_order_header.CustomerID AS customerID,
    sales_order_header.RevisionNumber AS revision_number,
    sales_order_header.OrderDate AS order_date,
    sales_order_header.DueDate AS due_date,
    sales_order_header.ShipDate AS ship_date,
    sales_order_header.ContactID AS contactID,
    sales_order_header.SalesPersonID AS sales_personID,
    sales_order_header.TerritoryID AS territoryID,
    sales_order_header.BillToAddressID AS bill_to_addressID,
    sales_order_header.ShipToAddressID ship_to_addressID,
    sales_order_header.ShipMethodID AS ship_methodID,
    sales_order_header.SubTotal AS sub_total,
    sales_order_header.TaxAmt AS tax_amount,
    sales_order_header.Freight AS freight,
    sales_order_header.TotalDue AS total_due
  FROM
    `tc-da-1.adwentureworks_db.salesorderheader` sales_order_header ),
  ProductDetails AS (
  SELECT
    sales_order_detail.SalesOrderID AS sales_orderID,
    STRING_AGG(DISTINCT CAST(sales_order_detail.ProductID AS STRING), ', ') AS productIDs,
    STRING_AGG(DISTINCT product_category.Name, ', ') AS product_category_names,
    STRING_AGG(DISTINCT product_subcategory.Name, ', ') AS product_subcategory_names
  FROM
    `tc-da-1.adwentureworks_db.salesorderdetail` sales_order_detail
  INNER JOIN
    `tc-da-1.adwentureworks_db.product` product
  ON
    sales_order_detail.ProductID = product.ProductID
  INNER JOIN
    `tc-da-1.adwentureworks_db.productsubcategory` product_subcategory
  ON
    product.ProductSubcategoryID = product_subcategory.ProductSubcategoryID
  INNER JOIN
    `tc-da-1.adwentureworks_db.productcategory` product_category
  ON
    product_subcategory.ProductCategoryID = product_category.ProductCategoryID
  GROUP BY
    sales_order_detail.SalesOrderID )
SELECT
  SalesOrders.*,
  ProductDetails.productIDs,
  ProductDetails.product_category_names,
  ProductDetails.product_subcategory_names
FROM
  SalesOrders
LEFT JOIN
  ProductDetails
ON
  SalesOrders.sales_orderID = ProductDetails.sales_orderID
ORDER BY
  SalesOrders.sales_orderID;



  -- Aggregated total profit by productID, grouped by salesorderID
WITH
  SalesDetails AS (
  SELECT
    sales_order_detail.SalesOrderID,
    sales_order_detail.ProductID,
    sales_order_detail.OrderQty,
    sales_order_detail.UnitPrice,
    sales_order_detail.UnitPriceDiscount,
    sales_order_detail.LineTotal AS TotalSales
  FROM
    `tc-da-1.adwentureworks_db.salesorderdetail` sales_order_detail ),
  RankedProductCosts AS (
  SELECT
    product_cost_history.ProductID,
    product_cost_history.StandardCost,
    product_cost_history.EndDate,
    ROW_NUMBER() OVER (PARTITION BY product_cost_history.ProductID ORDER BY CASE WHEN product_cost_history.EndDate IS NULL THEN 1 ELSE 2 END , product_cost_history.EndDate DESC ) AS rankednumber
  FROM
    `tc-da-1.adwentureworks_db.productcosthistory` product_cost_history
  WHERE
    IFNULL(product_cost_history.EndDate, CURRENT_DATE()) <= CURRENT_DATE() ),
  ProductCosts AS (
  SELECT
    ProductID,
    StandardCost,
    IFNULL(EndDate, CURRENT_DATE()) AS LastCostDate
  FROM
    RankedProductCosts
  WHERE
    rankednumber = 1 ),
  SalesAndCosts AS (
  SELECT
    SalesDetails.SalesOrderID,
    SalesDetails.ProductID,
    SalesDetails.TotalSales,
    ProductCosts.StandardCost * SalesDetails.OrderQty AS TotalCost,
    SalesDetails.TotalSales - (ProductCosts.StandardCost * SalesDetails.OrderQty) AS Profit
  FROM
    SalesDetails
  INNER JOIN
    ProductCosts
  ON
    SalesDetails.ProductID = ProductCosts.ProductID )
SELECT
  SalesOrderID AS sales_orderID,
  STRING_AGG(DISTINCT CAST(ProductID AS STRING), ', ') AS productIDs,
  SUM (TotalSales) AS total_sales,
  SUM (TotalCost) AS total_cost,
  SUM(Profit) AS total_profit
FROM
  SalesAndCosts
GROUP BY
  SalesOrderID
ORDER BY
  SalesOrderID;




  -- Aggregated total profit with correspponding product category and subcategory
WITH
  SalesDetails AS (
  SELECT
    sales_order_detail.SalesOrderID,
    sales_order_detail.ProductID,
    sales_order_detail.OrderQty,
    sales_order_detail.LineTotal AS Sales,
  FROM
    `tc-da-1.adwentureworks_db.salesorderdetail` sales_order_detail ),
  RankedProductCosts AS (
  SELECT
    product_cost_history.ProductID,
    product_cost_history.StandardCost,
    product_cost_history.EndDate,
    ROW_NUMBER() OVER (PARTITION BY product_cost_history.ProductID ORDER BY CASE WHEN product_cost_history.EndDate IS NULL THEN 1 ELSE 2 END , product_cost_history.EndDate DESC ) AS rankednumber
  FROM
    `tc-da-1.adwentureworks_db.productcosthistory` product_cost_history
  WHERE
    IFNULL(product_cost_history.EndDate, CURRENT_DATE()) <= CURRENT_DATE() ),
  ProductCosts AS (
  SELECT
    ProductID,
    StandardCost,
    IFNULL(EndDate, CURRENT_DATE()) AS LastCostDate
  FROM
    RankedProductCosts
  WHERE
    rankednumber = 1 ),
  SalesAndCosts AS (
  SELECT
    SalesDetails.SalesOrderID AS sales_orderID,
    SalesDetails.ProductID AS productID,
    SalesDetails.Sales AS sales,
    ProductCosts.StandardCost * SalesDetails.OrderQty AS cost,
    SalesDetails.sales - (ProductCosts.StandardCost * SalesDetails.OrderQty) AS profit
  FROM
    SalesDetails
  INNER JOIN
    ProductCosts
  ON
    SalesDetails.ProductID = ProductCosts.ProductID ),
  ProductDetails AS (
  SELECT
    product.ProductID,
    product_category.Name AS product_category_name,
    product_subcategory.Name AS product_subcategory_name
  FROM
    `tc-da-1.adwentureworks_db.product` product
  INNER JOIN
    `tc-da-1.adwentureworks_db.productsubcategory` product_subcategory
  ON
    product.ProductSubcategoryID = product_subcategory.ProductSubcategoryID
  INNER JOIN
    `tc-da-1.adwentureworks_db.productcategory` product_category
  ON
    product_subcategory.ProductCategoryID = product_category.ProductCategoryID )
SELECT
  SalesAndCosts.sales_orderID,
  ProductDetails.product_category_name,
  ProductDetails.product_subcategory_name,
  SUM(SalesAndCosts.sales) AS total_sales_,
  SUM(SalesAndCosts.cost) AS total_cost_,
  SUM(SalesAndCosts.profit) AS total_profit_
FROM
  SalesAndCosts
LEFT JOIN
  ProductDetails
ON
  SalesAndCosts.productID = ProductDetails.productid
GROUP BY
  SalesAndCosts.sales_orderID,
  ProductDetails.product_category_name,
  ProductDetails.product_subcategory_name
ORDER BY
  SalesAndCosts.sales_orderID;



-- Customer overview info

SELECT
  Customer.CustomerId AS customerID,
  CONCAT(Contact.FirstName, ' ', Contact.LastName) AS customer_full_name,
  MAX (Address.City) AS city,
  MAX (State_Province.Name) AS state,
  Country_Region.name AS country,
FROM
  `tc-da-1.adwentureworks_db.salesorderheader` AS Sales_Order_Header
INNER JOIN
  `tc-da-1.adwentureworks_db.customer` AS Customer
ON
  Sales_Order_Header.CustomerID = Customer.CustomerId
INNER JOIN
  `tc-da-1.adwentureworks_db.contact` AS Contact
ON
  Sales_Order_Header.ContactId = Contact.ContactId
INNER JOIN
  `tc-da-1.adwentureworks_db.customeraddress` AS Customer_address
ON
  Customer_address.CustomerID = Customer.CustomerID
INNER JOIN
  `tc-da-1.adwentureworks_db.address` AS Address
ON
  Customer_address.AddressID = Address.AddressID
INNER JOIN
  `tc-da-1.adwentureworks_db.stateprovince` AS State_Province
ON
  Address.StateProvinceID = State_Province.StateProvinceID
INNER JOIN
  `tc-da-1.adwentureworks_db.countryregion` AS Country_Region
ON
  State_Province.CountryRegionCode = Country_Region.CountryRegionCode
GROUP BY
  customerID,
  customer_full_name,
  country;

