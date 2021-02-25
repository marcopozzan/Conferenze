--Creazione Database su master
CREATE DATABASE DataSatDB

--Creazione delle viste per i file CSV su database DataSatDB 
--DROP VIEW IF EXISTS trip202001_csv;
--GO
CREATE VIEW trip202001_csv AS
SELECT 
VendorID
,cast(tpep_pickup_datetime as DATE) tpep_pickup_datetime
,cast(tpep_dropoff_datetime as DATE) tpep_dropoff_datetime
,passenger_count
-- ,trip_distance 
-- ,RateCodeID 
-- ,store_and_fwd_flag 
-- ,PULocationID
-- ,DOLocationID
-- ,payment_type 
-- ,fare_amount
-- ,extra
-- ,mta_tax 
-- ,tip_amount 
-- ,tolls_amount 
-- ,improvement_surcharge
,total_amount 
-- ,congestion_surcharge
FROM
    OPENROWSET(
        BULK N'https://adlsdemorepository.dfs.core.windows.net/synapse/yellow_tripdata_2020-01.csv',
        FORMAT = 'CSV',
        PARSER_VERSION='2.0',
HEADER_ROW = TRUE
) 
WITH(
VendorID INT,
tpep_pickup_datetime DATETIME2,
tpep_dropoff_datetime DATETIME2,
passenger_count INT,
trip_distance DECIMAL(10,2),
RateCodeID INT,
store_and_fwd_flag VARCHAR(10),
PULocationID INT,
DOLocationID INT,
payment_type INT,
fare_amount DECIMAL(10,2),
extra DECIMAL(10,2),
mta_tax DECIMAL(10,2),
tip_amount DECIMAL(10,2),
tolls_amount DECIMAL(10,2),
improvement_surcharge DECIMAL(10,2),
total_amount DECIMAL(10,2),
congestion_surcharge DECIMAL(10,2)
)
AS [trip202001_csv]


--Creazione delle viste per i file CSV su database DataSatDB 
-- DROP VIEW IF EXISTS trip202002_csv;
-- GO
CREATE VIEW trip202002_csv AS
SELECT 
VendorID
,cast(tpep_pickup_datetime as DATE) tpep_pickup_datetime
,cast(tpep_dropoff_datetime as DATE) tpep_dropoff_datetime
,passenger_count
-- ,trip_distance 
-- ,RateCodeID 
-- ,store_and_fwd_flag 
-- ,PULocationID
-- ,DOLocationID
-- ,payment_type 
-- ,fare_amount
-- ,extra
-- ,mta_tax 
-- ,tip_amount 
-- ,tolls_amount 
-- ,improvement_surcharge
,total_amount 
-- ,congestion_surcharge
FROM
    OPENROWSET(
        BULK N'https://adlsdemorepository.dfs.core.windows.net/synapse/yellow_tripdata_2020-02.csv',
        FORMAT = 'CSV',
        PARSER_VERSION='2.0',
HEADER_ROW = TRUE
) 
WITH(
VendorID INT,
tpep_pickup_datetime DATETIME2,
tpep_dropoff_datetime DATETIME2,
passenger_count INT,
trip_distance DECIMAL(10,2),
RateCodeID INT,
store_and_fwd_flag VARCHAR(10),
PULocationID INT,
DOLocationID INT,
payment_type INT,
fare_amount DECIMAL(10,2),
extra DECIMAL(10,2),
mta_tax DECIMAL(10,2),
tip_amount DECIMAL(10,2),
tolls_amount DECIMAL(10,2),
improvement_surcharge DECIMAL(10,2),
total_amount DECIMAL(10,2),
congestion_surcharge DECIMAL(10,2)
)
AS [trip202002_csv]




--Creazione delle viste per i file Parquet su database DataSatDB
-- DROP VIEW IF EXISTS trip202001_parquet;
-- GO
CREATE VIEW trip202001_parquet AS
SELECT 
VendorID
,cast(tpep_pickup_datetime as DATE) tpep_pickup_datetime
,cast(tpep_dropoff_datetime as DATE) tpep_dropoff_datetime
,passenger_count
-- ,trip_distance 
-- ,RateCodeID 
-- ,store_and_fwd_flag 
-- ,PULocationID
-- ,DOLocationID
-- ,payment_type 
-- ,fare_amount
-- ,extra
-- ,mta_tax 
-- ,tip_amount 
-- ,tolls_amount 
-- ,improvement_surcharge
,cast(total_amount as DECIMAL(10,2)) as total_amount
-- ,congestion_surcharge
FROM
    OPENROWSET(
        BULK N'https://adlsdemorepository.dfs.core.windows.net/synapse/yellow_tripdata_2020-01.parquet',
        FORMAT = 'PARQUET'
)
 as [trip202001_parquet ]



--Creazione delle viste per i file Parquet su database DataSatDB
-- DROP VIEW IF EXISTS trip202002_parquet;
-- GO
CREATE VIEW trip202002_parquet AS
SELECT 
VendorID
,cast(tpep_pickup_datetime as DATE) tpep_pickup_datetime
,cast(tpep_dropoff_datetime as DATE) tpep_dropoff_datetime
,passenger_count
-- ,trip_distance 
-- ,RateCodeID 
-- ,store_and_fwd_flag 
-- ,PULocationID
-- ,DOLocationID
-- ,payment_type 
-- ,fare_amount
-- ,extra
-- ,mta_tax 
-- ,tip_amount 
-- ,tolls_amount 
-- ,improvement_surcharge
,cast(total_amount as DECIMAL(10,2)) as total_amount
-- ,congestion_surcharge
FROM
    OPENROWSET(
        BULK N'https://adlsdemorepository.dfs.core.windows.net/synapse/yellow_tripdata_2020-02.parquet',
        FORMAT = 'PARQUET'
) as [trip202002_parquet]


-------------------------------------------------------Creiamo viste che mettono assieme i file CSV e Parquet -----------------------------
--creo una vista che unisce i due mesi CSV limitando solo 10 giorni dei vari mesi
-- DROP VIEW IF EXISTS trip2020_csv;
-- GO
CREATE VIEW trip2020_csv AS
select *
from dbo.trip202001_csv
WHERE (tpep_pickup_datetime  >= '20200101' AND tpep_pickup_datetime   < '20200110')
union all 
select *
from dbo.trip202002_csv
WHERE (tpep_pickup_datetime  >= '20200201' AND tpep_pickup_datetime   < '20200210')

--creo una vista che unisce i due mesi Parquet limitando solo 10 giorni dei vari mesi
-- DROP VIEW IF EXISTS trip2020_parquet;
-- GO
CREATE VIEW trip2020_parquet AS
select *
from dbo.trip202001_parquet
WHERE (tpep_pickup_datetime  >= '20200101' AND tpep_pickup_datetime   < '20200110')
union all 
select *
from dbo.trip202002_parquet
WHERE (tpep_pickup_datetime  >= '20200201' AND tpep_pickup_datetime   < '20200210')



--Verifico cardinalità
Select count(*)
from trip2020_csv

--Verifico cardinalità
Select count(*)
from trip2020_parquet


-------------------------------------------Creazione External Table-----------------------------------------------------------------------------------

CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Pa$$w0rdissima'

--DROP DATABASE SCOPED CREDENTIAL SqlOnDemandTest
CREATE DATABASE SCOPED CREDENTIAL SqlOnDemandTest
WITH IDENTITY='SHARED ACCESS SIGNATURE',  
SECRET = '?sv=2020-02-10&ss=bfqt&srt=sco&sp=rwdlacupx&se=2030-02-22T03:48:22Z&st=2021-02-21T19:48:22Z&spr=https&sig=2LtEjuarW9ERZ%2BaIMBUEdLOu0CKSy7BUSVaioM4bu74%3D'
GO
--DROP EXTERNAL DATA SOURCE DS_Taxi
CREATE EXTERNAL DATA SOURCE DS_Taxi WITH (
    LOCATION = 'https://adlsdemorepository.blob.core.windows.net',
    CREDENTIAL = SqlOnDemandTest
);

-- --Creaione del formato di importazione
-- DROP EXTERNAL FILE FORMAT exCsv
-- GO
CREATE EXTERNAL FILE FORMAT exCsv
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (FIELD_TERMINATOR =',',Encoding = 'UTF8',First_Row= 2) 
);

-- DROP EXTERNAL FILE FORMAT exParquet
-- GO
CREATE EXTERNAL FILE FORMAT exParquet
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'  
);


--Crea una copia locale nello storage di Synapse synapse/ext
-- DROP EXTERNAL TABLE trip_2020_csv_ext
-- GO
CREATE EXTERNAL TABLE trip_2020_csv_ext
WITH (
    LOCATION = 'synapse/ext/trip_2020_csv_ext',
    DATA_SOURCE = DS_Taxi,  
    FILE_FORMAT = exCsv
)
AS
SELECT *
from dbo.trip2020_csv
go

-- DROP EXTERNAL TABLE trip_2020_parquet_ext 
-- GO
CREATE EXTERNAL TABLE trip_2020_parquet_ext
WITH (
    LOCATION = 'synapse/ext/trip_2020_parquet_ext',
    DATA_SOURCE = DS_Taxi,  
    FILE_FORMAT = exParquet
)
AS
SELECT *
from dbo.trip2020_parquet
 