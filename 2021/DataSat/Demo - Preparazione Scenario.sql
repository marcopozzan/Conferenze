--Verifica del tipo di dati
EXEC sp_describe_first_result_set N'SELECT * FROM
    OPENROWSET(
        BULK N''https://adlsafterhour.dfs.core.windows.net/synapse/DataSat/yellow_tripdata_2020-01.parquet'',
        FORMAT = ''PARQUET''
        ) as test'




--Creazione delle viste sia per i file CSV che per i file parquet

DROP VIEW IF EXISTS trip202001_csv;
GO
CREATE VIEW trip202001_csv AS
SELECT 
VendorID
,cast(tpep_pickup_datetime as DATE) tpep_pickup_datetime
,cast(tpep_dropoff_datetime as DATE) tpep_dropoff_datetime
,passenger_count
,trip_distance 
,RateCodeID 
,store_and_fwd_flag 
,PULocationID
,DOLocationID
,payment_type 
,fare_amount
,extra
,mta_tax 
,tip_amount 
,tolls_amount 
,improvement_surcharge
,total_amount 
,congestion_surcharge
FROM
    OPENROWSET(
        BULK N'https://adlsafterhour.dfs.core.windows.net/synapse/DataSat/yellow_tripdata_2020-01.csv',
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


DROP VIEW IF EXISTS trip202002_csv;
GO
CREATE VIEW trip202002_csv AS
SELECT 
VendorID
,cast(tpep_pickup_datetime as DATE) tpep_pickup_datetime
,cast(tpep_dropoff_datetime as DATE) tpep_dropoff_datetime
,passenger_count
,trip_distance 
,RateCodeID 
,store_and_fwd_flag 
,PULocationID
,DOLocationID
,payment_type 
,fare_amount
,extra
,mta_tax 
,tip_amount 
,tolls_amount 
,improvement_surcharge
,total_amount 
,congestion_surcharge
FROM
    OPENROWSET(
        BULK N'https://adlsafterhour.dfs.core.windows.net/synapse/DataSat/yellow_tripdata_2020-02.csv',
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


DROP VIEW IF EXISTS trip202001_parquet;
GO
CREATE VIEW trip202001_parquet AS
SELECT 
VendorID
,cast(tpep_pickup_datetime as DATE) tpep_pickup_datetime
,cast(tpep_dropoff_datetime as DATE) tpep_dropoff_datetime
,passenger_count
,trip_distance 
,RateCodeID 
,store_and_fwd_flag 
,PULocationID
,DOLocationID
,payment_type 
,fare_amount
,extra
,mta_tax 
,tip_amount 
,tolls_amount 
,improvement_surcharge
,total_amount 
,congestion_surcharge
FROM
    OPENROWSET(
        BULK N'https://adlsafterhour.dfs.core.windows.net/synapse/DataSat/yellow_tripdata_2020-01.parquet',
        FORMAT = 'PARQUET'
)
 as [trip202001_parquet ]


DROP VIEW IF EXISTS trip202002_parquet;
GO
CREATE VIEW trip202002_parquet AS
SELECT 
VendorID
,cast(tpep_pickup_datetime as DATE) tpep_pickup_datetime
,cast(tpep_dropoff_datetime as DATE) tpep_dropoff_datetime
,passenger_count
,trip_distance 
,RateCodeID 
,store_and_fwd_flag 
,PULocationID
,DOLocationID
,payment_type 
,fare_amount
,extra
,mta_tax 
,tip_amount 
,tolls_amount 
,improvement_surcharge
,total_amount 
,congestion_surcharge
FROM
    OPENROWSET(
        BULK N'https://adlsafterhour.dfs.core.windows.net/synapse/DataSat/yellow_tripdata_2020-02.parquet',
        FORMAT = 'PARQUET'
) as [trip202002_parquet]

-------------------------------------------------------Creiamo viste che mettono assieme i file CSV e Parquet -----------------------------
--creo una vista che unisce i due mesi
DROP VIEW IF EXISTS trip2020_csv;
GO
CREATE VIEW trip2020_csv AS
select *
from dbo.trip202001_csv
WHERE (tpep_pickup_datetime  >= '20200101' AND tpep_pickup_datetime   < '20200107')
union all 
select *
from dbo.trip202002_csv
WHERE (tpep_pickup_datetime  >= '20200201' AND tpep_pickup_datetime   < '20200207')
--creo una vista che unisce i due mesi
DROP VIEW IF EXISTS trip2020_parquet;
GO
CREATE VIEW trip2020_parquet AS
select *
from dbo.trip202001_parquet
WHERE (tpep_pickup_datetime  >= '20200101' AND tpep_pickup_datetime   < '20200107')
union all 
select *
from dbo.trip202002_parquet
WHERE (tpep_pickup_datetime  >= '20200201' AND tpep_pickup_datetime   < '20200207')

-------------------------------------------------------Verifichiamo che il numero di righe sia uguale -----------------------------
--Verifico cardinalità
Select count(*)
from trip2020_csv

--Verifico cardinalità
Select count(*)
from trip2020_parquet


-----------------------------------------------------------Creazione External Table-----------------------------------------------------------------------------------
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Pa$$w0rdissima'

CREATE DATABASE SCOPED CREDENTIAL SqlOnDemandTest
WITH IDENTITY='SHARED ACCESS SIGNATURE',  
SECRET = '?sv=2019-12-12&ss=bfqt&srt=sco&sp=rwdlacupx&se=2021-01-30T05:45:52Z&st=2021-01-03T21:45:52Z&spr=https&sig=WJEdg94PkWgM7a4Ka5sJlCN1ARKheVFewn%2F5Wbh%2BwGY%3D'
GO
CREATE EXTERNAL DATA SOURCE DS_Taxi WITH (
    LOCATION = 'https://adlsafterhour.blob.core.windows.net',
    CREDENTIAL = SqlOnDemandTest
);

--Creaione del formato di importazione
DROP EXTERNAL FILE FORMAT exCsv
GO
CREATE EXTERNAL FILE FORMAT exCsv
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (FIELD_TERMINATOR =',',Encoding = 'UTF8',First_Row= 2) 
);

DROP EXTERNAL FILE FORMAT exParquet
GO
CREATE EXTERNAL FILE FORMAT exParquet
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'  
);


--Crea una copia locale nello storage di Synapse synapse/DataSat/ext
DROP EXTERNAL TABLE trip_2020_csv_ext
GO
CREATE EXTERNAL TABLE trip_2020_csv_ext
WITH (
    LOCATION = 'synapse/DataSat/trip_2020_csv_ext',
    DATA_SOURCE = DS_Taxi,  
    FILE_FORMAT = exCsv
)
AS
SELECT *
from dbo.trip2020_csv
go

DROP EXTERNAL TABLE taxi_parquet 
GO
CREATE EXTERNAL TABLE trip_2020_parquet_ext
WITH (
    LOCATION = 'synapse/DataSat/trip_2020_parquet_ext',
    DATA_SOURCE = DS_Taxi,  
    FILE_FORMAT = exParquet
)
AS
SELECT *
from dbo.trip2020_parquet
 
 

---------------------------------------------------------------Test 1--------------------------------------------------------------------

--Verifico i mega consumati 
SELECT * FROM sys.dm_external_data_processed
WHERE type = 'daily'


---------------------------------------------------------------Test 4--------------------------------------------------------------------
--Creo vista per aggregazione per csv
DROP VIEW IF EXISTS trip2020_csv_agg;
GO
CREATE VIEW trip2020_csv_agg AS
select SUM(total_amount) as total_amount,tpep_pickup_datetime
from  dbo.trip2020_csv --dbo.trip_2020_csv_ext
group by tpep_pickup_datetime
--Creo vista per aggregazione per parquet
DROP VIEW IF EXISTS trip2020_parquet_agg;
GO
CREATE VIEW trip2020_parquet_agg AS
select SUM(cast(total_amount as decimal)) as total_amount,tpep_pickup_datetime
from  dbo.trip2020_parquet --dbo.trip_2020_parquet_ext
group by tpep_pickup_datetime
