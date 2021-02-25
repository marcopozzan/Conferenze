-----------------TEST 1: Modalità importata con CSV e Parquet legati alle viste ---------------------
--Colleghiamoci con Power BI in modalità importata dalle viste
--Passo 1 Segnamoci i mega utilizzati per fare la base line -->T1:xxxx
SELECT * FROM sys.dm_external_data_processed
WHERE type = 'daily'
--Passo 2 Una volta caricato il file Power BI riverifichiamo quanto mi è costato il caricamento -->T2:xxxx
SELECT * FROM sys.dm_external_data_processed
WHERE type = 'daily'
--T1 per parquet
--T2 per parquet
-- Dimensione modello
----------Metto la griglia una card e uno slicer e verifichiamo che se eseguo di nuovo la sp non ho speso nulla
SELECT * FROM sys.dm_external_data_processed
WHERE type = 'daily'
--Passo 3 Carichiamo or in modalità importata il file parquet per vedere quanto costa il caricamento e l'occupazione del modello
--T1 per parquet
--T2 per parquet
-- Dimensione modello

-----------------TEST 2: Modalità importata con CSV e Parquet legati alle EXTERNAL TABLE ---------------------
--Colleghiamoci con Power BI in modalità importata ma alle tabelle esterne  e non alle viste
--Segnamoci i mega utilizzati per fare la base line -->T1:xxxx
SELECT * FROM sys.dm_external_data_processed
WHERE type = 'daily'
--Una volta caricato il file Power BI riverifichiamo quanto mi è costato il caricamento -->T2:xxxx
SELECT * FROM sys.dm_external_data_processed
WHERE type = 'daily'
--Passo 3 Carichiamo or ain modalità importata il file parquet per vedere quanto costa il caricamento e l'occupazione del modello
--T1 per parquet
--T2 per parquet
-- Dimensione modello
----------------TEST 3: Modalità DIRECT QUERY con CSV e Parquet legati attraverso le viste ---------------------
--Colleghiamoci con Power BI in modalità DIRECT QUERY ma alle viste e non alle tabelle esterne
--Segnamoci i mega utilizzati per fare la base line -->T1:xxxx
SELECT * FROM sys.dm_external_data_processed
WHERE type = 'daily'
--Una volta caricato il file Power BI riverifichiamo quanto mi è costato il caricamento -->T2:xxxx
SELECT * FROM sys.dm_external_data_processed
WHERE type = 'daily'
--Passo 3 Carichiamo or ain modalità importata il file parquet per vedere quanto costa il caricamento e l'occupazione del modello
--T1 per parquet
--T2 per parquet
-- Dimensione modello
--Note:legge sempre e comunque tutti i file csv e la dimensione non è sensibile ai filtri

----------------TEST 4: Modalità DIRECT QUERY con CSV e Parquet legati attraverso le EXTERNAL ---------------------
--Colleghiamoci con Power BI in modalità  DIRECT QUERY ma alle tabelle esterne  e non alle viste
--Segnamoci i mega utilizzati per fare la base line -->T1:xxxx
SELECT * FROM sys.dm_external_data_processed
WHERE type = 'daily'
--Una volta caricato il file Power BI riverifichiamo quanto mi è costato il caricamento -->T2:xxxx
SELECT * FROM sys.dm_external_data_processed
WHERE type = 'daily'
--Passo 3 Carichiamo or ain modalità importata il file parquet per vedere quanto costa il caricamento e l'occupazione del modello
--T1 per parquet
--T2 per parquet
-- Dimensione modello
--Note:non legge sempre e comunque tutti i file csv e la dimensione è sensibile ai filtri


----------------TEST 5: Modalità DIRECT QUERY con CSV e Parquet con AGGREGAZIONI IN POWER BI sia su viste  ---------------------
--Colleghiamoci con Power BI in modalità  DIRECT QUERY ma alle viste e non alle tabelle esterne con Aggregazioni
--Creo vista per aggregazione per csv
-- DROP VIEW IF EXISTS trip2020_csv_agg;
-- GO
CREATE VIEW trip2020_csv_agg AS
select SUM(total_amount) as total_amount,tpep_pickup_datetime
from dbo.trip2020_csv
group by tpep_pickup_datetime

--Creo vista per aggregazione per parquet
-- DROP VIEW IF EXISTS trip2020_parquet_agg;
-- GO
CREATE VIEW trip2020_parquet_agg AS
select SUM(total_amount) as total_amount,tpep_pickup_datetime
from dbo.trip2020_parquet
group by tpep_pickup_datetime

--Segnamoci i mega utilizzati per fare la base line -->T1:xxxx
SELECT * FROM sys.dm_external_data_processed
WHERE type = 'daily'
--Una volta caricato il file Power BI riverifichiamo quanto mi è costato il caricamento -->T2:xxxx
SELECT * FROM sys.dm_external_data_processed
WHERE type = 'daily'
--Passo 3 Carichiamo or ain modalità importata il file parquet per vedere quanto costa il caricamento e l'occupazione del modello
--T1 per parquet
--T2 per parquet
-- Dimensione modello
--Note:non legge sempre e comunque tutti i file csv e la dimensione è sensibile ai filtri

