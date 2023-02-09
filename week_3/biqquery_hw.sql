--  Create an external table using the fhv 2019 data.
CREATE OR REPLACE EXTERNAL TABLE `dtc-dataengineering-375516.trips_data_all.fhv_2019_data`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_dtc-dataengineering-375516/week_3/data/fhv_data_2019_*.csv.gz']
);

-- What is the count for fhv vehicle records for year 2019?
SELECT count(*) FROM `dtc-dataengineering-375516.trips_data_all.fhv_2019_data` LIMIT 10

-- Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
SELECT count(distinct(`Affiliated_base_number`)) from `dtc-dataengineering-375516.trips_data_all.fhv_2019_data` LIMIT 10
SELECT count(distinct(`Affiliated_base_number`)) from `dtc-dataengineering-375516.trips_data_all.bq_fhv_2019_data` LIMIT 10

--  How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
SELECT count(*) from `dtc-dataengineering-375516.trips_data_all.bq_fhv_2019_data` WHERE `PUlocationID` IS NULL and `DOlocationID` IS NULL LIMIT 10

--  Implement the optimized solution you chose for question 4.
CREATE OR REPLACE TABLE `dtc-dataengineering-375516.trips_data_all.fhv_2019_data_part_clust`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY `Affiliated_base_number` AS
SELECT * FROM `dtc-dataengineering-375516.trips_data_all.fhv_2019_data`

--  Write a query to retrieve the distinct affiliated_base_number 
--  between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).
Select distinct(Affiliated_base_number)
FROM `dtc-dataengineering-375516.trips_data_all.fhv_2019_data_part_clust`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31'

Select distinct(Affiliated_base_number)
FROM `dtc-dataengineering-375516.trips_data_all.bq_fhv_2019_data`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31'