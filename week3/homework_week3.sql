-- q1

CREATE OR REPLACE EXTERNAL TABLE `dtc_zoom.dezoomcamp_fhv_tripdata.fhv_tripdata`

OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_zoomcamp_data_lake/fhv/2019/fhv_tripdata_2019-*.csv.gz']
);

-- query count external table
SELECT COUNT(*) FROM `dtc_zoom.dezoomcamp_fhv_tripdata.fhv_tripdata` ;


-- q2

CREATE OR REPLACE TABLE `dtc_zoom.dezoomcamp_fhv_tripdata.fhv_tripdata_nonpartition`
AS SELECT * FROM `dtc_zoom.dezoomcamp_fhv_tripdata.fhv_tripdata`;

-- check estimation of data before run query in bigquery 
-- external table
SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM `dtc_zoom.dezoomcamp_fhv_tripdata.fhv_tripdata`;

SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM `dtc_zoom.dezoomcamp_fhv_tripdata.fhv_tripdata_nonpartition` ;

-- q3

SELECT
  COUNT(*)
FROM `dtc_zoom.dezoomcamp_fhv_tripdata.fhv_tripdata_nonpartition`
WHERE
  PUlocationID IS NULL
  AND DOlocationID IS NULL ;


  -- q5

  CREATE OR REPLACE TABLE
  `dtc_zoom.dezoomcamp_fhv_tripdata.tripdata_partitioned_custered`
PARTITION BY
  DATE(pickup_datetime)
CLUSTER BY
  affiliated_base_number AS
SELECT
  *
FROM
  `dtc_zoom.dezoomcamp_fhv_tripdata.fhv_tripdata`; 
