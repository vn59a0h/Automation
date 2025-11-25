CREATE EXTERNAL TABLE `sa_supply_chain_dl_secure.mm_db_data_acrcy`(
  -- Hudi Meta Columns
  `_hoodie_commit_time` string COMMENT 'Hudi Meta Column',
  `_hoodie_commit_seqno` string COMMENT 'Hudi Meta Column',
  `_hoodie_record_key` string COMMENT 'Hudi Meta Column',
  `_hoodie_partition_path` string COMMENT 'Hudi Meta Column',
  `_hoodie_file_name` string COMMENT 'Hudi Meta Column',
  
  -- Primary Key Fields
  `clnt` string COMMENT 'MANDT | Client - PRIMARY KEY',
  `hndl_unit_id` string COMMENT 'HUIDENT | Handling Unit Identification - PRIMARY KEY',
  `pick` string COMMENT 'PICKER | Picker - PRIMARY KEY',
  `chec` string COMMENT 'CHECKER | Checker - PRIMARY KEY',
  `stag_lane` string COMMENT 'STG_LANE | Staging lane - PRIMARY KEY',
  `dt` string COMMENT 'CREATED | Date - PRIMARY KEY',
  `prod` string COMMENT 'MATID | Product - PRIMARY KEY',
  `arti_nbr` string COMMENT 'EAN11 | International Article Number (EAN/UPC) - PRIMARY KEY',
  `tm` string COMMENT 'TIME | Time - PRIMARY KEY',

  -- Business Columns
  `chk_line_catg` string COMMENT 'LINE_CAT | HU check line category',
  `whse_cmplx` string COMMENT 'LGNUM | Warehouse Number/Warehouse Complex',
  `sys_qty` string COMMENT 'SYSTEM_QTY | System quantity',
  `sys_unit_meas` string COMMENT 'SUOM | System unit of measure',
  `cnt_qty` string COMMENT 'COUNTED_QTY | Counted quantity',
  `cnt_unit_meas` string COMMENT 'CUOM | Counted unit of measure',
  `diff` string COMMENT 'DIFFERENCE | Difference',
  `diff_unit_meas` string COMMENT 'DUOM | Difference unit of measure',

  `ds_load_ts` timestamp COMMENT 'DS_LOAD_START_TS | Data Load Timestamp'

)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
   'hoodie.query.as.ro.table'='false',
   'path'='gs://932bc9f50d47e94a1727cacc3a61c0e7a3fb3573d08c1f5b637d33ea7c6980/mm_db_data_acrcy')
STORED AS INPUTFORMAT
   'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
   'gs://932bc9f50d47e94a1727cacc3a61c0e7a3fb3573d08c1f5b637d33ea7c6980/mm_db_data_acrcy'
