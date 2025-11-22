CREATE EXTERNAL TABLE `sa_mdse_dl_secure.mak_physl_invt_doc`(
  -- Hudi Meta Columns
  `_hoodie_commit_time` string COMMENT 'Hudi Meta Column',
  `_hoodie_commit_seqno` string COMMENT 'Hudi Meta Column',
  `_hoodie_record_key` string COMMENT 'Hudi Meta Column',
  `_hoodie_partition_path` string COMMENT 'Hudi Meta Column',
  `_hoodie_file_name` string COMMENT 'Hudi Meta Column',
  
-- START, from here to end this will be manual work,
  -- Primary Key Fields, like below
  -- `clnt` string COMMENT 'MANDT | Client - PRIMARY KEY',

  -- Business Columns, like below
  --`trans_event_type` string COMMENT 'VGART | Transaction/Event Type',
--- END ---

  `ds_load_ts` timestamp COMMENT 'DS_LOAD_START_TS | Data Load Timestamp'

)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
   'hoodie.query.as.ro.table'='false',
   'path'='gs://86009702bc2c3bcd2c1c64cfb8e9a2e7ed7046d5e4b77786956ff1fe50586d/mak_physl_invt_doc')
STORED AS INPUTFORMAT
   'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
   'gs://86009702bc2c3bcd2c1c64cfb8e9a2e7ed7046d5e4b77786956ff1fe50586d/mak_physl_invt_doc'