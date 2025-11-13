import os
import json
from airflow import models
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, DataprocDeleteClusterOperator, DataprocCreateClusterOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from datetime import datetime,timedelta
import logging
import pendulum
from plugins.CustomModule import  on_failure_spotlight, send_p1_email
import pytz
from airflow.exceptions import AirflowFailException
from airflow.models import Param
from bfdms.dpaas import BFDMSDataprocCreateClusterOperator
from airflow.operators.empty import EmptyOperator

# Task 1. Cluster creation
# b. Ingestion from ICDS
#         
# Task 3. Upsert Target
# Task 4. Cluster deletion

SENSITIVITY="HS"
PRIORITY="P2"
TAGS = ["Ranjan", "P2", "Ephemeral", "SA", "SECURE", "MDSE", "MDD", "mdd_physl_invt_doc", "SLT"]
CLUSTER_NAME = "sa-mdse-dl-secure-mdd-physl-invt-doc"
ARTIFACTORY_URL = Variable.get("ARTIFACTORY_URL")
BANNER_NAME="msb"
TRUE_FLAG = "true"
TABLE_NAME="ABC" #SAP table name


# Dynamic cluster configuration parameters
machine_type = "{{ params.machineType }}"

ing_driver_cores = "{{ params.ingDriverCores }}"
ing_driver_memory = "{{ params.ingDriverMemory }}"
ing_exec_instances = "{{ params.ingExecInstances }}"
ing_exec_cores = "{{ params.ingExecCores }}"
ing_exec_memory = "{{ params.ingExecMemory }}"
ing_memory_overhead = "{{ params.ingMemoryOverhead }}"

ups_driver_cores = "{{ params.upsDriverCores }}"
ups_driver_memory = "{{ params.upsDriverMemory }}"
ups_exec_instances = "{{ params.upsExecInstances }}"
ups_exec_cores = "{{ params.upsExecCores }}"
ups_exec_memory = "{{ params.upsExecMemory }}"
ups_memory_overhead = "{{ params.upsMemoryOverhead }}"
cluster_type = "{{ params.clusterType }}"

ALERT_EMAIL_ADDRESSES = ['intltechdatamassmart@email.wal-mart.com']
BQ_SYNC = TRUE_FLAG
CCM_URL = Variable.get("CCM_URL")
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
ENV = Variable.get("ENV")

SCHEDULE = ""
if "DEV" in ENV:
    SCHEDULE = None
elif "PROD" in ENV:
    SCHEDULE="0 22 * * *"  # Schedule updated to run daily at 11:30 PM

dag_arr = DAG_ID.split('-')
if len(dag_arr) == 5:
    load_type = dag_arr[2].lower()
    target_schema = dag_arr[1][0:2].lower()+'_'+dag_arr[3].lower()
    target_table = dag_arr[4].lower()
else:
    raise AirflowException("Please make sure DAG name is in right format:<INTLDLDAT>-<DIVISION:SAWM>-<LOAD_TYPE: INC,FULL>-<TARGET_SCHEMA>-<TARGET_TABLE>")

# **************************** Read Global bucket property file *****************************
def read_properties(gcs_file):
    try:
        property_file=""
        if "non-prod" in CCM_URL:
            property_file=gcs_hook.download(GCS_CODE_BUCKET, gcs_file, filename=gcs_file.split("/")[-1])
        elif "prod" in CCM_URL:
            property_file='/usr/local/airflow/'+gcs_file
        props = {}
        with open(property_file,'r') as datafile:
            for line in datafile:
                if (len(line) > 1 and '=' in line):
                    key, value = line.split("=")
                    props[key.lower()] = value.strip('\n')
        return props
    except (FileNotFoundError, IOError):
        print("Global Property File is not found in local at: ",property_file)

# **************************** Get Cluster Configurations *****************************
def read_cluster_config():
    try:
        file_bytes = ('/usr/local/airflow/'+CLUSTER_CONFIG_FILE) if(ENV=='PROD') else (gcs_hook.download(GCS_CODE_BUCKET, CLUSTER_CONFIG_FILE, filename=CLUSTER_CONFIG_FILE.split("/")[-1]))

        with open(file_bytes, 'r') as datafile:
            data = json.load(datafile)
            CLUSTER_CONFIG = data[CLUSTER_TYPE]
            CLUSTER_CONFIG['dpaas_env'] = SENSITIVITY.lower()
            CLUSTER_CONFIG['cluster_config']['gce_cluster_config']['service_account'] = SERVICE_ACCOUNT
            CLUSTER_CONFIG['cluster_config']['gce_cluster_config']['metadata']['startup-script-url'] = 'gs://{}/{}'.format(GCS_CODE_BUCKET, init_actions)

            # Dynamic cluster configuration for custom cluster type
            if CLUSTER_TYPE == 'custom':
                CLUSTER_CONFIG['cluster_config']['master_config']['machine_type_uri'] = machine_type
                CLUSTER_CONFIG['cluster_config']['worker_config']['machine_type_uri'] = machine_type
                CLUSTER_CONFIG['cluster_config']['secondary_worker_config']['machine_type_uri'] = machine_type
                # You can make these dynamic as well if needed
                CLUSTER_CONFIG['cluster_config']['worker_config']['num_instances'] = 2
                CLUSTER_CONFIG['cluster_config']['secondary_worker_config']['num_instances'] = 0

        return CLUSTER_CONFIG
    except (FileNotFoundError, IOError):
        print("Failed to download or read cluster config file: ",file_bytes)

GCS_CODE_BUCKET = Variable.get("GCS_CODE_BUCKET")
CONN_ID_DPAAS = Variable.get("CONN_ID_"+SENSITIVITY+"_DPAAS")
SOFTWARE_CONFIGS = Variable.get("SOFTWARE_CONFIG_"+SENSITIVITY)

# Provide GoogleCloud Platform connection ID
gcs_hook = GoogleCloudStorageHook(gcp_conn_id=CONN_ID_DPAAS, delegate_to=None)
global_prop_file = 'configs/global/'+SENSITIVITY.lower()+'_bucket_dpaas.properties'
global_props = read_properties(global_prop_file)
team_space = global_props['team_space']
dpaas_env = global_props['dpaas_env']
REGION = global_props['region']
PROJECT_ID = global_props['project_id']
EMAIL=global_props['email']
SERVICE_ACCOUNT = global_props['service_account_'+SENSITIVITY.lower()]
logging.info(f"REGION: {REGION}, PROJECT_ID: {PROJECT_ID}, SERVICE_ACCOUNT: {SERVICE_ACCOUNT}")

CLUSTER_CONFIG_FILE = "configs/cluster_config/dpaas_cluster_create.json"
init_actions = global_props['init_actions']
CLUSTER_TYPE = 'custom'  # Changed from 'medium' to 'custom' for dynamic configuration
CLUSTER_CONFIG = read_cluster_config()
logging.info(f"CLUSTER_CONFIG: {CLUSTER_CONFIG}")


def failure_callback(context):
    if "prod" in PROJECT_ID.lower():
        if PRIORITY == "P1":
            send_p1_email(context,PROJECT_ID,EMAIL)
        on_failure_spotlight(context,PROJECT_ID,REGION)

default_args = {
    'owner': 'walmart',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ALERT_EMAIL_ADDRESSES,
    'email_on_retry': False,
    'retries': 1,
    'sla': timedelta(minutes=60),
    'retry_delay': timedelta(minutes=3),
    'max_active_runs':1,
    'on_failure_callback': failure_callback,
}


# *************************************** Spark Job - Ingestion from SAP Source ************************************************

ICDS_INGESTION_MAIN_CLASS = "za.co.massmart.icds.ds.IngestionPipeline"

JARS_FILES = [
               f"gs://{GCS_CODE_BUCKET}/utilities/hudi_jar/hudi-spark3.3-bundle_2.12-1.0.0.jar",
               f"gs://{GCS_CODE_BUCKET}/utilities/hudi_jar/hudi-hadoop-mr-bundle-1.0.0.jar", # Required for creating Hive external table using org.apache.hudi.hadoop.HoodieParquetInputFormat data format
               f"gs://{GCS_CODE_BUCKET}/utilities/hudi_jar/google-http-client-jackson2-1.43.1.jar",
               f"gs://{GCS_CODE_BUCKET}/utilities/hudi_jar/google-cloud-bigquery-2.24.4.jar",
               f"gs://{GCS_CODE_BUCKET}/utilities/hudi_jar/hudi-gcp-bundle-1.0.0.jar",
               f"gs://{GCS_CODE_BUCKET}/utilities/common_jars/mssql-jdbc-12.2.0.jre8.jar",
               f"gs://{GCS_CODE_BUCKET}/utilities/app_jars/icds-ds.jar"
               ]

ICDS_INGESTION_JOB_NAME = "ICDS_To_DataLake_Raw_Ingestion"
ICDS_INGESTION_CMD_LINE_ARGS = ["--banner", BANNER_NAME, "--sapTableName", TABLE_NAME, "--dpaasFlag", TRUE_FLAG]

ICDS_INGESTION_SPARK_PROP = {
    "spark.app.name": ICDS_INGESTION_JOB_NAME,
    "spark.master": "yarn",
    "spark.submit.deployMode": "client",

    # Dynamic Spark configuration for ingestion
    "spark.driver.cores": ing_driver_cores,
    "spark.driver.memory": ing_driver_memory,
    "spark.driver.memoryOverheadFactor": ing_memory_overhead,
    "spark.executor.instances": ing_exec_instances,
    "spark.executor.cores": ing_exec_cores,
    "spark.executor.memory": ing_exec_memory,
    "spark.executor.memoryOverheadFactor": ing_memory_overhead,

    "spark.task.maxFailures": "3",
    "spark.executor.extraJavaOptions": "-Dsun.net.http.allowRestrictedHeaders=true",
    "spark.jars.repositories": ARTIFACTORY_URL,
    "spark.driver.extraJavaOptions": f"-Druntime.context.system.property.override.enabled=true -Druntime.context.environmentType=lab -Dscm.server.url={CCM_URL} -Dcom.walmart.platform.metrics.logfile.path=/dev/null -Dcom.walmart.platform.logging.logfile.path=/dev/null -Dcom.walmart.platform.txnmarking.logfile.path=/dev/null",
    "spark.yarn.maxAppAttempts": "1"
}

ICDS_INGESTION_SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "main_class": ICDS_INGESTION_MAIN_CLASS,
        "args": ICDS_INGESTION_CMD_LINE_ARGS,
        "jar_file_uris": JARS_FILES,
        "properties": ICDS_INGESTION_SPARK_PROP
    },
}

# *************************************** Spark Job - Upsert with Target Table ************************************************

UPSERT_TARGET_MAIN_CLASS = "za.co.massmart.icds.ds.UpsertPipelineV1"

UPSERT_TARGET_JOB_NAME = "Upsert_Target_DataLake_Table"

UPSERT_TARGET_CMD_LINE_ARGS = ["--banner", BANNER_NAME, "--sapTableName", TABLE_NAME,"--bqSync",BQ_SYNC]

UPSERT_TARGET_SPARK_PROP = {
    "spark.app.name": UPSERT_TARGET_JOB_NAME,
    "spark.master": "yarn",
    "spark.submit.deployMode": "client",

    # Dynamic Spark configuration for upsert
    "spark.driver.cores": ups_driver_cores,
    "spark.driver.memory": ups_driver_memory,
    "spark.driver.memoryOverheadFactor": ups_memory_overhead,
    "spark.executor.instances": ups_exec_instances,
    "spark.executor.cores": ups_exec_cores,
    "spark.executor.memory": ups_exec_memory,
    "spark.executor.memoryOverheadFactor": ups_memory_overhead,

    "spark.task.maxFailures": "3",
    "spark.executor.extraJavaOptions": "-Dsun.net.http.allowRestrictedHeaders=true",
    "spark.jars.repositories": ARTIFACTORY_URL,
    "spark.driver.extraJavaOptions": f"-Druntime.context.system.property.override.enabled=true -Druntime.context.environmentType=lab -Dscm.server.url={CCM_URL} -Dcom.walmart.platform.metrics.logfile.path=/dev/null -Dcom.walmart.platform.logging.logfile.path=/dev/null -Dcom.walmart.platform.txnmarking.logfile.path=/dev/null",
    "spark.yarn.maxAppAttempts": "1"
}

UPSERT_TARGET_SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "main_class": UPSERT_TARGET_MAIN_CLASS,
        "args": UPSERT_TARGET_CMD_LINE_ARGS,
        "jar_file_uris": JARS_FILES,
        "properties": UPSERT_TARGET_SPARK_PROP
    },
}

# DAG Parameters for dynamic configuration
dag_params = {
    "machineType": Param(default="n1-standard-4", type="string", description="Machine type for cluster nodes"),
    "ingDriverCores": Param(default="2", type="string", description="Driver cores for ingestion job"),
    "ingDriverMemory": Param(default="3g", type="string", description="Driver memory for ingestion job"),
    "ingExecInstances": Param(default="2", type="string", description="Executor instances for ingestion job"),
    "ingExecCores": Param(default="2", type="string", description="Executor cores for ingestion job"),
    "ingExecMemory": Param(default="4g", type="string", description="Executor memory for ingestion job"),
    "ingMemoryOverhead": Param(default="0.1", type="string", description="Memory overhead factor for ingestion job"),
    "upsDriverCores": Param(default="2", type="string", description="Driver cores for upsert job"),
    "upsDriverMemory": Param(default="3g", type="string", description="Driver memory for upsert job"),
    "upsExecInstances": Param(default="2", type="string", description="Executor instances for upsert job"),
    "upsExecCores": Param(default="2", type="string", description="Executor cores for upsert job"),
    "upsExecMemory": Param(default="4g", type="string", description="Executor memory for upsert job"),
    "upsMemoryOverhead": Param(default="0.1", type="string", description="Memory overhead factor for upsert job"),
    "clusterType": Param(default="micro", type="string", decription="cluster type for the job"),
    "numberInstances": Param(default="2", type="string", description="number of instances for job")
}
with models.DAG(DAG_ID, tags=TAGS, start_date=pendulum.datetime(2025, 6, 1, tz="UTC"), default_args=default_args, max_active_runs=1, catchup=False, schedule_interval=SCHEDULE, params=dag_params) as dag:

    create_cluster = BFDMSDataprocCreateClusterOperator(
        task_id='{}_create_cluster'.format(DAG_ID.replace('-','_').lower()),
        cluster_name=CLUSTER_NAME,
        region=REGION,
        project_id=PROJECT_ID,
        dpaas_config = CLUSTER_CONFIG,
        gcp_conn_id=CONN_ID_DPAAS,
        delete_on_error=True
    )

    icds_ingest_spark_task = DataprocSubmitJobOperator(
        task_id='icds_ingest_to_dl',
        job=ICDS_INGESTION_SPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id=CONN_ID_DPAAS
    )

    upsert_spark_task = DataprocSubmitJobOperator(
        task_id='upsert_dl_target_table',
        job=UPSERT_TARGET_SPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id=CONN_ID_DPAAS
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id='{}_delete_cluster'.format(DAG_ID.replace('-','_').lower()),
        cluster_name=CLUSTER_NAME,
        project_id=PROJECT_ID,
        region=REGION,
        trigger_rule='all_done',
        gcp_conn_id=CONN_ID_DPAAS
    )

    end = EmptyOperator(task_id="end")

    create_cluster >> icds_ingest_spark_task >> upsert_spark_task >> [delete_cluster, end]