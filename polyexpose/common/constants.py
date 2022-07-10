# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# Config
ICEBERG = "iceberg"
# Arguments
ARG_SQL_COMMAND = "sql_command"
ARG_GCS_WAREHOUSE_DIR = "gcs_warehouse_dir"
ARG_GCS_TEMP_BUCKET = "gcs_temp_bucket"
ARG_TABLE_NAME = "table_name"
ARG_BIGQUERY_DATASET = "bigquery_dataset"
ARG_EXCHANGE_DIR = "exchange_dir"
ARG_PROJECT_NUMBER = "project_number"
ARG_REGION = "region"
ARG_TOPIC_ID = "topic_id"
ARG_STREAM_TIME = "stream_time"
ARG_TIMESTAMP_OFFSET_MILI = "timestamp_offset_mili"
ARG_DATA_DIR = "data_dir"
# Expose modes
EXPOSE_MODE_SQL = "SQL"
EXPOSE_MODE_CSV = "CSV"
EXPOSE_MODE_GQL = "GQL"
EXPOSE_MODE_EVENT = "EVENT"
# SQL commands
SQL_QUERY_SELECT = "SELECT"
SQL_QUERY_FROM = "FROM"
SQL_QUERY_WHERE = "WHERE"
SQL_QUERY_AND = "AND"
SQL_SHOW_TABLES = "SHOW TABLES;"
SQL_DESCRIBE_TABLE = "DESCRIBE TABLE"
# SPARK Template names
SPARK_TEMPLATE_ICEBERG_SHOW_CATALOG = "iceberg_show_catalog"
SPARK_TEMPLATE_ICEBERG_EXEC_SQL = "iceberg_exec_sql"
SPARK_TEMPLATE_ICEBERG_EXPORT_CSV = "iceberg_export_csv"
SPARK_TEMPLATE_ICEBERG_EXPORT_BIGQUERY = "iceberg_export_bigquery"
SPARK_TEMPLATE_ICEBERG_STREAM_PUBSUB = "iceberg_stream_pubsub"
SPARK_TEMPLATE_CREATE_SAMPLE_TABLE = "create_sample_table"
# SPARK Jars
GCS_JAR_FOLDER = "/jars/"
SPARK_ICEBERG_JAR = "iceberg-spark-runtime-3.1_2.12-0.13.2.jar"
SPARK_BIGQUERY_JAR = "spark-bigquery-with-dependencies_2.12-0.25.2.jar"
SPARK_PUBSUBLITE_JAR = "pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar"
# Hasura
HASURA_API_GRAPHQL = "v1/graphql"
HASURA_API_METADATA = "v1/metadata"
HASURA_BQ_SOURCE_PREFIX = "polyexpose_bq_source"
# Log parsing
LOG_PARSE_TABLE_DELIMITER = "TABLE:"
RE_GCS = "gs://(.*?)/(.*)"
LINE_SPLIT_NL = "\n"
LINE_SPLIT_BLANK = " "
GS_PREFIX = "gs://"
# Internal
CATALOG_CACHE_LOCAL_FILE = ".catalog_cache"
