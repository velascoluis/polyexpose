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

import polyexpose.common.utils as utils
import polyexpose.common.constants as constants
import yaml
import json
import logging
import importlib.resources


class IcebergTable:
    """
    IcebertTable class, main abstraction.
    It basically provides automation for exposing the table as:
    1) SQL - This expose mode, exposes the table as a SQL table. Not much to do here, we just need a SQL compute engine, SPARK in  this case, the solution accepts a random SQL statement thats gets executed against the Iceberg tables on GCS
    2) CSV - This expose mode exposes the table as CSV files, basically exports  the data on CSV format using SPARK, and leave it on a GCS exchnage folder
    3) GQL - This expose mode exposes the table with a GQL interface. For this expose mode, we rely on the OSS hasura GQL component to translate from GQL to SQL.
    4) EVENT - This expose mode exposes the table as a series of events. For this expose mode, we build and populate a pubsub lite topic. We accept a timestamp offset to setup if past data should be exposed as events or just new inserted data.
    """

    def __init__(self, table_name, config):
        self.__config = config
        self.__table_name = table_name
        self.__sql_enabled = False
        self.__csv_enabled = False
        self.__gql_enabled = False
        self.__event_enabled = False

    def show_schema(self):
        """
        Executes a plain SQL DESCRIBE TABLE to visualize the table schema.
        Print output on standard
        Same thing as df.printSchema
        """
        sql_command = constants.SQL_DESCRIBE_TABLE + " " + self.__table_name
        self.exec_sql(sql_command)

    def query_table(self, projection_list, predicate_list):
        """
        Executes a plain SQL query (SELECT). It builds a simple SQL statement accepting predicates and projections.
        Print output on standard
        :param list str projection_list: The fields to extract (e.g. field1, field2)
        :param list predicate_list: List of filters (e.g. field1 > 5000)
        """
        sql_command = constants.SQL_QUERY_SELECT + " "
        for i, projection in enumerate(projection_list):
            if i == len(projection_list) - 1:
                sql_command = sql_command + projection
            else:
                sql_command = sql_command + projection + ","
        sql_command = (
            sql_command + " " + constants.SQL_QUERY_FROM + " " + self.__table_name
        )
        if len(predicate_list) > 0:
            sql_command = sql_command + " " + constants.SQL_QUERY_WHERE
            for i, predicate in enumerate(predicate_list):
                if i == len(predicate_list) - 1:
                    sql_command = sql_command + " " + predicate
                else:
                    sql_command = (
                        sql_command + " " + predicate + constants.SQL_QUERY_AND
                    )
        logging.info(f"Executing SQL query: {sql_command}")
        self.exec_sql(sql_command)

    def exec_sql(self, sql_command):
        """
        Executes a random SQL statement.
        Print output on standard
        :param sql_command : The SQL command to execute.
        """
        if not self.__sql_enabled:
            logging.error(
                f"SQL Expose mode is not enabled on table: {self.__table_name}"
            )
            raise Exception("SQL not enabled!")
        for spark_template in self.__config["spark"]["spark_templates"]:
            if constants.SPARK_TEMPLATE_ICEBERG_EXEC_SQL in spark_template.keys():
                spark_script = (
                    constants.GS_PREFIX
                    + self.__config["gcs"]["bucket_name"]
                    + self.__config["gcs"]["scripts_folder"]
                    + spark_template[constants.SPARK_TEMPLATE_ICEBERG_EXEC_SQL]
                )
        job_output = utils.submit_job(
            self.__config["gcp"]["gcp_project_id"],
            self.__config["spark"]["dataproc_gcp_region"],
            self.__config["spark"]["dataproc_cluster_name"],
            self.__config["gcs"]["bucket_name"],
            spark_script,
            [
                f"--{constants.ARG_SQL_COMMAND}=" + sql_command,
                f"--{constants.ARG_GCS_WAREHOUSE_DIR}="
                + constants.GS_PREFIX
                + self.__config["gcs"]["bucket_name"]
                + self.__config["gcs"]["warehouse_dir_folder"],
            ],
        )
        print(job_output)

    def exec_gql(self, gql_command):
        """
        Executes a random GQL query.
        Print output on standard
        :param gql_command : The GQL command to execute.
        """
        if not self.__gql_enabled:
            logging.error(
                f"GQL Expose mode is not enabled on table: {self.__table_name}"
            )
            raise Exception("GQL not enabled!")
        request_output = utils.send_request(
            self.__config["hasura"]["url"] + constants.HASURA_API_GRAPHQL,
            gql_command,
            True,
        )
        print(request_output)

    def expose_as(self, expose_mode):
        """
        Enables exposure modes on the table
        :param expose_mode : A valid expose mode (SQL,GQL,CSV,EVENT)
        """
        if self.__table_name is None:
            logging.error("No table selected")
            raise Exception("No table selected")
        else:
            if expose_mode == constants.EXPOSE_MODE_SQL:
                self.__sql_enabled = True
            elif expose_mode == constants.EXPOSE_MODE_CSV:
                self.__csv_enabled = True
                utils.create_gcs_bucket(
                    self.__config["gcs"]["bucket_name"],
                    self.__config["gcs"]["bucket_location"],
                )
                for spark_template in self.__config["spark"]["spark_templates"]:
                    if (
                        constants.SPARK_TEMPLATE_ICEBERG_EXPORT_CSV
                        in spark_template.keys()
                    ):
                        spark_script = (
                            constants.GS_PREFIX
                            + self.__config["gcs"]["bucket_name"]
                            + self.__config["gcs"]["scripts_folder"]
                            + spark_template[
                                constants.SPARK_TEMPLATE_ICEBERG_EXPORT_CSV
                            ]
                        )
                job_output = utils.submit_job(
                    self.__config["gcp"]["gcp_project_id"],
                    self.__config["spark"]["dataproc_gcp_region"],
                    self.__config["spark"]["dataproc_cluster_name"],
                    self.__config["gcs"]["bucket_name"],
                    spark_script,
                    [
                        f"--{constants.ARG_EXCHANGE_DIR}="
                        + constants.GS_PREFIX
                        + self.__config["gcs"]["bucket_name"],
                        f"--{constants.ARG_TABLE_NAME}=" + self.__table_name,
                        f"--{constants.ARG_GCS_WAREHOUSE_DIR}="
                        + constants.GS_PREFIX
                        + self.__config["gcs"]["bucket_name"]
                        + self.__config["gcs"]["warehouse_dir_folder"],
                    ],
                )
                print(job_output)

            elif expose_mode == constants.EXPOSE_MODE_GQL:
                self.__gql_enabled = True
                """
                In order to expose data as GQL, we will use hasura.io, hasura takes data in a supported backend and expose it as GQL.
                We need to configure a few things here:
                1) hasura supported backends are postgres and BigQuery, so we need to place our Iceberg tables on one of them, we will use BigQuery
                2) Then we need to call the hasura API for registering the new BQ dataset - this is done via the bigquery_add_source operation
                3) Finally we call the API again the track the new table - this is done via the bigquery_track_table operation
                
                """
                # 1 Copy data to BigQuery
                utils.create_bq_dataset(
                    self.__config["gcp"]["gcp_project_id"],
                    self.__config["bigquery"]["dataset"],
                    self.__config["bigquery"]["dataset_location"],
                )
                spark_templates_list = self.__config["spark"]["spark_templates"]
                for spark_template in spark_templates_list:
                    if (
                        constants.SPARK_TEMPLATE_ICEBERG_EXPORT_BIGQUERY
                        in spark_template.keys()
                    ):
                        spark_script = (
                            constants.GS_PREFIX
                            + self.__config["gcs"]["bucket_name"]
                            + self.__config["gcs"]["scripts_folder"]
                            + spark_template[
                                constants.SPARK_TEMPLATE_ICEBERG_EXPORT_BIGQUERY
                            ]
                        )
                job_output = utils.submit_job(
                    self.__config["gcp"]["gcp_project_id"],
                    self.__config["spark"]["dataproc_gcp_region"],
                    self.__config["spark"]["dataproc_cluster_name"],
                    self.__config["gcs"]["bucket_name"],
                    spark_script,
                    [
                        f"--{constants.ARG_GCS_TEMP_BUCKET}="
                        + self.__config["gcs"]["bucket_name"],
                        f"--{constants.ARG_TABLE_NAME}=" + (self.__table_name),
                        f"--{constants.ARG_GCS_WAREHOUSE_DIR}="
                        + constants.GS_PREFIX
                        + self.__config["gcs"]["bucket_name"]
                        + self.__config["gcs"]["warehouse_dir_folder"],
                        f"--{constants.ARG_BIGQUERY_DATASET}="
                        + self.__config["bigquery"]["dataset"],
                    ],
                )
                print(job_output)
                # 2 Add BQ to hasura
                with open(self.__config["gcp"]["sa_key"], "r") as f:
                    sa_key_json = json.load(f)
                bigquery_add_source = {}
                bigquery_add_source["type"] = "bigquery_add_source"
                bigquery_add_source["args"] = {}
                bigquery_add_source["args"]["name"] = {}
                bigquery_add_source["args"]["name"] = (
                    constants.HASURA_BQ_SOURCE_PREFIX
                    + "_"
                    + (self.__table_name).replace(".", "_")
                )
                bigquery_add_source["args"]["configuration"] = {}
                bigquery_add_source["args"]["replace_configuration"] = True
                bigquery_add_source["args"]["configuration"][
                    "service_account"
                ] = sa_key_json
                bigquery_add_source["args"]["configuration"][
                    "project_id"
                ] = self.__config["gcp"]["gcp_project_id"]
                bigquery_add_source["args"]["configuration"]["datasets"] = [
                    self.__config["bigquery"]["dataset"]
                ]
                hasura_url = self.__config["hasura"]["url"]
                utils.send_request(
                    hasura_url + constants.HASURA_API_METADATA,
                    bigquery_add_source,
                    False,
                )
                # 3 Track table
                bigquery_track_table = {}
                bigquery_track_table["type"] = "bigquery_track_table"
                bigquery_track_table["args"] = {}
                bigquery_track_table["args"]["table"] = {}
                bigquery_track_table["args"]["table"]["dataset"] = {}
                bigquery_track_table["args"]["table"]["dataset"] = self.__config[
                    "bigquery"
                ]["dataset"]
                bigquery_track_table["args"]["table"]["name"] = (
                    self.__table_name
                ).replace(".", "_")
                bigquery_track_table["args"]["source"] = (
                    constants.HASURA_BQ_SOURCE_PREFIX
                    + "_"
                    + (self.__table_name).replace(".", "_")
                )
                utils.send_request(
                    hasura_url + constants.HASURA_API_METADATA,
                    bigquery_track_table,
                    False,
                )
            elif expose_mode == constants.EXPOSE_MODE_EVENT:
                self.__event_enabled = True
                for spark_template in self.__config["spark"]["spark_templates"]:
                    if (
                        constants.SPARK_TEMPLATE_ICEBERG_STREAM_PUBSUB
                        in spark_template.keys()
                    ):
                        spark_script = (
                            constants.GS_PREFIX
                            + self.__config["gcs"]["bucket_name"]
                            + self.__config["gcs"]["scripts_folder"]
                            + spark_template[
                                constants.SPARK_TEMPLATE_ICEBERG_STREAM_PUBSUB
                            ]
                        )
                utils.create_pubsub_lite(
                    self.__config["gcp"]["gcp_project_number"],
                    "reservation_" + self.__table_name,
                    self.__config["pubsublite"]["pubsublite_gcp_region"],
                    "topic_" + self.__table_name,
                    "subscription_" + self.__table_name,
                )
                job_output = utils.submit_job(
                    self.__config["gcp"]["gcp_project_id"],
                    self.__config["spark"]["dataproc_gcp_region"],
                    self.__config["spark"]["dataproc_cluster_name"],
                    self.__config["gcs"]["bucket_name"],
                    spark_script,
                    [
                        f"--{constants.ARG_PROJECT_NUMBER}="
                        + self.__config["gcp"]["gcp_project_number"],
                        f"--{constants.ARG_REGION}="
                        + self.__config["pubsublite"]["pubsublite_gcp_region"],
                        f"--{constants.ARG_TABLE_NAME}=" + self.__table_name,
                        f"--{constants.ARG_GCS_WAREHOUSE_DIR}="
                        + constants.GS_PREFIX
                        + self.__config["gcs"]["bucket_name"]
                        + self.__config["gcs"]["warehouse_dir_folder"],
                        f"--{constants.ARG_TOPIC_ID}=" + "topic_" + self.__table_name,
                        f"--{constants.ARG_STREAM_TIME}="
                        + self.__config["pubsublite"]["stream_time"],
                        f"--{constants.ARG_TIMESTAMP_OFFSET_MILI}="
                        + self.__config["pubsublite"]["timestamp_offset_mili"],
                    ],
                )
                print(job_output)
            else:
                logging.error("Unknown expose mode!")


class PolyExpose:
    """
    Main wrapper.
    It basically maintains a replica of the catalog and provides a way to access the underlying tables.
    """

    def __init__(self, yaml_config):
        with open(yaml_config, "r") as yaml_file:
            data = yaml.load(yaml_file, Loader=yaml.FullLoader)
        self.__config = data
        logging.info(f"Object created with config: {yaml_config}")
        self.__initiated = False
        self.__catalog_cache_local_file = constants.CATALOG_CACHE_LOCAL_FILE
        self.__catalog_tables = []

    def init(self):
        self.__initiated = True
        # Deploy SPARK scripts
        for spark_template in self.__config["spark"]["spark_templates"]:
            with importlib.resources.path(
                "polyexpose.spark_templates",
                # TODO(velascoluis): Fix access to dict values of config file
                list(spark_template.values())[0],
            ) as src_path:
                utils.deploy_to_gcs(
                    src_path,
                    self.__config["gcs"]["bucket_name"],
                    self.__config["gcs"]["scripts_folder"],
                )
        # Deploy sample data
        with importlib.resources.path(
            "polyexpose.data",
            # TODO(velascoluis): Make it constant
            "userdata.parquet",
        ) as src_path:
            utils.deploy_to_gcs(
                src_path,
                self.__config["gcs"]["bucket_name"],
                self.__config["gcs"]["sample_data_folder"],
            )
        print("Package initialized!")

    def create_sample_table(self, table_name):
        """
        Creates a sample table
        """
        if not self.__initiated:
            logging.error(f"Please call init method first")
            raise Exception("Not initiated!")
        for spark_template in self.__config["spark"]["spark_templates"]:
            if constants.SPARK_TEMPLATE_CREATE_SAMPLE_TABLE in spark_template.keys():
                spark_script = (
                    constants.GS_PREFIX
                    + self.__config["gcs"]["bucket_name"]
                    + self.__config["gcs"]["scripts_folder"]
                    + spark_template[constants.SPARK_TEMPLATE_CREATE_SAMPLE_TABLE]
                )
        job_output = utils.submit_job(
            self.__config["gcp"]["gcp_project_id"],
            self.__config["spark"]["dataproc_gcp_region"],
            self.__config["spark"]["dataproc_cluster_name"],
            self.__config["gcs"]["bucket_name"],
            spark_script,
            [
                f"--{constants.ARG_DATA_DIR}="
                + constants.GS_PREFIX
                + self.__config["gcs"]["bucket_name"]
                + self.__config["gcs"]["sample_data_folder"],
                f"--{constants.ARG_TABLE_NAME}=" + table_name,
                f"--{constants.ARG_GCS_WAREHOUSE_DIR}="
                + constants.GS_PREFIX
                + self.__config["gcs"]["bucket_name"]
                + self.__config["gcs"]["warehouse_dir_folder"],
            ],
        )
        print(job_output)

    def get_table(self, table_name):
        """
        Return the current selected table
        :param table_name : The table name.
        """
        if not self.__initiated:
            logging.error(f"Please call init method first")
            raise Exception("Not initiated!")
        if table_name in self.__catalog_tables:
            return IcebergTable(table_name, self.__config)
        else:
            logging.error("Table not found on catalog, is it loaded?!")
            raise Exception("Table not found!")

    def load_catalog(self, force_reload):
        """
        Loads and show the SPARK catalog and save it locally.
        It uses a local file as a temporary cache.
        """
        if not self.__initiated:
            logging.error(f"Please call init method first")
            raise Exception("Not initiated!")
        try:
            if force_reload:
                raise OSError
            if not self.__catalog_tables:
                catalog_file = open(self.__catalog_cache_local_file, "r")
                for line in catalog_file:
                    self.__catalog_tables.append(line)
        except OSError:
            for spark_template in self.__config["spark"]["spark_templates"]:
                if (
                    constants.SPARK_TEMPLATE_ICEBERG_SHOW_CATALOG
                    in spark_template.keys()
                ):
                    spark_script = (
                        constants.GS_PREFIX
                        + self.__config["gcs"]["bucket_name"]
                        + self.__config["gcs"]["scripts_folder"]
                        + spark_template[constants.SPARK_TEMPLATE_ICEBERG_SHOW_CATALOG]
                    )
            job_output = utils.submit_job(
                self.__config["gcp"]["gcp_project_id"],
                self.__config["spark"]["dataproc_gcp_region"],
                self.__config["spark"]["dataproc_cluster_name"],
                self.__config["gcs"]["bucket_name"],
                spark_script,
                [
                    f"--{constants.ARG_GCS_WAREHOUSE_DIR}="
                    + constants.GS_PREFIX
                    + self.__config["gcs"]["bucket_name"]
                    + self.__config["gcs"]["warehouse_dir_folder"]
                ],
            )
            logging.info(f"Metadata extracted, parsing output ...")
            catalog_file = open(self.__catalog_cache_local_file, "w")
            for line in job_output.split(constants.LINE_SPLIT_NL):
                if line.startswith(constants.LOG_PARSE_TABLE_DELIMITER):
                    self.__catalog_tables.append(
                        line.split(constants.LINE_SPLIT_BLANK)[1]
                    )
                    catalog_file.write(line.split(constants.LINE_SPLIT_BLANK)[1])
            logging.info(f"Table names extracted from catalog")

    def show_tables(self):
        if not self.__initiated:
            logging.error(f"Please call init method first")
            raise Exception("Not initiated!")
        print(f"Tables avaliable in catalog:{self.__catalog_tables}")
