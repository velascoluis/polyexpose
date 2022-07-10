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

import pyspark
from pyspark.sql import SparkSession
import argparse


def export_bigquery(gcs_temp_bucket, table_name, gcs_warehouse_dir, bigquery_dataset):
    config = pyspark.SparkConf().setAll(
        [
            (
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            ),
            (
                "spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog",
            ),
            ("spark.sql.catalog.spark_catalog.type", "hive"),
            ("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog"),
            ("spark.sql.catalog.local.type", "hadoop"),
            ("spark.sql.catalog.local.warehouse", gcs_warehouse_dir),
        ]
    )

    spark = SparkSession.builder.config(conf=config).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("temporaryGcsBucket", gcs_temp_bucket)
    print("POLYEXPOSE - Reading table: " + table_name + "\n")
    df = spark.table(table_name)
    df.write.format("bigquery").mode("overwrite").option(
        "table", bigquery_dataset + "." + table_name.replace(".", "_")
    ).save()
    print(
        "POLYEXPOSE - Data staged in : "
        + bigquery_dataset
        + "."
        + table_name.replace(".", "_")
    )
    print(
        "POLYEXPOSE - You can now issue GQL queries against "
        + bigquery_dataset
        + "_"
        + table_name.replace(".", "_")
    )
    spark.stop()


def main(params):
    export_bigquery(
        params.gcs_temp_bucket,
        params.table_name,
        params.gcs_warehouse_dir,
        params.bigquery_dataset,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark SQL driver code")
    parser.add_argument("--gcs_temp_bucket", type=str, default="None")
    parser.add_argument("--table_name", type=str, default="None")
    parser.add_argument("--gcs_warehouse_dir", type=str, default="None")
    parser.add_argument("--bigquery_dataset", type=str, default="None")
    params = parser.parse_args()
    main(params)
