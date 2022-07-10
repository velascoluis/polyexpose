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


def export_sql(exchange_dir, table_name,gcs_warehouse_dir):
    config = pyspark.SparkConf().setAll([('spark.sql.extensions',
                                          'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'),
                                         ('spark.sql.catalog.spark_catalog',
                                          'org.apache.iceberg.spark.SparkSessionCatalog'),
                                         ('spark.sql.catalog.spark_catalog.type', 'hive'), (
                                             'spark.sql.catalog.local',
                                             'org.apache.iceberg.spark.SparkCatalog'),
                                         ('spark.sql.catalog.local.type', 'hadoop'), (
                                             'spark.sql.catalog.local.warehouse',
                                             gcs_warehouse_dir)])

    spark = SparkSession.builder.config(conf=config).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("POLYEXPOSE - Reading table: " + table_name +"\n")
    df = spark.table(table_name)
    df.write.format('csv').mode('overwrite').save(exchange_dir+"/"+table_name)
    print("POLYEXPOSE - Data exposed as CSV in : " + exchange_dir +"/"+table_name)
    spark.stop()


def main(params):
    export_sql(params.exchange_dir,params.table_name,params.gcs_warehouse_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Spark SQL driver code')
    parser.add_argument('--exchange_dir', type=str, default='None')
    parser.add_argument('--table_name', type=str, default='None')
    parser.add_argument('--gcs_warehouse_dir', type=str, default='None')
    params = parser.parse_args()
    main(params)


