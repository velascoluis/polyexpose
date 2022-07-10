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
from pyspark.sql.functions import lit,expr
from pyspark.sql.types import BinaryType, StringType
import argparse
from time import time
import uuid


def stream_pubsub(project_number,region,table_name,gcs_warehouse_dir,topic_id,stream_time,timestamp_offset_mili):
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
    print("POLYEXPOSE - Reading table in stream mode: " + table_name +"\n")
    sdf = spark.readStream.format("iceberg").option("stream-from-timestamp", str(int(time() * 1000)+int(timestamp_offset_mili))).load(table_name)
    #Concat all the columns on the data column, according to the expected pubsub lite schema, select only that column
    sdf_data = sdf.withColumn("data",expr("concat_ws(',',*)").cast(BinaryType())).select("data")
    #Add key column with no value
    sdf_ps = sdf_data.withColumn("key", lit("NOKEY").cast(BinaryType()))
    query = (
    sdf_ps.writeStream.format("pubsublite").option("pubsublite.topic",
        f"projects/{project_number}/locations/{region}/topics/{topic_id}",)
    .option("checkpointLocation", "/tmp/app" + uuid.uuid4().hex)
    .outputMode("append")
    .trigger(processingTime="1 second")
    .start()
    )
    query.awaitTermination(int(stream_time))
    print("POLYEXPOSE - Query executed, check subscription \n")
    


def main(params):
    stream_pubsub(params.project_number,params.region,params.table_name,params.gcs_warehouse_dir,params.topic_id,params.stream_time,params.timestamp_offset_mili)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Spark SQL driver code')
    parser.add_argument('--project_number', type=str, default='None')
    parser.add_argument('--region', type=str, default='None')
    parser.add_argument('--table_name', type=str, default='None')
    parser.add_argument('--gcs_warehouse_dir', type=str, default='None')
    parser.add_argument('--topic_id', type=str, default='None')
    parser.add_argument('--stream_time', type=str, default='None')
    parser.add_argument('--timestamp_offset_mili', type=str, default='None')
    params = parser.parse_args()
    main(params)
