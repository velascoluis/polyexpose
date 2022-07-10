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

import logging
import polyexpose.common.constants as constants
from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage
import re
import requests
import json
import os
from google.cloud.pubsublite import AdminClient, Reservation, Topic, Subscription
from google.cloud.pubsublite.types import (
    CloudRegion,
    ReservationPath,
    SubscriptionPath,
    TopicPath,
)
from google.protobuf.duration_pb2 import Duration
from google.api_core.exceptions import AlreadyExists, NotFound
from google.cloud import bigquery
from google.cloud import storage


def deploy_to_gcs(source, bucket_name, location_dir):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    # TODO(velascoluis): Fix "/dir/" into "dir/"
    blob = bucket.blob(location_dir[1:] + os.path.basename(source))
    blob.upload_from_filename(source)
    logging.info("Files deployed to GCS OK")


def create_pubsub_lite(
    project_number, reservation_id, cloud_region, topic_id, subscription_id
):
    throughput_capacity = 4
    num_partitions = 1
    cloud_region = CloudRegion(cloud_region)
    reservation_path = ReservationPath(project_number, cloud_region, reservation_id)
    reservation = Reservation(
        name=str(reservation_path), throughput_capacity=throughput_capacity
    )
    client = AdminClient(cloud_region)
    try:
        response = client.create_reservation(reservation)
        logging.info(f"{response.name} created successfully.")
    except AlreadyExists:
        logging.info(f"{reservation_path} already exists.")

    topic_path = TopicPath(project_number, cloud_region, topic_id)
    topic = Topic(
        name=str(topic_path),
        partition_config=Topic.PartitionConfig(
            count=num_partitions,
            capacity=Topic.PartitionConfig.Capacity(
                publish_mib_per_sec=4,
                subscribe_mib_per_sec=8,
            ),
        ),
        retention_config=Topic.RetentionConfig(
            per_partition_bytes=30 * 1024 * 1024 * 1024,
            period=Duration(seconds=60 * 60 * 24 * 7),
        ),
        reservation_config=Topic.ReservationConfig(
            throughput_reservation=str(reservation_path),
        ),
    )
    try:
        response = client.create_topic(topic)
        logging.info(f"{response.name} created successfully.")
    except AlreadyExists:
        logging.info(f"{topic_path} already exists.")

    subscription_path = SubscriptionPath(project_number, cloud_region, subscription_id)
    subscription = Subscription(
        name=str(subscription_path),
        topic=str(topic_path),
        delivery_config=Subscription.DeliveryConfig(
            delivery_requirement=Subscription.DeliveryConfig.DeliveryRequirement.DELIVER_IMMEDIATELY,
        ),
    )

    client = AdminClient(cloud_region)
    try:
        response = client.create_subscription(subscription)
        logging.info(f"{response.name} created successfully.")
    except AlreadyExists:
        logging.info(f"{subscription_path} already exists.")


def submit_job(
    project_id, region, cluster_name, bucket_name, spark_template_name, arg_list
):

    jar_list = [
        constants.GS_PREFIX
        + bucket_name
        + constants.GCS_JAR_FOLDER
        + constants.SPARK_ICEBERG_JAR,
        constants.GS_PREFIX
        + bucket_name
        + constants.GCS_JAR_FOLDER
        + constants.SPARK_BIGQUERY_JAR,
        constants.GS_PREFIX
        + bucket_name
        + constants.GCS_JAR_FOLDER
        + constants.SPARK_PUBSUBLITE_JAR,
    ]

    job_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": spark_template_name,
            "jar_file_uris": jar_list,
            "args": arg_list,
        },
    }
    logging.info(f"Executing SPARK job: {spark_template_name} with args: {arg_list}")
    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    response = operation.result()
    matches = re.match(constants.RE_GCS, response.driver_output_resource_uri)
    output = (
        storage.Client()
        .get_bucket(matches.group(1))
        .blob(f"{matches.group(2)}.000000000")
        .download_as_bytes()
        .decode("UTF-8")
    )
    logging.info(output)
    return output


def create_bq_dataset(project_id, dataset_name, location):
    client = bigquery.Client()
    dataset_id = "{}.{}".format(project_id, dataset_name)
    try:
        client.get_dataset(dataset_id)
        logging.info("Dataset {} already exists".format(dataset_id))
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        dataset = client.create_dataset(dataset, timeout=30)
        logging.info("Created dataset {}.{}".format(client.project, dataset.dataset_id))


def create_gcs_bucket(bucket_name, location):
    client = storage.Client()
    try:
        bucket = client.get_bucket(bucket_name)
        logging.info("Bucket {} already exists".format(bucket_name))
    except NotFound:
        bucket = client.bucket(bucket_name)
        bucket.storage_class = "STANDARD"
        client.create_bucket(bucket, location=location)
    logging.info("Created bucket {} in {}".format(bucket_name, location))


def send_request(url, request, gql):
    logging.info(f"POLYEXPOSE - Sending request:{json.dumps(request)} to: {url}")
    if gql:
        result = requests.post(url, json={"query": request})
    else:
        result = requests.post(url, json=request)
    logging.info(result.text)
    return result.text
