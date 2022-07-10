#!/bin/sh
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

LOG_DATE=`date`
echo "#################################################################"
echo "${LOG_DATE} Starting infrastructure BOOTSTRAPING .."

if [ "$#" -ne 1 ]; then
    echo "Illegal number of parameters"
    echo "Usage: ${0} config.yaml"
fi

PLAT=`uname`
if [ ! "${PLAT}" = "Linux" ]; then
   echo "This script needs to be executed on Linux"
   exit 1
fi

timeout -k 2 2 bash -c "sudo chmod --help" > /dev/null 2>&1 &
if [ ! $? -eq 0 ];then
    echo "This script needs sudo access"
    exit 1
fi

SED_BIN=`which sed`
if [ ! $? -eq 0 ];then
    LOG_DATE=`date`
    echo "sed not found!"
    exit 1
fi

CURL_BIN=`which curl`
if [ ! $? -eq 0 ];then
    LOG_DATE=`date`
    echo "curl not found!"
    exit 1
fi


TERRAFORM_BIN=`which terraform`
if [ ! $? -eq 0 ];then
    LOG_DATE=`date`
    echo "#################################################################"
    echo "${LOG_DATE} Installing terraform .."
    sudo apt-get update && sudo apt-get install -y gnupg software-properties-common curl
    curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo apt-key add -
    sudo apt-add-repository "deb [arch=amd64] https://apt.releases.hashicorp.com $(lsb_release -cs) main"
    sudo apt-get update && sudo apt-get install terraform

    LOG_DATE=`date`
    echo "#################################################################"
    echo "${LOG_DATE} Terraform deployed OK"
fi

YQ_BIN=`which yq`
if [ ! $? -eq 0 ];then
    LOG_DATE=`date`
    echo "#################################################################"
    echo "${LOG_DATE} Installing yq .."
    sudo wget -qO /usr/local/bin/yq https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64
    sudo chmod a+x /usr/local/bin/yq
    LOG_DATE=`date`
    echo "#################################################################"
    echo "${LOG_DATE} yq deployed OK"
fi


GCLOUD_BIN=`which gcloud`
if [ ! $? -eq 0 ];then
    LOG_DATE=`date`
    echo "gcloud not found! Please install first and configure Google Cloud SDK"
    exit 1
fi

KUBECTL_BIN=`which kubectl`
if [ ! $? -eq 0 ];then
    LOG_DATE=`date`
    echo "kubectl not found! Please install first and configure Google Cloud SDK"
    exit 1
fi

GSUTIL_BIN=`which gsutil`
if [ ! $? -eq 0 ];then
    LOG_DATE=`date`
    echo "gsutil not found! Please install first and configure Google Cloud SDK"
    exit 1
fi

CONFIG_FILE=${1}
if [ ! -f "${CONFIG_FILE}" ]; then
    echo "Unable to find ${CONFIG_FILE}"
    exit 1
fi


SPARK_ICEBERG_JAR_LOCATION="jars/iceberg-spark-runtime-3.1_2.12-0.13.2.jar"
if [ ! -f "${SPARK_ICEBERG_JAR_LOCATION}" ]; then
    echo "Unable to find ${SPARK_ICEBERG_JAR_LOCATION}"
    exit 1
fi
SPARK_BIGQUERY_JAR_LOCATION="jars/spark-bigquery-with-dependencies_2.12-0.25.2.jar"
if [ ! -f "${SPARK_BIGQUERY_JAR_LOCATION}" ]; then
    echo "Unable to find ${SPARK_BIGQUERY_JAR_LOCATION}"
    exit 1
fi
SPARK_PUBSUBLITE_JAR_LOCATION="jars/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar"
if [ ! -f "${SPARK_PUBSUBLITE_JAR_LOCATION}" ]; then
    echo "Unable to find ${SPARK_PUBSUBLITE_JAR_LOCATION}"
    exit 1
fi

export PROJECT=`cat ${CONFIG_FILE} | yq e '.gcp.gcp_project_id' -`
if [ -z ${PROJECT} ]
then
      echo "Error reading PROJECT"
      exit 1
else
    echo "PROJECT : ${PROJECT}"
fi
export REGION=`cat ${CONFIG_FILE} | yq e '.gcp.gcp_region' -`
if [ -z ${REGION} ]
then
      echo "Error reading REGION"
      exit 1
else
    echo "REGION : ${REGION}"
fi
export ZONE=`cat ${CONFIG_FILE} | yq e '.gcp.gcp_zone' -`
if [ -z ${ZONE} ]
then
      echo "Error reading ZONE"
      exit 1
else
    echo "ZONE : ${ZONE}"
fi
export BUCKETNAME=`cat ${CONFIG_FILE} | yq e '.gcs.bucket_name' -`
if [ -z ${BUCKETNAME} ]
then
      echo "Error reading BUCKETNAME"
      exit 1
else
    echo "BUCKETNAME : ${BUCKETNAME}"
fi
export DATAPROCNAME=`cat ${CONFIG_FILE} | yq e '.spark.dataproc_cluster_name' -`
if [ -z ${DATAPROCNAME} ]
then
      echo "Error reading DATAPROCNAME"
      exit 1
else
    echo "DATAPROCNAME : ${DATAPROCNAME}"
fi
export TF_SVC_ACCOUNT=`cat ${CONFIG_FILE} | yq e '.gcp.sa_terraform_name' -` 
if [ -z ${TF_SVC_ACCOUNT} ]
then
      echo "Error reading TF_SVC_ACCOUNT"
      exit 1
else
    echo "TF_SVC_ACCOUNT : ${TF_SVC_ACCOUNT}"
fi
export HASURA_SVC_ACCOUNT=`cat ${CONFIG_FILE} | yq e '.gcp.sa_name' -`
if [ -z ${HASURA_SVC_ACCOUNT} ]
then
      echo "Error reading HASURA_SVC_ACCOUNT"
      exit 1
else
    echo "HASURA_SVC_ACCOUNT : ${HASURA_SVC_ACCOUNT}"
fi
export TF_SVC_ACCOUNT_KEY_FILE=`cat ${CONFIG_FILE} | yq e '.gcp.sa_terraform_key' -`
if [ -z ${TF_SVC_ACCOUNT_KEY_FILE} ]
then
      echo "Error reading TF_SVC_ACCOUNT_KEY_FILE"
      exit 1
else
    echo "TF_SVC_ACCOUNT_KEY_FILE : ${TF_SVC_ACCOUNT_KEY_FILE}"
fi
export HASURA_SVC_ACCOUNT_KEY_FILE=`cat ${CONFIG_FILE} | yq e '.gcp.sa_key' -`
if [ -z ${HASURA_SVC_ACCOUNT_KEY_FILE} ]
then
      echo "Error reading HASURA_SVC_ACCOUNT_KEY_FILE"
      exit 1
else
    echo "HASURA_SVC_ACCOUNT_KEY_FILE : ${HASURA_SVC_ACCOUNT_KEY_FILE}"
fi
export DBNAME_PREFIX=`cat ${CONFIG_FILE} | yq e '.gke.dbname_prefix' -`
if [ -z ${DBNAME_PREFIX} ]
then
      echo "Error reading DBNAME_PREFIX"
      exit 1
else
    echo "DBNAME_PREFIX : ${DBNAME_PREFIX}"
fi
export GKENAME=`cat ${CONFIG_FILE} | yq e '.gke.gke_cluster_name' -`
if [ -z ${GKENAME} ]
then
      echo "Error reading GKENAME"
      exit 1
else
    echo "GKENAME : ${GKENAME}"
fi

echo "ENABLING PROJECT APIs"
#Enable project APIs
${GCLOUD_BIN} config set project ${PROJECT}
PROJECT_APIS_LIST="compute.googleapis.com servicenetworking.googleapis.com cloudresourcemanager.googleapis.com container.googleapis.com storage-api.googleapis.com sqladmin.googleapis.com iam.googleapis.com  iamcredentials.googleapis.com containerregistry.googleapis.com"
for API_NAME in ${PROJECT_APIS_LIST}
do
  LOG_DATE=`date`
  echo "${LOG_DATE} Enabling API .. " ${API_NAME}
  ${GCLOUD_BIN} services enable ${API_NAME}
done
LOG_DATE=`date`
echo "#################################################################"
echo "${LOG_DATE} CREATING SERVICE ACCOUNTS"
# Create the service account for TF
${GCLOUD_BIN} iam service-accounts describe ${TF_SVC_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com
if [ $? -eq 0 ]; then
    LOG_DATE=`date`
    echo "${LOG_DATE} Service account ${TF_SVC_ACCOUNT} already exists, recreating it ..."
    ${GCLOUD_BIN} iam service-accounts delete --quiet ${TF_SVC_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com
    ${GCLOUD_BIN} iam service-accounts create ${TF_SVC_ACCOUNT}
else
    LOG_DATE=`date`
    echo "${LOG_DATE} Creating Service Account .. " ${TF_SVC_ACCOUNT}
    ${GCLOUD_BIN} iam service-accounts create ${TF_SVC_ACCOUNT}
fi
TF_SA_ROLES_LIST="roles/dataproc.admin roles/compute.admin roles/container.admin roles/container.hostServiceAgentUser  roles/compute.securityAdmin roles/compute.networkAdmin roles/resourcemanager.projectIamAdmin roles/cloudkms.admin roles/cloudsql.admin roles/dns.admin roles/iam.securityAdmin roles/iam.serviceAccountAdmin roles/iam.serviceAccountUser roles/servicenetworking.networksAdmin roles/storage.admin"
for ROLE_NAME in ${TF_SA_ROLES_LIST}
do
  LOG_DATE=`date`
  echo "${LOG_DATE} Adding role .. " ${ROLE_NAME}
  ${GCLOUD_BIN} projects add-iam-policy-binding ${PROJECT} --member serviceAccount:${TF_SVC_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com --role ${ROLE_NAME}
done
# create a key
if [ -f ${TF_SVC_ACCOUNT_KEY_FILE} ]; then
    LOG_DATE=`date`
    echo "${LOG_DATE} ${TF_SVC_ACCOUNT_KEY_FILE} exists."
    rm ${TF_SVC_ACCOUNT_KEY_FILE}
fi
LOG_DATE=`date`
echo "Creating SA key ..."
${GCLOUD_BIN} iam service-accounts keys create ${TF_SVC_ACCOUNT_KEY_FILE} --iam-account=${TF_SVC_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com

# Create the service account for hasura
${GCLOUD_BIN} iam service-accounts describe ${HASURA_SVC_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com
if [ $? -eq 0 ]; then
    LOG_DATE=`date`
    echo "${LOG_DATE} Service account ${HASURA_SVC_ACCOUNT} already exists, recreating it ..."
    ${GCLOUD_BIN} iam service-accounts delete --quiet ${HASURA_SVC_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com
    ${GCLOUD_BIN} iam service-accounts create ${HASURA_SVC_ACCOUNT}
else
    LOG_DATE=`date`
    echo "${LOG_DATE} Creating Service Account .. " ${HASURA_SVC_ACCOUNT}
    ${GCLOUD_BIN} iam service-accounts create ${HASURA_SVC_ACCOUNT}
fi
HASURA_SA_ROLES_LIST="roles/cloudsql.admin roles/bigquery.admin"
for ROLE_NAME in ${HASURA_SA_ROLES_LIST}
do
  LOG_DATE=`date`
  echo "${LOG_DATE} Adding role .. " ${ROLE_NAME}
  ${GCLOUD_BIN} projects add-iam-policy-binding ${PROJECT} --member serviceAccount:${HASURA_SVC_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com --role ${ROLE_NAME}
done
# create a key
if [ -f ${HASURA_SVC_ACCOUNT_KEY_FILE} ]; then
    LOG_DATE=`date`
    echo "${LOG_DATE} ${HASURA_SVC_ACCOUNT_KEY_FILE} exists."
    rm ${HASURA_SVC_ACCOUNT_KEY_FILE}
fi
LOG_DATE=`date`
echo "Creating SA key ..."
${GCLOUD_BIN} iam service-accounts keys create ${HASURA_SVC_ACCOUNT_KEY_FILE} --iam-account=${HASURA_SVC_ACCOUNT}@${PROJECT}.iam.gserviceaccount.com

LOG_DATE=`date`
echo "#################################################################"
echo "${LOG_DATE} LAUNCHING TERRAFORM"
export LC_CTYPE=C
export LC_ALL=C
HASURA_SQL_PW=`cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 20 | head -n 1`

export TF_VAR_project=${PROJECT}
export TF_VAR_region=${REGION}
export TF_VAR_zone=${ZONE}
export TF_VAR_credentials=${TF_SVC_ACCOUNT_KEY_FILE}
export TF_VAR_dbname_prefix=${DBNAME_PREFIX}
export TF_VAR_gkename=${GKENAME}
export TF_VAR_dataprocname=${DATAPROCNAME}
export TF_VAR_bucketname=${BUCKETNAME}
export PLAN_NAME="polyexpose-infra.plan"
LOG_DATE=`date`
echo "${LOG_DATE} Deploying database, spark cluster, gcs bucket and kubernetes cluster for hasura ..."

${TERRAFORM_BIN} init -reconfigure
${TERRAFORM_BIN} plan -out=${PLAN_NAME}
${TERRAFORM_BIN} apply ${PLAN_NAME}
export DBNAME=`terraform output --raw dbname`
#Final steps, basically automation of https://hasura.io/docs/latest/graphql/core/deployment/deployment-guides/google-kubernetes-engine-cloud-sql/
LOG_DATE=`date`
echo "#################################################################"
echo "${LOG_DATE} POST-PROCESSING"
LOG_DATE=`date`
echo "${LOG_DATE} Setting password to " ${HASURA_SQL_PW}
${GCLOUD_BIN} sql users set-password postgres --instance ${DBNAME} --password ${HASURA_SQL_PW}
LOG_DATE=`date`
echo "${LOG_DATE} Generating kubernetes secret ..."
${GCLOUD_BIN} container clusters get-credentials ${GKENAME} --zone ${ZONE}
${KUBECTL_BIN} create secret generic cloudsql-instance-credentials --from-file=credentials.json=${HASURA_SVC_ACCOUNT_KEY_FILE}
${KUBECTL_BIN} create secret generic cloudsql-db-credentials --from-literal=username=postgres --from-literal=password=${HASURA_SQL_PW}
LOG_DATE=`date`
echo "${LOG_DATE} Downloading and editing deployment file for graphQL engine ..."
${CURL_BIN} -O https://raw.githubusercontent.com/hasura/graphql-engine/stable/install-manifests/google-cloud-k8s-sql/deployment.yaml
export INSTANCE_CONNECTION_NAME=`${GCLOUD_BIN} sql instances describe ${DBNAME} --format="value(connectionName)"`
${SED_BIN} -i -e "s/\[INSTANCE_CONNECTION_NAME\]/${INSTANCE_CONNECTION_NAME}/g" deployment.yaml
LOG_DATE=`date`
echo "${LOG_DATE} Creating deployment ..."
${KUBECTL_BIN} apply -f deployment.yaml
LOG_DATE=`date`
echo "${LOG_DATE} Exposing service ..."
${KUBECTL_BIN} expose deploy/hasura --port 80 --target-port 8080 --type LoadBalancer
${KUBECTL_BIN} get service
LOG_DATE=`date`
echo "${LOG_DATE} Staging jars ..."
${GSUTIL_BIN} cp ${SPARK_ICEBERG_JAR_LOCATION} gs://${BUCKETNAME}/jars/
${GSUTIL_BIN} cp ${SPARK_BIGQUERY_JAR_LOCATION} gs://${BUCKETNAME}/jars/
${GSUTIL_BIN} cp ${SPARK_PUBSUBLITE_JAR_LOCATION} gs://${BUCKETNAME}/jars/
