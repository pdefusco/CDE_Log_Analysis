############################################
# Author(s): Paul de Fusco                 #
############################################

from cdepy import cdeconnection
from cdepy import cdejob
from cdepy import cdemanager
from cdepy import cderesource
from cdepy.utils import sparkEventLogParser
import os
import json

############################################
# Create a Connection to CDE and set Token #
############################################

JOBS_API_URL = os.environ['JOBS_API_URL']
WORKLOAD_USER = os.environ['PROJECT_OWNER']
WORKLOAD_PASSWORD = os.environ['WORKLOAD_PWD']

myCdeConnection = cdeconnection.CdeConnection(JOBS_API_URL, WORKLOAD_USER, WORKLOAD_PASSWORD)

myCdeConnection.setToken()

############################################
# Create a CDE Files Resource Definition   #
############################################

CDE_RESOURCE_NAME = "myFilesCdeResource"
myCdeFilesResource = cderesource.CdeFilesResource(CDE_RESOURCE_NAME)
myCdeFilesResourceDefinition = myCdeFilesResource.createResourceDefinition()

myCdeFilesResourceDefinition

############################################
# Create a CDE Spark Job  Definition       #
############################################

CDE_JOB_NAME = "myCdeSparkJob"
APPLICATION_FILE_NAME = "pysparksql.py"

myCdeSparkJob = cdejob.CdeSparkJob(myCdeConnection)
myCdeSparkJobDefinition = myCdeSparkJob.createJobDefinition(CDE_JOB_NAME, CDE_RESOURCE_NAME, APPLICATION_FILE_NAME, executorMemory="2g", executorCores=2)

myCdeSparkJobDefinition

##########################################
# Create Resource and Job in CDE Cluster #
##########################################

LOCAL_FILE_PATH = "examples"
LOCAL_FILE_NAME = "pysparksql.py"

myCdeClusterManager = cdemanager.CdeClusterManager(myCdeConnection)

#############################################################
### CREATE FILES RESOURCE AND UPLOAD PYSPARK SCRIPT TO IT ###
#############################################################

myCdeClusterManager.createResource(myCdeFilesResourceDefinition)
myCdeClusterManager.uploadFile(CDE_RESOURCE_NAME, LOCAL_FILE_PATH, LOCAL_FILE_NAME)

###################################
### CREATE SPARK JOB AND RUN IT ###
###################################

myCdeClusterManager.createJob(myCdeSparkJobDefinition)
myCdeClusterManager.runJob(CDE_JOB_NAME)

#########################
### VALIDATE JOB RUNS ###
#########################

# Please give the prior job run a minute to complete before moving on

jobRuns = myCdeClusterManager.listJobRuns()
json.loads(jobRuns)

#####################
### DOWNLOAD LOGS ###
#####################

JOB_RUN_ID = "18421"
logTypes = myCdeClusterManager.showAvailableLogTypes(JOB_RUN_ID)
json.loads(logTypes)

LOGS_TYPE = "driver/event"
sparkEventLogs = myCdeClusterManager.downloadJobRunLogs(JOB_RUN_ID, LOGS_TYPE)

sparkEventLogsClean = sparkEventLogParser(sparkEventLogs)

print(sparkEventLogsClean)

#####################
### SAVE LOGS     ###
#####################

for jobId in range(18400,18520):
  
  LOGS_TYPE = "driver/event"
  sparkEventLogs = myCdeClusterManager.downloadJobRunLogs(str(jobId), LOGS_TYPE)
  sparkEventLogsClean = sparkEventLogParser(sparkEventLogs)
  
  with open("sparklogs/spark_logs_{}.txt".format(jobId), "w") as text_file:
      text_file.write(sparkEventLogsClean)


