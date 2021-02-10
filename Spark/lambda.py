import json

from datetime import datetime
from copy import deepcopy
import json
import boto3
import uuid
import sys

def lambda_handler(event, context):
    # TODO implement
    
    conn = boto3.client("emr")

    clusters = conn.list_clusters()
    # choose the correct cluster
    clusters = [c["Id"] for c in clusters["Clusters"] 
                if c["Status"]["State"] in ["RUNNING", "WAITING"]]
    if not clusters:
        sys.stderr.write("No valid clusters\n")
        sys.stderr.exit()
    cluster_id = clusters[0]
    # code location on youra emr master node
    CODE_DIR = "/home/hadoop/"
    
    # spark configuration example
    step_args = ["/usr/bin/spark-submit", "", "",
                 CODE_DIR + "wordcount.py", '', '']

    step = {"Name": "ProcessWordCount",
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
                'Args': step_args
            }
        }
    action = conn.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
    return "Added step: %s"%(action)

