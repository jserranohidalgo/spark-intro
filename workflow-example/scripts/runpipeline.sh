#!/bin/bash

CLUSTER_ID=$1
MAIN_JAR=s3://$2/jars/workflow-example-assembly-0.0.1-SNAPSHOT.jar
PROFILE=default

aws emr add-steps \
  --cluster-id $CLUSTER_ID \
  --steps Type=Spark,Name="My program",ActionOnFailure=CONTINUE,Args=[--class,com.company.project.main.Main,$MAIN_JAR] \
  --profile $PROFILE
