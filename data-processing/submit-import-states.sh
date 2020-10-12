#!/bin/sh

spark-submit --jars ~/postgresql-42.2.16.jar \
	           --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7 \
             --master spark://10.0.0.4:7077 import_state_codes.py
