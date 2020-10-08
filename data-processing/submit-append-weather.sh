#!/bin/sh
#spark-submit --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7 \
#             --master spark://10.0.0.4:7077 wordcount.py \
#             s3a://commoncrawl/crawl-data/CC-MAIN-2020-16/segment.paths.gz \
#             > spark-s3.log
#
spark-submit --jars ~/postgresql-42.2.16.jar \
             --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7 \
             --master spark://10.0.0.4:7077 append_weather_data.py \
             1968 2005
