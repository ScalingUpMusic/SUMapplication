# Accessing the cluster

To access the Ambalt master run the following command and use the password K8zu6fCc:
ssh root@169.54.147.180 

To access Ambalt1 (the Spark master) from the Ambalt master:
ssh ambalt1

Prior to using Spark on Ambalt1, navigate to the Spark home folder and log in as the user spark:
cd /home/spark
su spark


# Source Files

## oneTag.py

Standalone python script for building a predictive model based on a provided tag.

Can run from src with the following command:

	 $SPARK_HOME/bin/spark-submit --master spark://spark1:7077 oneTag.py -t <tag string> -d <data path> -v

All arguments are optionally (if no tag provided uses 'rock')

If no data path is provided it assumes that the AdditionalFiles folder from the million song dataset is in ```/root/data/AdditionalFiles```

Note that $SPARK_HOME is set to ```/usr/local/spark```

Right now this works with the subset of data. I tried getting the full dataset AdditionalFiles and running this on it (I added the option -a or --alldata to the python script to do that), but spark runs out of memory.

## rockTagShell.py

Runs in the shell (doesn't set up SparkContext because shell does that)

## data notes

downloaded additional files

	rsync -avzuP publicdata.opensciencedatacloud.org::ark:/31807/osdc-c1c763e4/AdditionalFiles.tar.gz /root/data/AdditionalFiles.tar.gz
	tar -zxvf AdditionalFiles.tar.gz

## MultiTagApp

This folder contains a scala script that uses the MLlib package to train machine learning models that identify the genre of a song. It also contains an SBT file specifying versions and dependencies. To run locally on Ambalt1, log in to Ambalt1 as user spark, navigate to the directory, and run the following commands:

sbt package
/usr/hdp/current/spark-client/bin/spark-submit /home/spark/SUMapplication/src/MultiTagApp/target/scala-2.10/multi-tag-model_2.10-1.0.jar

To run on the cluster:

sbt package
/usr/hdp/current/spark-client/bin/spark-submit --master yarn-cluster --class "MultiTagModel" /home/spark/SUMapplication/src/MultiTagApp/target/scala-2.10/multi-tag-model_2.10-1.0.jar


## TagCountApp

This folder contains a scala script that determines how often each genre appears in the dataset and generates a text file with genres and counts, sorted by counts. The folder also contains an SBT file specifying versions and dependencies. To run locally on Ambalt1, log in to Ambalt1 as user spark, navigate to the directory, and run the following commands:

sbt package
/usr/hdp/current/spark-client/bin/spark-submit /home/spark/SUMapplication/src/MultiTagApp/target/scala-2.10/tag-count_2.10-1.0.jar

To run on the cluster:

sbt package
/usr/hdp/current/spark-client/bin/spark-submit --master yarn-cluster --class "TagCount" /home/spark/SUMapplication/src/MultiTagApp/target/scala-2.10/tag-count_2.10-1.0.jar


## AnalyzeModels

This folder contains a scala script that collects statistics about each model's cross-validation performance saved during training time and spits them out as a text file. The folder also contains an SBT file specifying versions and dependencies, as well as an R script to do some basic analysis and cleaning of the generated data. To run locally on Ambalt1, log in to Ambalt1 as user spark, navigate to the directory, and run the following commands:

sbt package
/usr/hdp/current/spark-client/bin/spark-submit /home/spark/SUMapplication/src/MultiTagApp/target/scala-2.10/summarize-model-performance_2.10-1.0.jar

To run on the cluster:

sbt package
/usr/hdp/current/spark-client/bin/spark-submit --master yarn-cluster --class "SummarizeModelPerformance" /home/spark/SUMapplication/src/MultiTagApp/target/scala-2.10/summarize-model-performance_2.10-1.0.jar

