# Instructions for building OneTagApp
ssh to spark master (ambari2 in our case)

cd to SUMapplication/src/OneTagApp

	sbt package

Double check that the jar file built

	find . -name \*.jar

# Instructions for running OneTagApp
To run this file:

	SPARK_HOME/bin/spark-submit --class "OneTagModel" $(find . -name \*.jar)

To run on ambari w/ our spark location:

	/usr/hdp/current/spark-client/bin/spark-submit --class "OneTagModel" $(find . -name \*.jar)
