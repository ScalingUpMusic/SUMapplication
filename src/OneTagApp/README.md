# Instructions for building OneTagApp
ssh to spark master (ambari2 in our case)

cd to SUMapplication/src/OneTagApp

	sbt package

Double check that the jar file built

	find . -name \*one-tag-model*.jar

# Instructions for running OneTagApp
To run this file training a model on \<tag\>:

	SPARK_HOME/bin/spark-submit --class "OneTagModel" $(find . -name \*one-tag-model*.jar) <tag>

For example, to run on ambari w/ our spark location and \<tag\> "alternative":

	/usr/hdp/current/spark-client/bin/spark-submit --class "OneTagModel" $(find . -name \*one-tag-model*.jar) alternative
