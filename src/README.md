# Production Code

## h52hdfs.py

This file extracts features from every track .h5 file (1 million songs) in our GPFS cluster and saves the results in a row of a text file on our HDFS cluster.

To Run
```
spark-submit --master yarn-cluster <options> h52hdfs.py
```

## MultiPredictorApp

Spark scala application to load classification models saved to HDFS and hydrate them with data to make classification predictions.

# Test Code

## oneTag_1_*.py

Standalone python script for building a predictive model based on a provided tag. We had to create a couple versions as we struggled with some older versions of Spark.

Can run from src with the following command:

	 $SPARK_HOME/bin/spark-submit --master spark://spark1:7077 oneTag.py -t <tag string> -d <data path> -v

All arguments are optionally (if no tag provided uses 'rock')

If no data path is provided it assumes that the AdditionalFiles folder from the million song dataset is in ```/root/data/AdditionalFiles```

Note that $SPARK_HOME is set to ```/usr/local/spark```

Right now this works with the subset of data. I tried getting the full dataset AdditionalFiles and running this on it (I added the option -a or --alldata to the python script to do that), but spark runs out of memory.

## rockTagShell.py

Runs in the shell (doesn't set up SparkContext because shell does that)

