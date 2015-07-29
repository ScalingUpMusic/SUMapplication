# Source Files

## oneTag.py

Standalone python script for building a predictive model based on a provided tag.

Can run from src with the following command:

	 $SPARK_HOME/bin/spark-submit --master local[4] oneTag.py -t <tag string> -d <data path> -v

All arguments are optionally (if no tag provided uses 'rock')

If no data path is provided it assumes that the AdditionalFiles folder from the million song dataset is in ```/root/data/AdditionalFiles```

Note that $SPARK_HOME is set to ```/usr/local/spark```

## rockTagShell.py

Runs in the shell (doesn't set up SparkContext because shell does that)

## data notes

downloaded additional files

	rsync -avzuP publicdata.opensciencedatacloud.org::ark:/31807/osdc-c1c763e4/AdditionalFiles.tar.gz /root/data/AdditionalFiles.tar.gz