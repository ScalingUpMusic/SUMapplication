# Source Files

## rockTag.py

Standalone python script for building a predictive model based on 'rock' tags.

Can run from src with the following command:

	 $SPARK_HOME/bin/spark-submit --master local[4] rockTag.py

Note that $SPARK_HOME is set to ```/usr/local/spark```

This assumes that the AdditionalFiles folder from the million song dataset is in ```/root/data/AdditionalFiles```
