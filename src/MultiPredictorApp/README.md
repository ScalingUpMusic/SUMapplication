Build the jar file:

```
sbt package
```

Run the prediction script (grabs a random song from the MSD and generates a prediction from each saved SVM model:

```
$SPARK_HOME/bin/spark-submit ./target/scala-2.10/multi-predictor_2.10-1.0.jar
```
