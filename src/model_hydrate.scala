import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, LogisticRegressionModel}

val tag = "rock"

// Hydrate the model
val model = LogisticRegressionModel.load(sc, "saved_model_" + tag)
println(model)

